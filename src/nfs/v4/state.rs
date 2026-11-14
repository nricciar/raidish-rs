//! NFSv4.1 state (RFC 8881): client IDs, sessions and their slots, and
//! the open and lock state behind stateids, kept in memory on this node.
//!
//! A table made by a new state owner (`with_grace`) knows, from the
//! cluster's records, which clients held state before: for a grace period
//! those may reclaim their opens and locks, and nobody may make new ones
//! (`GRACE`), until each has said it is done (`RECLAIM_COMPLETE`) or the
//! period is over. A table without records has no grace period: reclaims
//! are refused (`NO_GRACE`) and clients open anew.

use super::err;
use crate::disk::FileId;
use std::collections::{HashMap, HashSet};
use std::sync::Mutex;
use std::time::{Duration, Instant};

/// The lease clients renew (with every `SEQUENCE`), in seconds.
pub const LEASE_SECS: u32 = 45;
/// Slots a session gets at most.
pub const MAX_SLOTS: u32 = 64;
/// Operations in one COMPOUND at most.
pub const MAX_OPS: u32 = 64;
/// Largest request / reply (a 1 MiB WRITE or READ, with room).
pub const MAX_REQUEST: u32 = (1 << 20) + 64 * 1024;
/// Largest reply kept for replays (`sa_cachethis`).
pub const MAX_CACHED_REPLY: u32 = 64 * 1024;

pub const SHARE_READ: u32 = 1;
pub const SHARE_WRITE: u32 = 2;

/// A stateid: `seqid` and 12 opaque bytes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub struct Stateid {
    pub seqid: u32,
    pub other: [u8; 12],
}

impl Stateid {
    pub const ANONYMOUS: Stateid = Stateid {
        seqid: 0,
        other: [0; 12],
    };
    pub const BYPASS: Stateid = Stateid {
        seqid: u32::MAX,
        other: [0xff; 12],
    };
    /// "The current stateid" of the COMPOUND (RFC 8881 §16.2.3.1.2).
    pub const CURRENT: Stateid = Stateid {
        seqid: 1,
        other: [0; 12],
    };
    /// What CLOSE hands back.
    pub const INVALID: Stateid = Stateid {
        seqid: u32::MAX,
        other: [0; 12],
    };

    pub fn is_special(&self) -> bool {
        self.other == [0; 12] || self.other == [0xff; 12]
    }
}

/// A byte range, `[start, end)`; `end` is `u64::MAX` for "to the end".
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Range {
    pub start: u64,
    pub end: u64,
}

impl Range {
    /// `offset` and `length` as LOCK has them (`u64::MAX` length: all).
    pub fn of(offset: u64, length: u64) -> Result<Range, u32> {
        if length == 0 {
            return Err(err::INVAL);
        }
        let end = if length == u64::MAX {
            u64::MAX
        } else {
            offset.checked_add(length).ok_or(err::INVAL)?
        };
        Ok(Range { start: offset, end })
    }

    fn overlaps(&self, o: &Range) -> bool {
        self.start < o.end && o.start < self.end
    }

    /// As LOCK4denied reports it.
    pub fn length(&self) -> u64 {
        if self.end == u64::MAX {
            u64::MAX
        } else {
            self.end - self.start
        }
    }
}

/// A lock that's in the way.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Denied {
    pub range: Range,
    pub write: bool,
    pub client: u64,
    pub owner: Vec<u8>,
}

#[derive(Debug, Clone)]
enum Kind {
    Open {
        owner: Vec<u8>,
        access: u32,
        deny: u32,
    },
    Lock {
        owner: Vec<u8>,
        /// The open it was made under.
        open: [u8; 12],
        /// Held ranges: `(range, write)`, sorted, never overlapping.
        held: Vec<(Range, bool)>,
    },
    /// A pNFS layout of the whole file (read, or read-write).
    Layout { rw: bool },
}

#[derive(Debug, Clone)]
struct State {
    client: u64,
    file: FileId,
    seqid: u32,
    kind: Kind,
}

/// One slot of a session's reply cache.
#[derive(Debug, Default)]
struct Slot {
    seqid: u32,
    /// The reply to `seqid`, if it was to be cached.
    reply: Option<Vec<u8>>,
    busy: bool,
}

struct Session {
    client: u64,
    slots: Vec<Slot>,
}

struct Client {
    owner: Vec<u8>,
    verifier: [u8; 8],
    confirmed: bool,
    /// The next CREATE_SESSION's `csa_sequence`, and the last reply.
    cs_seq: u32,
    cs_reply: Option<Vec<u8>>,
    renewed: Instant,
    reclaim_complete: bool,
    /// Held state before a failover, so may reclaim it in the grace period.
    may_reclaim: bool,
}

/// The grace period after a state owner change.
struct Grace {
    until: Instant,
    /// Clients (owner strings) that may reclaim.
    eligible: HashSet<Vec<u8>>,
    /// Those that haven't said RECLAIM_COMPLETE yet.
    waiting: HashSet<Vec<u8>>,
}

#[derive(Default)]
struct Inner {
    grace: Option<Grace>,
    next: u64,
    clients: HashMap<u64, Client>,
    sessions: HashMap<[u8; 16], Session>,
    states: HashMap<[u8; 12], State>,
    /// Stateids by file.
    files: HashMap<FileId, Vec<[u8; 12]>>,
}

impl Inner {
    fn in_grace(&self) -> bool {
        self.grace
            .as_ref()
            .is_some_and(|g| Instant::now() < g.until && !g.waiting.is_empty())
    }

    /// May `client` reclaim (`reclaim`), or make new state? Err: why not.
    fn grace_check(&self, client: u64, reclaim: bool) -> Result<(), u32> {
        let may = self
            .clients
            .get(&client)
            .is_some_and(|c| c.may_reclaim && !c.reclaim_complete);
        match (self.in_grace(), reclaim) {
            (true, false) => Err(err::GRACE),
            (true, true) if !may => Err(err::RECLAIM_BAD),
            (false, true) => Err(err::NO_GRACE),
            _ => Ok(()),
        }
    }

    fn id(&mut self) -> u64 {
        self.next += 1;
        self.next
    }

    fn drop_client(&mut self, id: u64) {
        self.clients.remove(&id);
        self.sessions.retain(|_, s| s.client != id);
        let gone: Vec<[u8; 12]> = self
            .states
            .iter()
            .filter(|(_, s)| s.client == id)
            .map(|(k, _)| *k)
            .collect();
        for k in gone {
            self.remove_state(&k);
        }
    }

    fn remove_state(&mut self, other: &[u8; 12]) {
        if let Some(s) = self.states.remove(other)
            && let Some(v) = self.files.get_mut(&s.file)
        {
            v.retain(|o| o != other);
            if v.is_empty() {
                self.files.remove(&s.file);
            }
        }
    }

    fn on_file(&self, file: FileId) -> impl Iterator<Item = (&[u8; 12], &State)> {
        self.files
            .get(&file)
            .into_iter()
            .flatten()
            .filter_map(move |o| self.states.get(o).map(|s| (o, s)))
    }
}

/// What `exchange_id` found or made.
pub struct Exchanged {
    pub clientid: u64,
    pub sequenceid: u32,
    pub confirmed: bool,
}

/// What `sequence` decided about a request.
pub enum Sequenced {
    /// A new request: run it (and `finish` the slot after).
    New { client: u64 },
    /// A retransmission: send this reply again.
    Replay(Vec<u8>),
}

pub struct StateTable {
    /// Tells this server instance's ids from a previous one's.
    instance: u32,
    inner: Mutex<Inner>,
}

impl Default for StateTable {
    fn default() -> Self {
        Self::new()
    }
}

impl StateTable {
    pub fn new() -> Self {
        StateTable {
            instance: (crate::fs::new_guid(0x5e55) >> 32) as u32 | 1,
            inner: Default::default(),
        }
    }

    /// A table for a new state owner: the clients `owners` held state
    /// with the previous one and may reclaim it for `grace`.
    pub fn with_grace(owners: Vec<Vec<u8>>, grace: Duration) -> Self {
        let t = Self::new();
        if !owners.is_empty() {
            let set: HashSet<Vec<u8>> = owners.into_iter().collect();
            t.inner.lock().unwrap().grace = Some(Grace {
                until: Instant::now() + grace,
                eligible: set.clone(),
                waiting: set,
            });
        }
        t
    }

    /// Whether new opens and locks wait for reclaims now.
    pub fn in_grace(&self) -> bool {
        self.inner.lock().unwrap().in_grace()
    }

    /// Once the grace period is over (everyone reclaimed, or time ran
    /// out): the clients that never came back, whose records can go. None
    /// while it lasts (or once reported).
    pub fn grace_over(&self) -> Option<Vec<Vec<u8>>> {
        let mut inner = self.inner.lock().unwrap();
        if inner.grace.is_none() || inner.in_grace() {
            return None;
        }
        let g = inner.grace.take().unwrap();
        Some(g.waiting.into_iter().collect())
    }

    fn other(&self, inner: &mut Inner) -> [u8; 12] {
        let mut o = [0u8; 12];
        o[..4].copy_from_slice(&self.instance.to_be_bytes());
        o[4..].copy_from_slice(&inner.id().to_be_bytes());
        o
    }

    fn clientid(&self, inner: &mut Inner) -> u64 {
        ((self.instance as u64) << 32) | (inner.id() & 0xffff_ffff)
    }

    // ---- client IDs ----

    /// EXCHANGE_ID (RFC 8881 §18.35): the client record for `owner`.
    pub fn exchange_id(
        &self,
        owner: &[u8],
        verifier: [u8; 8],
        update: bool,
    ) -> Result<Exchanged, u32> {
        let mut inner = self.inner.lock().unwrap();
        let existing = inner
            .clients
            .iter()
            .filter(|(_, c)| c.owner == owner)
            .map(|(id, c)| (*id, c.confirmed, c.verifier, c.cs_seq))
            .collect::<Vec<_>>();
        let confirmed = existing.iter().find(|e| e.1).copied();
        if update {
            return match confirmed {
                Some((id, _, v, seq)) if v == verifier => Ok(Exchanged {
                    clientid: id,
                    sequenceid: seq,
                    confirmed: true,
                }),
                Some(_) => Err(err::NOT_SAME),
                None => Err(err::NOENT),
            };
        }
        if let Some((id, _, v, seq)) = confirmed
            && v == verifier
        {
            // The same client again (another connection, or a retry).
            return Ok(Exchanged {
                clientid: id,
                sequenceid: seq,
                confirmed: true,
            });
        }
        // New, or rebooted (a new verifier): a fresh, unconfirmed record.
        // Any unconfirmed one for this owner is replaced; a confirmed one
        // goes once the new one is confirmed.
        for (id, c, ..) in &existing {
            if !c {
                inner.clients.remove(id);
            }
        }
        let id = self.clientid(&mut inner);
        let may_reclaim = inner.in_grace()
            && inner
                .grace
                .as_ref()
                .is_some_and(|g| g.eligible.contains(owner));
        inner.clients.insert(
            id,
            Client {
                owner: owner.to_vec(),
                verifier,
                confirmed: false,
                cs_seq: 1,
                cs_reply: None,
                renewed: Instant::now(),
                reclaim_complete: false,
                may_reclaim,
            },
        );
        Ok(Exchanged {
            clientid: id,
            sequenceid: 1,
            confirmed: false,
        })
    }

    /// CREATE_SESSION: check `seq` against the client's (a replay gets the
    /// last reply back), make the session (`slots` slots), confirm the
    /// client. `encode(sessionid)` makes the reply. Also returns the
    /// client's owner string if this confirmed it (to record).
    pub fn create_session(
        &self,
        clientid: u64,
        seq: u32,
        slots: u32,
        encode: impl FnOnce([u8; 16]) -> Vec<u8>,
    ) -> Result<(Vec<u8>, Option<Vec<u8>>), u32> {
        let mut inner = self.inner.lock().unwrap();
        if (clientid >> 32) as u32 != self.instance {
            return Err(err::STALE_CLIENTID);
        }
        let Some(c) = inner.clients.get(&clientid) else {
            return Err(err::STALE_CLIENTID);
        };
        if seq.wrapping_add(1) == c.cs_seq
            && let Some(r) = &c.cs_reply
        {
            return Ok((r.clone(), None));
        }
        if seq != c.cs_seq {
            return Err(err::SEQ_MISORDERED);
        }
        let owner = c.owner.clone();
        let mut sid = [0u8; 16];
        sid[..4].copy_from_slice(&self.instance.to_be_bytes());
        sid[4..12].copy_from_slice(&clientid.to_be_bytes());
        sid[12..].copy_from_slice(&(inner.id() as u32).to_be_bytes());
        inner.sessions.insert(
            sid,
            Session {
                client: clientid,
                slots: (0..slots.clamp(1, MAX_SLOTS))
                    .map(|_| Slot::default())
                    .collect(),
            },
        );
        // Confirming this record retires an older confirmed one for the
        // same owner (the client rebooted): its state goes with it.
        let old: Vec<u64> = inner
            .clients
            .iter()
            .filter(|(id, x)| **id != clientid && x.owner == owner && x.confirmed)
            .map(|(id, _)| *id)
            .collect();
        for id in old {
            inner.drop_client(id);
        }
        let reply = encode(sid);
        let c = inner.clients.get_mut(&clientid).unwrap();
        let newly = !c.confirmed;
        c.confirmed = true;
        c.cs_seq = seq.wrapping_add(1);
        c.cs_reply = Some(reply.clone());
        c.renewed = Instant::now();
        Ok((reply, newly.then_some(owner)))
    }

    pub fn destroy_session(&self, sid: &[u8; 16]) -> Result<(), u32> {
        match self.inner.lock().unwrap().sessions.remove(sid) {
            Some(_) => Ok(()),
            None => Err(err::BADSESSION),
        }
    }

    /// DESTROY_CLIENTID. Returns the client's owner string (to forget).
    pub fn destroy_clientid(&self, clientid: u64) -> Result<Vec<u8>, u32> {
        let mut inner = self.inner.lock().unwrap();
        let Some(c) = inner.clients.get(&clientid) else {
            return Err(err::STALE_CLIENTID);
        };
        let owner = c.owner.clone();
        if inner.sessions.values().any(|s| s.client == clientid) {
            return Err(err::CLIENTID_BUSY);
        }
        inner.drop_client(clientid);
        Ok(owner)
    }

    pub fn session_exists(&self, sid: &[u8; 16]) -> bool {
        self.inner.lock().unwrap().sessions.contains_key(sid)
    }

    /// The session's slot count.
    pub fn slots(&self, sid: &[u8; 16]) -> Option<u32> {
        self.inner
            .lock()
            .unwrap()
            .sessions
            .get(sid)
            .map(|s| s.slots.len() as u32)
    }

    /// SEQUENCE: check slot `slot`'s `seqid` (RFC 8881 §2.10.6.1), renew
    /// the client's lease.
    pub fn sequence(&self, sid: &[u8; 16], slot: u32, seqid: u32) -> Result<Sequenced, u32> {
        let mut inner = self.inner.lock().unwrap();
        let Some(s) = inner.sessions.get_mut(sid) else {
            return Err(err::BADSESSION);
        };
        let client = s.client;
        let Some(sl) = s.slots.get_mut(slot as usize) else {
            return Err(err::BADSLOT);
        };
        if seqid == sl.seqid.wrapping_add(1) {
            if sl.busy {
                return Err(err::DELAY);
            }
            sl.busy = true;
            sl.seqid = seqid;
            sl.reply = None;
        } else if seqid == sl.seqid {
            if sl.busy {
                return Err(err::DELAY);
            }
            return match &sl.reply {
                Some(r) => Ok(Sequenced::Replay(r.clone())),
                None => Err(err::RETRY_UNCACHED_REP),
            };
        } else {
            return Err(err::SEQ_MISORDERED);
        }
        if let Some(c) = inner.clients.get_mut(&client) {
            c.renewed = Instant::now();
        }
        Ok(Sequenced::New { client })
    }

    /// The request on a slot is done; keep its reply for replays if asked.
    pub fn finish(&self, sid: &[u8; 16], slot: u32, reply: Option<Vec<u8>>) {
        let mut inner = self.inner.lock().unwrap();
        if let Some(sl) = inner
            .sessions
            .get_mut(sid)
            .and_then(|s| s.slots.get_mut(slot as usize))
        {
            sl.busy = false;
            sl.reply = reply;
        }
    }

    /// RECLAIM_COMPLETE (once per client).
    pub fn reclaim_complete(&self, client: u64) -> Result<(), u32> {
        let mut inner = self.inner.lock().unwrap();
        let c = inner.clients.get_mut(&client).ok_or(err::STALE_CLIENTID)?;
        if c.reclaim_complete {
            return Err(err::COMPLETE_ALREADY);
        }
        c.reclaim_complete = true;
        let owner = c.owner.clone();
        if let Some(g) = &mut inner.grace {
            g.waiting.remove(&owner);
        }
        Ok(())
    }

    /// Forget clients whose lease ran out (with some slack), and all they
    /// held. Returns their owner strings (to forget).
    pub fn expire(&self) -> Vec<Vec<u8>> {
        let mut inner = self.inner.lock().unwrap();
        let limit = Duration::from_secs(LEASE_SECS as u64 + 15);
        let gone: Vec<u64> = inner
            .clients
            .iter()
            .filter(|(_, c)| c.renewed.elapsed() > limit)
            .map(|(id, _)| *id)
            .collect();
        let mut owners = Vec::new();
        for id in &gone {
            if let Some(c) = inner.clients.get(id) {
                owners.push(c.owner.clone());
            }
            inner.drop_client(*id);
        }
        owners
    }

    // ---- stateids ----

    /// Check `sid` (not a special one) for `file`, from `client`.
    fn check(
        &self,
        inner: &Inner,
        client: u64,
        sid: &Stateid,
        file: Option<FileId>,
    ) -> Result<(), u32> {
        if sid.other[..4] != self.instance.to_be_bytes() {
            return Err(err::STALE_STATEID);
        }
        let s = inner.states.get(&sid.other).ok_or(err::BAD_STATEID)?;
        if s.client != client || file.is_some_and(|f| f != s.file) {
            return Err(err::BAD_STATEID);
        }
        // seqid 0: "whatever is current" (4.1).
        if sid.seqid != 0 {
            if sid.seqid < s.seqid {
                return Err(err::OLD_STATEID);
            }
            if sid.seqid > s.seqid {
                return Err(err::BAD_STATEID);
            }
        }
        Ok(())
    }

    /// May `client` do I/O on `file` with `sid` (a write if `write`)?
    pub fn check_io(
        &self,
        client: u64,
        sid: &Stateid,
        file: FileId,
        write: bool,
    ) -> Result<(), u32> {
        let inner = self.inner.lock().unwrap();
        if sid.is_special() {
            // Anonymous I/O: refused if an open denies it.
            let denied = inner.on_file(file).any(|(_, s)| match &s.kind {
                Kind::Open { deny, .. } => deny & if write { SHARE_WRITE } else { SHARE_READ } != 0,
                _ => false,
            });
            return if denied { Err(err::LOCKED) } else { Ok(()) };
        }
        self.check(&inner, client, sid, Some(file))?;
        // A lock owner's write may not cross another owner's lock.
        Ok(())
    }

    /// TEST_STATEID for one stateid.
    pub fn test(&self, client: u64, sid: &Stateid) -> u32 {
        let inner = self.inner.lock().unwrap();
        match self.check(&inner, client, sid, None) {
            Ok(()) => err::OK,
            Err(e) => e,
        }
    }

    /// FREE_STATEID: a lock stateid holding nothing.
    pub fn free(&self, client: u64, sid: &Stateid) -> Result<(), u32> {
        let mut inner = self.inner.lock().unwrap();
        self.check(&inner, client, sid, None)?;
        match &inner.states[&sid.other].kind {
            Kind::Lock { held, .. } if held.is_empty() => {
                inner.remove_state(&sid.other);
                Ok(())
            }
            _ => Err(err::LOCKS_HELD),
        }
    }

    // ---- opens ----

    /// OPEN `file` for `owner` with `access` / `deny`: the open stateid
    /// (a second open by the same owner upgrades the first).
    pub fn open(
        &self,
        client: u64,
        owner: &[u8],
        file: FileId,
        access: u32,
        deny: u32,
        reclaim: bool,
    ) -> Result<Stateid, u32> {
        let mut inner = self.inner.lock().unwrap();
        if !inner.clients.get(&client).is_some_and(|c| c.confirmed) {
            return Err(err::STALE_CLIENTID);
        }
        inner.grace_check(client, reclaim)?;
        let mut mine = None;
        for (o, s) in inner.on_file(file) {
            if let Kind::Open {
                owner: ow,
                access: a,
                deny: d,
            } = &s.kind
            {
                if s.client == client && ow == owner {
                    mine = Some(*o);
                    continue;
                }
                if access & d != 0 || deny & a != 0 {
                    return Err(err::SHARE_DENIED);
                }
            }
        }
        if let Some(o) = mine {
            let s = inner.states.get_mut(&o).unwrap();
            if let Kind::Open {
                access: a, deny: d, ..
            } = &mut s.kind
            {
                *a |= access;
                *d |= deny;
            }
            s.seqid += 1;
            return Ok(Stateid {
                seqid: s.seqid,
                other: o,
            });
        }
        let other = self.other(&mut inner);
        inner.states.insert(
            other,
            State {
                client,
                file,
                seqid: 1,
                kind: Kind::Open {
                    owner: owner.to_vec(),
                    access,
                    deny,
                },
            },
        );
        inner.files.entry(file).or_default().push(other);
        Ok(Stateid { seqid: 1, other })
    }

    /// OPEN_DOWNGRADE to `access` / `deny` (a subset of what's open).
    pub fn downgrade(
        &self,
        client: u64,
        sid: &Stateid,
        file: FileId,
        access: u32,
        deny: u32,
    ) -> Result<Stateid, u32> {
        let mut inner = self.inner.lock().unwrap();
        self.check(&inner, client, sid, Some(file))?;
        let s = inner.states.get_mut(&sid.other).unwrap();
        let Kind::Open {
            access: a, deny: d, ..
        } = &mut s.kind
        else {
            return Err(err::BAD_STATEID);
        };
        if access & !*a != 0 || deny & !*d != 0 || access == 0 {
            return Err(err::INVAL);
        }
        *a = access;
        *d = deny;
        s.seqid += 1;
        Ok(Stateid {
            seqid: s.seqid,
            other: sid.other,
        })
    }

    /// CLOSE (refused while the owner still holds locks on the file).
    pub fn close(&self, client: u64, sid: &Stateid, file: FileId) -> Result<(), u32> {
        let mut inner = self.inner.lock().unwrap();
        self.check(&inner, client, sid, Some(file))?;
        if !matches!(inner.states[&sid.other].kind, Kind::Open { .. }) {
            return Err(err::BAD_STATEID);
        }
        let locks: Vec<([u8; 12], bool)> = inner
            .on_file(file)
            .filter_map(|(o, s)| match &s.kind {
                Kind::Lock { open, held, .. } if *open == sid.other => Some((*o, !held.is_empty())),
                _ => None,
            })
            .collect();
        if locks.iter().any(|(_, held)| *held) {
            return Err(err::LOCKS_HELD);
        }
        for (o, _) in locks {
            inner.remove_state(&o);
        }
        inner.remove_state(&sid.other);
        // Layouts are returned on the client's last close of the file.
        let still_open = inner
            .on_file(file)
            .any(|(_, s)| s.client == client && matches!(s.kind, Kind::Open { .. }));
        if !still_open {
            let layouts: Vec<[u8; 12]> = inner
                .on_file(file)
                .filter(|(_, s)| s.client == client && matches!(s.kind, Kind::Layout { .. }))
                .map(|(o, _)| *o)
                .collect();
            for o in layouts {
                inner.remove_state(&o);
            }
        }
        Ok(())
    }

    // ---- layouts ----

    /// LAYOUTGET: a layout of `file` for `client`, by its open, lock or
    /// layout stateid `by` (one layout stateid per client and file; asking
    /// again, or for read-write, updates it).
    pub fn layout_get(
        &self,
        client: u64,
        by: &Stateid,
        file: FileId,
        rw: bool,
    ) -> Result<Stateid, u32> {
        let mut inner = self.inner.lock().unwrap();
        self.check(&inner, client, by, Some(file))?;
        let existing = inner.on_file(file).find_map(|(o, s)| match s.kind {
            Kind::Layout { .. } if s.client == client => Some(*o),
            _ => None,
        });
        let other = match existing {
            Some(o) => o,
            None => {
                let o = self.other(&mut inner);
                inner.states.insert(
                    o,
                    State {
                        client,
                        file,
                        seqid: 0,
                        kind: Kind::Layout { rw },
                    },
                );
                inner.files.entry(file).or_default().push(o);
                o
            }
        };
        let s = inner.states.get_mut(&other).unwrap();
        if let Kind::Layout { rw: r } = &mut s.kind {
            *r |= rw;
        }
        s.seqid += 1;
        Ok(Stateid {
            seqid: s.seqid,
            other,
        })
    }

    /// LAYOUTRETURN of a file's layout (layouts are whole-file: any return
    /// drops it).
    pub fn layout_return(&self, client: u64, sid: &Stateid, file: FileId) -> Result<(), u32> {
        let mut inner = self.inner.lock().unwrap();
        self.check(&inner, client, sid, Some(file))?;
        if !matches!(inner.states[&sid.other].kind, Kind::Layout { .. }) {
            return Err(err::BAD_STATEID);
        }
        inner.remove_state(&sid.other);
        Ok(())
    }

    /// LAYOUTRETURN of every layout the client has.
    pub fn layout_return_all(&self, client: u64) {
        let mut inner = self.inner.lock().unwrap();
        let all: Vec<[u8; 12]> = inner
            .states
            .iter()
            .filter(|(_, s)| s.client == client && matches!(s.kind, Kind::Layout { .. }))
            .map(|(o, _)| *o)
            .collect();
        for o in all {
            inner.remove_state(&o);
        }
    }

    /// Whether `client` has a layout of `file`.
    pub fn has_layout(&self, client: u64, file: FileId) -> bool {
        let inner = self.inner.lock().unwrap();
        inner
            .on_file(file)
            .any(|(_, s)| s.client == client && matches!(s.kind, Kind::Layout { .. }))
    }

    /// Whether `file` is open (by anyone, here).
    pub fn is_open(&self, file: FileId) -> bool {
        let inner = self.inner.lock().unwrap();
        inner
            .on_file(file)
            .any(|(_, s)| matches!(s.kind, Kind::Open { .. }))
    }

    // ---- locks ----

    fn conflict(
        inner: &Inner,
        file: FileId,
        client: u64,
        owner: &[u8],
        r: &Range,
        write: bool,
    ) -> Option<Denied> {
        for (_, s) in inner.on_file(file) {
            if let Kind::Lock {
                owner: ow, held, ..
            } = &s.kind
            {
                if s.client == client && ow == owner {
                    continue;
                }
                for (h, w) in held {
                    if h.overlaps(r) && (write || *w) {
                        return Some(Denied {
                            range: *h,
                            write: *w,
                            client: s.client,
                            owner: ow.clone(),
                        });
                    }
                }
            }
        }
        None
    }

    /// LOCK, by a new lock owner (under `open`) or an existing lock
    /// stateid. Returns the lock stateid, or what's in the way.
    pub fn lock(
        &self,
        client: u64,
        file: FileId,
        by: Locker<'_>,
        r: Range,
        write: bool,
        reclaim: bool,
    ) -> Result<Result<Stateid, Denied>, u32> {
        let mut inner = self.inner.lock().unwrap();
        inner.grace_check(client, reclaim)?;
        let (other, owner) = match by {
            Locker::New { open, owner } => {
                self.check(&inner, client, &open, Some(file))?;
                if !matches!(inner.states[&open.other].kind, Kind::Open { .. }) {
                    return Err(err::BAD_STATEID);
                }
                // A lock owner has one stateid per file.
                let existing = inner.on_file(file).find_map(|(o, s)| match &s.kind {
                    Kind::Lock { owner: ow, .. } if s.client == client && ow == owner => Some(*o),
                    _ => None,
                });
                match existing {
                    Some(o) => (o, owner.to_vec()),
                    None => {
                        let o = self.other(&mut inner);
                        inner.states.insert(
                            o,
                            State {
                                client,
                                file,
                                seqid: 0,
                                kind: Kind::Lock {
                                    owner: owner.to_vec(),
                                    open: open.other,
                                    held: Vec::new(),
                                },
                            },
                        );
                        inner.files.entry(file).or_default().push(o);
                        (o, owner.to_vec())
                    }
                }
            }
            Locker::Existing(sid) => {
                self.check(&inner, client, &sid, Some(file))?;
                match &inner.states[&sid.other].kind {
                    Kind::Lock { owner, .. } => (sid.other, owner.clone()),
                    _ => return Err(err::BAD_STATEID),
                }
            }
        };
        if let Some(d) = Self::conflict(&inner, file, client, &owner, &r, write) {
            return Ok(Err(d));
        }
        let s = inner.states.get_mut(&other).unwrap();
        if let Kind::Lock { held, .. } = &mut s.kind {
            set_range(held, r, Some(write));
        }
        s.seqid += 1;
        Ok(Ok(Stateid {
            seqid: s.seqid,
            other,
        }))
    }

    /// LOCKT: what would be in the way of `owner`'s lock.
    pub fn test_lock(
        &self,
        client: u64,
        owner: &[u8],
        file: FileId,
        r: Range,
        write: bool,
    ) -> Option<Denied> {
        let inner = self.inner.lock().unwrap();
        Self::conflict(&inner, file, client, owner, &r, write)
    }

    /// LOCKU.
    pub fn unlock(
        &self,
        client: u64,
        sid: &Stateid,
        file: FileId,
        r: Range,
    ) -> Result<Stateid, u32> {
        let mut inner = self.inner.lock().unwrap();
        self.check(&inner, client, sid, Some(file))?;
        let s = inner.states.get_mut(&sid.other).unwrap();
        let Kind::Lock { held, .. } = &mut s.kind else {
            return Err(err::BAD_STATEID);
        };
        set_range(held, r, None);
        s.seqid += 1;
        Ok(Stateid {
            seqid: s.seqid,
            other: sid.other,
        })
    }

    /// Drop all state on a file (it was removed).
    pub fn forget_file(&self, file: FileId) {
        let mut inner = self.inner.lock().unwrap();
        for o in inner.files.remove(&file).unwrap_or_default() {
            inner.states.remove(&o);
        }
    }
}

/// Who's locking: a lock owner new to the file, or a lock stateid.
pub enum Locker<'a> {
    New { open: Stateid, owner: &'a [u8] },
    Existing(Stateid),
}

/// Set `r` in `held` to `kind` (`Some(write)`), or clear it (`None`),
/// splitting and merging ranges as POSIX locks do.
fn set_range(held: &mut Vec<(Range, bool)>, r: Range, kind: Option<bool>) {
    let mut out: Vec<(Range, bool)> = Vec::with_capacity(held.len() + 2);
    for (h, w) in held.drain(..) {
        if !h.overlaps(&r) {
            out.push((h, w));
            continue;
        }
        if h.start < r.start {
            out.push((
                Range {
                    start: h.start,
                    end: r.start,
                },
                w,
            ));
        }
        if h.end > r.end {
            out.push((
                Range {
                    start: r.end,
                    end: h.end,
                },
                w,
            ));
        }
    }
    if let Some(w) = kind {
        out.push((r, w));
    }
    out.sort_by_key(|(h, _)| h.start);
    // Merge neighbours of the same kind.
    for (h, w) in out {
        match held.last_mut() {
            Some((p, pw)) if *pw == w && p.end == h.start => p.end = h.end,
            _ => held.push((h, w)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn r(a: u64, b: u64) -> Range {
        Range { start: a, end: b }
    }

    #[test]
    fn ranges_split_and_merge() {
        let mut held = Vec::new();
        set_range(&mut held, r(0, 100), Some(true));
        set_range(&mut held, r(40, 60), Some(false));
        assert_eq!(
            held,
            vec![(r(0, 40), true), (r(40, 60), false), (r(60, 100), true)]
        );
        set_range(&mut held, r(40, 60), Some(true));
        assert_eq!(held, vec![(r(0, 100), true)]);
        set_range(&mut held, r(10, 20), None);
        assert_eq!(held, vec![(r(0, 10), true), (r(20, 100), true)]);
        set_range(&mut held, r(0, u64::MAX), None);
        assert!(held.is_empty());
        assert_eq!(Range::of(5, u64::MAX).unwrap(), r(5, u64::MAX));
        assert!(Range::of(u64::MAX - 1, 5).is_err());
        assert!(Range::of(1, 0).is_err());
    }

    fn client(t: &StateTable, owner: &[u8]) -> u64 {
        let x = t.exchange_id(owner, [1; 8], false).unwrap();
        t.create_session(x.clientid, x.sequenceid, 8, |_| Vec::new())
            .unwrap();
        x.clientid
    }

    #[test]
    fn share_reservations_and_locks_conflict_across_clients() {
        let t = StateTable::new();
        let (a, b) = (client(&t, b"a"), client(&t, b"b"));
        let oa = t
            .open(a, b"oa", 7, SHARE_READ | SHARE_WRITE, SHARE_WRITE, false)
            .unwrap();
        // b may read alongside, not write (a denies writes).
        assert_eq!(
            t.open(b, b"ob", 7, SHARE_WRITE, 0, false),
            Err(err::SHARE_DENIED)
        );
        let ob = t.open(b, b"ob", 7, SHARE_READ, 0, false).unwrap();
        // Byte-range locks.
        let la = t
            .lock(
                a,
                7,
                Locker::New {
                    open: oa,
                    owner: b"la",
                },
                r(0, 100),
                true,
                false,
            )
            .unwrap()
            .unwrap();
        let denied = t
            .lock(
                b,
                7,
                Locker::New {
                    open: ob,
                    owner: b"lb",
                },
                r(50, 60),
                false,
                false,
            )
            .unwrap()
            .unwrap_err();
        assert_eq!(
            (denied.range, denied.write, denied.client),
            (r(0, 100), true, a)
        );
        assert!(t.test_lock(b, b"lb", 7, r(100, 200), true).is_none());
        assert_eq!(t.close(a, &oa, 7), Err(err::LOCKS_HELD));
        let la = t.unlock(a, &la, 7, r(0, 100)).unwrap();
        assert_eq!(la.seqid, 2);
        assert!(t.test_lock(b, b"lb", 7, r(0, 10), true).is_none());
        // An old seqid is refused, the current one (or 0) works.
        assert_eq!(t.check_io(a, &Stateid { seqid: 0, ..oa }, 7, true), Ok(()));
        t.close(a, &oa, 7).unwrap();
        assert_eq!(t.check_io(a, &oa, 7, false), Err(err::BAD_STATEID));
        // Anonymous writes respect deny modes.
        let _ = t
            .open(b, b"ob2", 8, SHARE_READ, SHARE_WRITE, false)
            .unwrap();
        assert_eq!(
            t.check_io(a, &Stateid::ANONYMOUS, 8, true),
            Err(err::LOCKED)
        );
        assert_eq!(t.check_io(a, &Stateid::ANONYMOUS, 8, false), Ok(()));
    }

    #[test]
    fn slots_replay_and_misorder() {
        let t = StateTable::new();
        let x = t.exchange_id(b"c", [2; 8], false).unwrap();
        assert!(!x.confirmed);
        let (reply, newly) = t
            .create_session(x.clientid, x.sequenceid, 4, |sid| sid.to_vec())
            .unwrap();
        assert_eq!(newly.as_deref(), Some(&b"c"[..]), "confirmed: record it");
        let sid: [u8; 16] = reply.clone().try_into().unwrap();
        // CREATE_SESSION replayed: the same reply.
        assert_eq!(
            t.create_session(x.clientid, x.sequenceid, 4, |_| vec![9])
                .unwrap(),
            (reply.clone(), None)
        );
        assert!(t.exchange_id(b"c", [2; 8], false).unwrap().confirmed);

        assert!(matches!(t.sequence(&sid, 0, 1), Ok(Sequenced::New { .. })));
        assert_eq!(
            t.sequence(&sid, 0, 1).err(),
            Some(err::DELAY),
            "still running"
        );
        t.finish(&sid, 0, Some(b"cached".to_vec()));
        assert!(matches!(t.sequence(&sid, 0, 1), Ok(Sequenced::Replay(r)) if r == b"cached"));
        assert_eq!(t.sequence(&sid, 0, 5).err(), Some(err::SEQ_MISORDERED));
        assert_eq!(t.sequence(&sid, 9, 1).err(), Some(err::BADSLOT));
        assert!(matches!(t.sequence(&sid, 0, 2), Ok(Sequenced::New { .. })));
        t.finish(&sid, 0, None);
        assert_eq!(t.sequence(&sid, 0, 2).err(), Some(err::RETRY_UNCACHED_REP));

        // A reboot (new verifier) replaces the client once confirmed.
        let y = t.exchange_id(b"c", [3; 8], false).unwrap();
        assert_ne!(y.clientid, x.clientid);
        t.create_session(y.clientid, y.sequenceid, 4, |_| Vec::new())
            .unwrap();
        assert_eq!(t.sequence(&sid, 0, 3).err(), Some(err::BADSESSION));
        assert_eq!(t.destroy_clientid(y.clientid), Err(err::CLIENTID_BUSY));
    }
}
