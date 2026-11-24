//! The shared vocabulary: small types for values that are easy to mix up
//! as bare integers or strings. Portable (builds for wasm too).

use serde::{Deserialize, Serialize};
use std::fmt;

/// A cluster-wide fencing epoch: every lease grant, metaslab lend and
/// return takes a fresh, larger one (see `crate::cluster::state`). Disk
/// servers refuse writes stamped with an epoch below a domain's floor (see
/// `crate::fence`). [`Epoch::NONE`] is what a sender without a lease sends:
/// it is below every grant, so it never fences anyone out.
#[derive(
    Serialize, Deserialize, Clone, Copy, Debug, Default, PartialEq, Eq, Hash, PartialOrd, Ord,
)]
#[serde(transparent)]
pub struct Epoch(u64);

impl Epoch {
    /// No lease (below every granted epoch).
    pub const NONE: Epoch = Epoch(0);

    /// An epoch read off the wire or out of a record.
    pub const fn from_raw(v: u64) -> Self {
        Epoch(v)
    }

    pub const fn get(self) -> u64 {
        self.0
    }

    /// The next epoch to hand out after this one.
    pub fn next(self) -> Self {
        Epoch(self.0 + 1)
    }

    pub fn is_none(self) -> bool {
        self.0 == 0
    }

    pub fn to_le_bytes(self) -> [u8; 8] {
        self.0.to_le_bytes()
    }

    pub fn from_le_bytes(b: [u8; 8]) -> Self {
        Epoch(u64::from_le_bytes(b))
    }
}

/// Tests write epochs as literals.
#[cfg(test)]
impl From<u64> for Epoch {
    fn from(v: u64) -> Self {
        Epoch(v)
    }
}

#[cfg(test)]
impl PartialEq<u64> for Epoch {
    fn eq(&self, other: &u64) -> bool {
        self.0 == *other
    }
}

/// Shorthand for an epoch literal in tests.
#[cfg(test)]
pub const fn ep(v: u64) -> Epoch {
    Epoch(v)
}

/// A lease resource from its string form, in tests.
#[cfg(test)]
pub fn res(r: impl fmt::Display) -> LeaseResource {
    r.to_string().parse().expect("a lease resource")
}

/// Shorthand for a file id literal in tests.
#[cfg(test)]
pub const fn fid(v: u64) -> FileId {
    FileId(v)
}

/// Shorthand for a metaslab index literal in tests.
#[cfg(test)]
pub const fn ms(v: u32) -> MetaslabIdx {
    MetaslabIdx(v)
}

impl fmt::Display for Epoch {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl std::str::FromStr for Epoch {
    type Err = std::num::ParseIntError;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        s.parse().map(Epoch)
    }
}

/// A transaction group: the pool commits in TXGs, each stamped with the
/// next number. Blocks carry the TXG they were born in (see
/// `crate::blkptr`), so births compare as TXGs, never as other counters.
#[derive(
    Serialize, Deserialize, Clone, Copy, Debug, Default, PartialEq, Eq, Hash, PartialOrd, Ord,
)]
#[serde(transparent)]
pub struct Txg(u64);

impl Txg {
    /// Before the first TXG (a pool's first commit is TXG 1).
    pub const ZERO: Txg = Txg(0);

    /// After every TXG (as a hold: everything is held).
    pub const MAX: Txg = Txg(u64::MAX);

    pub const fn from_raw(v: u64) -> Self {
        Txg(v)
    }

    pub const fn get(self) -> u64 {
        self.0
    }

    pub fn next(self) -> Self {
        Txg(self.0 + 1)
    }

    /// `n` TXGs later.
    pub fn after(self, n: u64) -> Self {
        Txg(self.0 + n)
    }

    /// The one before (`ZERO` stays `ZERO`).
    pub fn prev(self) -> Self {
        Txg(self.0.saturating_sub(1))
    }

    /// Its slot in a ring of `n` (the uberblock ring).
    pub fn slot(self, n: u64) -> u64 {
        self.0 % n
    }

    /// How many TXGs `self` is past `earlier` (0 if it isn't).
    pub fn since(self, earlier: Txg) -> u64 {
        self.0.saturating_sub(earlier.0)
    }
}

impl fmt::Display for Txg {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl std::str::FromStr for Txg {
    type Err = std::num::ParseIntError;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        s.parse().map(Txg)
    }
}

#[cfg(test)]
impl From<u64> for Txg {
    fn from(v: u64) -> Self {
        Txg(v)
    }
}

#[cfg(test)]
impl PartialEq<u64> for Txg {
    fn eq(&self, other: &u64) -> bool {
        self.0 == *other
    }
}

#[cfg(test)]
impl PartialOrd<u64> for Txg {
    fn partial_cmp(&self, other: &u64) -> Option<std::cmp::Ordering> {
        self.0.partial_cmp(other)
    }
}

/// A metaslab of the pool, by position (see `crate::metaslab`): the unit
/// of space that is lent to writers and fenced on its own.
#[derive(
    Serialize, Deserialize, Clone, Copy, Debug, Default, PartialEq, Eq, Hash, PartialOrd, Ord,
)]
#[serde(transparent)]
pub struct MetaslabIdx(u32);

impl MetaslabIdx {
    pub const fn from_raw(v: u32) -> Self {
        MetaslabIdx(v)
    }

    pub const fn get(self) -> u32 {
        self.0
    }

    /// As an index into the pool's list of metaslabs.
    pub const fn index(self) -> usize {
        self.0 as usize
    }

    /// The metaslab at position `i` of the list.
    pub fn of_index(i: usize) -> Self {
        MetaslabIdx(u32::try_from(i).expect("fewer than 2^32 metaslabs"))
    }
}

impl fmt::Display for MetaslabIdx {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

#[cfg(test)]
impl From<u32> for MetaslabIdx {
    fn from(v: u32) -> Self {
        MetaslabIdx(v)
    }
}

/// A file, directory or other object of the namespace: its dnode's
/// number. Ids are never reused within a pool (see `crate::inode`).
#[derive(
    Serialize, Deserialize, Clone, Copy, Debug, Default, PartialEq, Eq, Hash, PartialOrd, Ord,
)]
#[serde(transparent)]
pub struct FileId(u64);

impl FileId {
    /// No object: ids start at 1 (a dnode's `parent` at the top).
    pub const NONE: FileId = FileId(0);

    pub const fn from_raw(v: u64) -> Self {
        FileId(v)
    }

    pub const fn get(self) -> u64 {
        self.0
    }

    pub const fn to_le_bytes(self) -> [u8; 8] {
        self.0.to_le_bytes()
    }

    pub const fn from_le_bytes(b: [u8; 8]) -> Self {
        FileId(u64::from_le_bytes(b))
    }
}

impl fmt::Display for FileId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

#[cfg(test)]
impl From<u64> for FileId {
    fn from(v: u64) -> Self {
        FileId(v)
    }
}

#[cfg(test)]
impl PartialEq<u64> for FileId {
    fn eq(&self, other: &u64) -> bool {
        self.0 == *other
    }
}

/// An amount of memory (or other storage), in bytes. Constructors and
/// accessors name the unit, so a KiB count can't pass for a MiB one.
#[derive(
    Serialize, Deserialize, Clone, Copy, Debug, Default, PartialEq, Eq, Hash, PartialOrd, Ord,
)]
#[serde(transparent)]
pub struct Bytes(u64);

impl Bytes {
    pub const fn new(bytes: u64) -> Self {
        Bytes(bytes)
    }

    pub const fn from_kib(kib: u64) -> Self {
        Bytes(kib.saturating_mul(1 << 10))
    }

    pub const fn from_mib(mib: u64) -> Self {
        Bytes(mib.saturating_mul(1 << 20))
    }

    pub const fn get(self) -> u64 {
        self.0
    }

    /// Whole KiB (rounded down).
    pub const fn kib(self) -> u64 {
        self.0 >> 10
    }

    /// Whole MiB (rounded down).
    pub const fn mib(self) -> u64 {
        self.0 >> 20
    }
}

/// A run of `len` blocks from block `start` (pool blocks unless said
/// otherwise). Always start and length, never start and end.
#[derive(Serialize, Deserialize, Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Extent {
    pub start: u64,
    pub len: u64,
}

impl Extent {
    pub const fn new(start: u64, len: u64) -> Self {
        Extent { start, len }
    }

    pub fn start(&self) -> u64 {
        self.start
    }

    pub fn len(&self) -> u64 {
        self.len
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// One past the last block.
    pub fn end(&self) -> u64 {
        self.start.saturating_add(self.len)
    }

    pub fn contains(&self, block: u64) -> bool {
        (self.start..self.end()).contains(&block)
    }
}

/// An extent of a block born in TXG `birth` (what frees need: a snapshot
/// holds blocks born at or before its TXG).
#[derive(Serialize, Deserialize, Debug, Clone, Copy, PartialEq, Eq)]
pub struct Born {
    pub extent: Extent,
    pub birth: Txg,
}

/// A node: its id is the pool position of its first disk (see
/// `crate::cluster::members`), its name `node-<id>`. Raft's node ids are
/// the same numbers.
#[derive(
    Serialize, Deserialize, Clone, Copy, Debug, Default, PartialEq, Eq, Hash, PartialOrd, Ord,
)]
#[serde(transparent)]
pub struct NodeId(u64);

impl NodeId {
    pub const fn from_raw(v: u64) -> Self {
        NodeId(v)
    }

    pub const fn get(self) -> u64 {
        self.0
    }

    /// `node-<id>`, the name it goes by everywhere.
    pub fn name(self) -> String {
        self.to_string()
    }
}

impl fmt::Display for NodeId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "node-{}", self.0)
    }
}

impl std::str::FromStr for NodeId {
    type Err = NameError;
    /// Parses a node's name (`node-<id>`).
    fn from_str(s: &str) -> Result<Self, NameError> {
        s.strip_prefix("node-")
            .and_then(|id| id.parse().ok())
            .map(NodeId)
            .ok_or_else(|| name_err("node", s, "node-<id>"))
    }
}

/// A claim to hold a lease whose resource the context gives (`pool`,
/// `ds/<name>`, `nfs4`, ...): `owner` at `epoch`. Checked against the
/// cluster's lease table, which only matches if that owner still holds it
/// at exactly that epoch.
#[derive(Serialize, Deserialize, Clone, Debug, Default, PartialEq, Eq, Hash)]
pub struct Holding {
    pub owner: String,
    pub epoch: Epoch,
}

impl Holding {
    pub fn new(owner: impl Into<String>, epoch: Epoch) -> Self {
        Holding {
            owner: owner.into(),
            epoch,
        }
    }
}

impl fmt::Display for Holding {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}@{}", self.owner, self.epoch)
    }
}

/// A lease named by its resource (`writer/<node>`, ...) and the epoch it
/// was granted at, whoever holds it: epochs are cluster-wide, so the pair
/// names one grant.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
pub struct LeaseRef {
    pub resource: LeaseResource,
    pub epoch: Epoch,
}

impl LeaseRef {
    pub fn new(resource: LeaseResource, epoch: Epoch) -> Self {
        LeaseRef { resource, epoch }
    }
}

impl fmt::Display for LeaseRef {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}@{}", self.resource, self.epoch)
    }
}

/// What a cluster lease is on. Each kind has one string form (`pool`,
/// `ds/<name>`, ...), which is how it's stored and sent.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum LeaseResource {
    /// The committer's: the one pool writer.
    Pool,
    /// The NFSv4 state owner's (see `crate::nfs::v4`).
    Nfs4,
    /// A dataset's writer: who runs volume / service `name`.
    Dataset(String),
    /// A node's agent, held for as long as it runs.
    Node(String),
    /// A writer process (an agent's, or a CLI tool's), named by node or
    /// process; metaslabs are lent to it.
    Writer(String),
    /// An object open for writing (an upload, an NFS write).
    Object(FileId),
}

impl LeaseResource {
    pub fn dataset(name: impl Into<String>) -> Self {
        LeaseResource::Dataset(name.into())
    }

    pub fn node(name: impl Into<String>) -> Self {
        LeaseResource::Node(name.into())
    }

    pub fn writer(name: impl Into<String>) -> Self {
        LeaseResource::Writer(name.into())
    }
}

impl fmt::Display for LeaseResource {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            LeaseResource::Pool => f.write_str("pool"),
            LeaseResource::Nfs4 => f.write_str("nfs4"),
            LeaseResource::Dataset(n) => write!(f, "ds/{}", n),
            LeaseResource::Node(n) => write!(f, "node/{}", n),
            LeaseResource::Writer(n) => write!(f, "writer/{}", n),
            LeaseResource::Object(id) => write!(f, "obj/{}", id),
        }
    }
}

impl std::str::FromStr for LeaseResource {
    type Err = NameError;
    fn from_str(s: &str) -> Result<Self, NameError> {
        let bad = || {
            name_err(
                "lease resource",
                s,
                "pool, nfs4, ds/, node/, writer/ or obj/<id>",
            )
        };
        Ok(match s.split_once('/') {
            None if s == "pool" => LeaseResource::Pool,
            None if s == "nfs4" => LeaseResource::Nfs4,
            Some((_, "")) | None => return Err(bad()),
            Some(("ds", n)) => LeaseResource::dataset(n),
            Some(("node", n)) => LeaseResource::node(n),
            Some(("writer", n)) => LeaseResource::writer(n),
            Some(("obj", id)) => LeaseResource::Object(FileId(id.parse().map_err(|_| bad())?)),
            Some(_) => return Err(bad()),
        })
    }
}

impl Serialize for LeaseResource {
    fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        s.collect_str(self)
    }
}

impl<'de> Deserialize<'de> for LeaseResource {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        let s = String::deserialize(d)?;
        s.parse().map_err(serde::de::Error::custom)
    }
}

/// What holds a lease (see [`Holder`]).
#[derive(Serialize, Deserialize, Clone, Copy, Debug, Default, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum HolderRole {
    /// A node's agent (it runs volumes, services, VMs).
    Agent,
    Committer,
    /// The NFSv4 state owner.
    Nfs4,
    /// A node's NFS server, writing an object.
    Nfs,
    /// A node's files API, writing an object (an upload).
    Files,
    /// The committer, replaying a gone holder's intent log.
    Replay,
    /// A command-line tool.
    #[default]
    Tool,
}

/// Who holds a lease, as it describes itself when taking it: what it is,
/// on which node, and where that node serves.
#[derive(Serialize, Deserialize, Clone, Debug, Default, PartialEq, Eq)]
pub struct Holder {
    pub role: HolderRole,
    /// The node it runs on (`node-<id>`), if it's a node's.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub node: Option<String>,
    /// Where that node's frontend listens (an agent's).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub addr: Option<String>,
    /// Anything else worth showing: a path, a command.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub detail: String,
}

impl Holder {
    pub fn new(role: HolderRole, node: Option<&str>) -> Self {
        Holder {
            role,
            node: node.map(str::to_string),
            ..Default::default()
        }
    }

    pub fn with_addr(mut self, addr: impl Into<String>) -> Self {
        self.addr = Some(addr.into());
        self
    }

    pub fn with_detail(mut self, detail: impl Into<String>) -> Self {
        self.detail = detail.into();
        self
    }

    /// Its node, if it has one and is `role`.
    pub fn node_as(&self, role: HolderRole) -> Option<&str> {
        self.node.as_deref().filter(|_| self.role == role)
    }
}

impl fmt::Display for Holder {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let role = match self.role {
            HolderRole::Agent => "agent",
            HolderRole::Committer => "committer",
            HolderRole::Nfs4 => "nfs4",
            HolderRole::Nfs => "nfs",
            HolderRole::Files => "files",
            HolderRole::Replay => "log replay",
            HolderRole::Tool => "tool",
        };
        f.write_str(role)?;
        if let Some(n) = &self.node {
            write!(f, " {}", n)?;
        }
        if let Some(a) = &self.addr {
            write!(f, " @{}", a)?;
        }
        if !self.detail.is_empty() {
            write!(f, " {}", self.detail)?;
        }
        Ok(())
    }
}

/// Milliseconds since the Unix epoch (wasm too).
pub fn now_ms() -> u64 {
    web_time::SystemTime::now()
        .duration_since(web_time::UNIX_EPOCH)
        .map_or(0, |d| d.as_millis() as u64)
}

// ---- Names ----
//
// Each kind of name is checked once, when it is made (parsed from a
// request, a command line, or a record), and is then known to be valid
// wherever it goes: into a lease resource, an NQN, libvirt XML, a URL.

/// A name that broke its kind's rules.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NameError {
    pub kind: &'static str,
    pub name: String,
    pub rule: &'static str,
}

impl fmt::Display for NameError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "bad {} name '{}' ({})", self.kind, self.name, self.rule)
    }
}

impl std::error::Error for NameError {}

fn name_err(kind: &'static str, name: &str, rule: &'static str) -> NameError {
    NameError {
        kind,
        name: name.to_string(),
        rule,
    }
}

fn plain_chars(name: &str, extra: &[u8]) -> bool {
    name.bytes().all(|b| {
        b.is_ascii_alphanumeric() || b == b'_' || b == b'-' || b == b'.' || extra.contains(&b)
    })
}

/// Defines a validated name newtype: `$check(&str) -> Result<(), NameError>`
/// is the only way in.
macro_rules! name_type {
    ($(#[$m:meta])* $t:ident, $check:path) => {
        $(#[$m])*
        #[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
        #[serde(try_from = "String", into = "String")]
        pub struct $t(String);

        impl $t {
            pub fn as_str(&self) -> &str {
                &self.0
            }
        }

        impl TryFrom<String> for $t {
            type Error = NameError;
            fn try_from(s: String) -> Result<Self, NameError> {
                $check(&s)?;
                Ok($t(s))
            }
        }

        impl TryFrom<&str> for $t {
            type Error = NameError;
            fn try_from(s: &str) -> Result<Self, NameError> {
                $check(s)?;
                Ok($t(s.to_string()))
            }
        }

        impl std::str::FromStr for $t {
            type Err = NameError;
            fn from_str(s: &str) -> Result<Self, NameError> {
                $t::try_from(s)
            }
        }

        impl From<$t> for String {
            fn from(n: $t) -> String {
                n.0
            }
        }

        impl std::ops::Deref for $t {
            type Target = str;
            fn deref(&self) -> &str {
                &self.0
            }
        }

        impl AsRef<str> for $t {
            fn as_ref(&self) -> &str {
                &self.0
            }
        }

        impl std::borrow::Borrow<str> for $t {
            fn borrow(&self) -> &str {
                &self.0
            }
        }

        impl fmt::Display for $t {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str(&self.0)
            }
        }

        impl PartialEq<str> for $t {
            fn eq(&self, o: &str) -> bool {
                self.0 == o
            }
        }

        impl PartialEq<&str> for $t {
            fn eq(&self, o: &&str) -> bool {
                self.0 == *o
            }
        }
    };
}

/// Longest dataset (volume, bucket, queue, ...) name, in bytes.
pub const MAX_DATASET_NAME: usize = 128;

const DATASET_RULE: &str = "1 to 128 of A-Z a-z 0-9 . _ -, starting with a letter, digit or _";

fn check_user_dataset(name: &str) -> Result<(), NameError> {
    let first_ok = name
        .bytes()
        .next()
        .is_some_and(|b| b.is_ascii_alphanumeric() || b == b'_');
    if first_ok && name.len() <= MAX_DATASET_NAME && plain_chars(name, &[]) {
        Ok(())
    } else {
        Err(name_err("dataset", name, DATASET_RULE))
    }
}

fn check_dataset(name: &str) -> Result<(), NameError> {
    match name.strip_prefix('.') {
        // Raidish's own datasets (`.vms`, `.events`): a name no user can pick.
        Some(rest) if name.len() <= MAX_DATASET_NAME => {
            check_user_dataset(rest).map_err(|_| name_err("dataset", name, DATASET_RULE))
        }
        Some(_) => Err(name_err("dataset", name, DATASET_RULE)),
        None => check_user_dataset(name),
    }
}

name_type!(
    /// A dataset's name: a volume, a filesystem, a service (bucket, queue,
    /// etcd, kv), or one of raidish's own (`.`-prefixed: see
    /// [`DatasetName::is_system`]; users can't make those, see
    /// [`DatasetName::user`]).
    DatasetName,
    check_dataset
);

impl DatasetName {
    /// A name a user may give a new dataset (not a system one).
    pub fn user(name: &str) -> Result<Self, NameError> {
        check_user_dataset(name)?;
        Ok(DatasetName(name.to_string()))
    }

    /// One of raidish's own datasets.
    pub fn is_system(&self) -> bool {
        self.0.starts_with('.')
    }
}

/// Longest VM name, in bytes.
pub const MAX_VM_NAME: usize = 64;

fn check_vm(name: &str) -> Result<(), NameError> {
    let first_ok = name
        .bytes()
        .next()
        .is_some_and(|b| b.is_ascii_alphanumeric() || b == b'_');
    if first_ok && name.len() <= MAX_VM_NAME && plain_chars(name, &[]) {
        Ok(())
    } else {
        Err(name_err(
            "VM",
            name,
            "1 to 64 of A-Z a-z 0-9 . _ -, starting with a letter, digit or _",
        ))
    }
}

name_type!(
    /// A VM's name (its libvirt domain's, and its definition's in the pool).
    VmName,
    check_vm
);

fn check_snapshot(name: &str) -> Result<(), NameError> {
    // Not starting with `-` (an option to a tool) or `.` (`..`).
    let first_ok = name
        .bytes()
        .next()
        .is_some_and(|b| b.is_ascii_alphanumeric() || b == b'_');
    if first_ok && name.len() <= 128 && plain_chars(name, b":") {
        Ok(())
    } else {
        Err(name_err(
            "snapshot",
            name,
            "1 to 128 of A-Z a-z 0-9 - _ . :, starting with a letter, digit or _",
        ))
    }
}

name_type!(
    /// A snapshot's own name (the part after `@`).
    SnapshotName,
    check_snapshot
);

/// A snapshot of a volume: `volume@snapshot`.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
#[serde(try_from = "String", into = "String")]
pub struct SnapshotRef {
    pub volume: DatasetName,
    pub snap: SnapshotName,
}

impl SnapshotRef {
    pub fn new(volume: DatasetName, snap: SnapshotName) -> Self {
        SnapshotRef { volume, snap }
    }
}

impl std::str::FromStr for SnapshotRef {
    type Err = NameError;
    fn from_str(full: &str) -> Result<Self, NameError> {
        let Some((v, s)) = full.split_once('@') else {
            return Err(name_err("snapshot", full, "expected volume@snapshot"));
        };
        Ok(SnapshotRef {
            volume: v.parse()?,
            snap: s.parse()?,
        })
    }
}

impl TryFrom<String> for SnapshotRef {
    type Error = NameError;
    fn try_from(s: String) -> Result<Self, NameError> {
        s.parse()
    }
}

impl From<SnapshotRef> for String {
    fn from(r: SnapshotRef) -> String {
        r.to_string()
    }
}

impl fmt::Display for SnapshotRef {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}@{}", self.volume, self.snap)
    }
}

fn check_bucket(name: &str) -> Result<(), NameError> {
    let b = name.as_bytes();
    let ok = (3..=63).contains(&b.len())
        && b.iter()
            .all(|&c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == b'-' || c == b'.')
        && b[0].is_ascii_alphanumeric()
        && b[b.len() - 1].is_ascii_alphanumeric()
        && !name.contains("..")
        && name.parse::<std::net::Ipv4Addr>().is_err();
    if ok {
        Ok(())
    } else {
        Err(name_err(
            "bucket",
            name,
            "3 to 63 lowercase letters, digits, '.' and '-'",
        ))
    }
}

name_type!(
    /// An S3 bucket's name (S3's rules: those that work as host names).
    /// Always a valid [`DatasetName`] too.
    BucketName,
    check_bucket
);

fn check_queue(name: &str) -> Result<(), NameError> {
    if name.ends_with(".fifo") {
        return Err(name_err("queue", name, "FIFO queues aren't supported"));
    }
    let ok = (1..=80).contains(&name.len())
        && name
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_')
        && check_user_dataset(name).is_ok();
    if ok {
        Ok(())
    } else {
        Err(name_err(
            "queue",
            name,
            "1 to 80 letters, digits, '-' and '_', not starting with '-'",
        ))
    }
}

name_type!(
    /// An SQS queue's name. Always a valid [`DatasetName`] too.
    QueueName,
    check_queue
);

impl From<BucketName> for DatasetName {
    fn from(b: BucketName) -> Self {
        DatasetName(b.0)
    }
}

impl From<QueueName> for DatasetName {
    fn from(q: QueueName) -> Self {
        DatasetName(q.0)
    }
}

#[cfg(test)]
#[path = "tests/types_tests.rs"]
mod tests;

/// What sort of failure an error is: what every protocol (HTTP, NFS, S3,
/// SQS, etcd, CSI) maps its status from, and what crosses the wire with a
/// refusal so the far side knows it too.
#[derive(Serialize, Deserialize, Clone, Copy, Debug, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
pub enum ErrorKind {
    NotFound,
    Exists,
    /// A directory (or bucket, ...) that still has entries.
    NotEmpty,
    NotDir,
    /// A directory (or volume, ...) where a file was wanted.
    NotFile,
    /// Malformed or impossible: asking again won't help.
    Invalid,
    /// The target changed meanwhile, or is someone else's right now.
    Conflict,
    /// Not now (no committer, no quorum): it may be asked again.
    Unavailable,
    NoSpace,
    /// Not allowed for whoever asked.
    Forbidden,
    /// Something broke (a disk, a checksum, a bug).
    Internal,
}

impl ErrorKind {
    /// The HTTP status it is answered with.
    pub fn http_status(self) -> u16 {
        match self {
            ErrorKind::NotFound => 404,
            ErrorKind::Exists | ErrorKind::NotEmpty | ErrorKind::Conflict => 409,
            ErrorKind::NotDir | ErrorKind::NotFile | ErrorKind::Invalid => 400,
            ErrorKind::Unavailable => 503,
            ErrorKind::NoSpace => 507,
            ErrorKind::Forbidden => 403,
            ErrorKind::Internal => 500,
        }
    }
}

/// What an object is. In JSON it's named as `name` gives it (on disk,
/// bincode stores the variant's index).
#[derive(Serialize, Deserialize, Debug, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum ObjType {
    File,
    #[serde(rename = "dir")]
    Directory,
    /// Copy-on-write virtual block device (see `crate::volume`).
    Volume,
    /// A volume as it was at some TXG, read-only (see `crate::snapshot`).
    /// No directory links it: its volume's dnode does.
    Snapshot,
    /// The dataset directory (see `crate::dataset`): the uberblock links it.
    Datasets,
    /// A `kv` dataset's B+tree (see `crate::btree`): its records are the
    /// tree's nodes.
    #[serde(rename = "kv")]
    KvTree,
}

impl ObjType {
    pub fn name(&self) -> &'static str {
        match self {
            ObjType::File => "file",
            ObjType::Directory => "dir",
            ObjType::Volume => "volume",
            ObjType::Snapshot => "snapshot",
            ObjType::Datasets => "datasets",
            ObjType::KvTree => "kv",
        }
    }
}

/// How a volume is exported to the kernel when it is auto-mounted.
#[derive(Debug, Serialize, Deserialize, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum Transport {
    Nbd,
    Nvme,
}

impl std::str::FromStr for Transport {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, String> {
        match s {
            "nbd" => Ok(Transport::Nbd),
            "nvme" => Ok(Transport::Nvme),
            _ => Err(format!("unknown transport '{}' (nbd or nvme)", s)),
        }
    }
}

impl std::fmt::Display for Transport {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Transport::Nbd => "nbd",
            Transport::Nvme => "nvme",
        })
    }
}

/// Placement of a tree dataset across the cluster's `listen` nodes.
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub struct DatasetProps {
    /// Nodes (by name) that may run the dataset, most preferred first. It
    /// runs on the first live one and stays there (no fail-back) until
    /// moved. Empty: any node.
    pub nodes: Vec<String>,
    /// A volume's: export it and attach the kernel device wherever it runs.
    /// None: only run by hand (`nbd` / `nvme`).
    pub automount: Option<Transport>,
    /// A disabled dataset isn't placed anywhere.
    pub enabled: bool,
    /// Asked to move to this node (`volume move`): it goes there while that
    /// node is live, and the node clears this once it runs the volume.
    pub move_to: Option<String>,
    /// An `etcd` dataset's: the port every node takes its clients on (they
    /// reach it wherever it runs, see `crate::frontend`).
    #[serde(default)]
    pub port: Option<u16>,
}

impl Default for DatasetProps {
    fn default() -> Self {
        DatasetProps {
            nodes: Vec::new(),
            automount: None,
            enabled: true,
            move_to: None,
            port: None,
        }
    }
}

/// A snapshot as listed.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SnapshotInfo {
    /// `volume@snapshot`.
    pub name: String,
    pub txg: Txg,
    /// Names these contents in any pool (kept by send/receive).
    pub guid: u64,
    /// When it was taken, ns since the epoch.
    pub created_ns: u64,
    /// Bytes only it references (freed if it's destroyed).
    pub unique_bytes: u64,
    /// Volumes cloned from it.
    pub clones: Vec<String>,
}

impl SnapshotInfo {
    /// The snapshot's own name (after the `@`).
    pub fn short(&self) -> &str {
        self.name.split_once('@').map_or(&self.name, |(_, s)| s)
    }
}

/// A volume as listed (see `FileSystem::list_volumes`).
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub struct VolumeInfo {
    pub name: String,
    pub size_bytes: u64,
    pub volblocksize: u32,
    pub props: DatasetProps,
}

/// How a dataset's contents are laid out.
#[derive(Serialize, Deserialize, Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum DatasetFormat {
    /// Directories and files; the root is a directory. Changed through the
    /// committer, object by object (`crate::writer::FsOp`).
    Posix,
    /// A virtual block device (a volume, see `crate::volume`): a fixed-size
    /// tree of `volblocksize` records.
    Block,
    /// Byte-string keys to values, in a B+tree whose nodes are the root
    /// object's records (see `crate::kv`, `crate::btree`).
    Kv,
    /// An etcd v3 keyspace (revisions, history, leases; see `crate::etcd`)
    /// in a B+tree.
    Etcd,
    /// An S3 bucket (objects and their bodies, uploads; see `crate::s3`)
    /// in a B+tree.
    S3,
    /// An SQS queue (messages, their visibility; see `crate::sqs`) in a
    /// B+tree.
    Queue,
    /// The cluster's activity log (see `crate::events`): one per pool,
    /// written by the committer only, in a B+tree.
    Events,
    /// The cluster's VM definitions (see `crate::vm_store`): one per
    /// pool, written by the committer only (each one checked as it's
    /// stored), in a B+tree.
    Vms,
}

impl DatasetFormat {
    pub fn name(&self) -> &'static str {
        match self {
            DatasetFormat::Posix => "posix",
            DatasetFormat::Block => "block",
            DatasetFormat::Kv => "kv",
            DatasetFormat::Etcd => "etcd",
            DatasetFormat::S3 => "s3",
            DatasetFormat::Queue => "queue",
            DatasetFormat::Events => "events",
            DatasetFormat::Vms => "vms",
        }
    }

    /// Whether its root object holds a B+tree (see `crate::btree`).
    pub fn is_btree(&self) -> bool {
        matches!(
            self,
            DatasetFormat::Kv
                | DatasetFormat::Etcd
                | DatasetFormat::S3
                | DatasetFormat::Queue
                | DatasetFormat::Events
                | DatasetFormat::Vms
        )
    }

    /// Whether a front end serves it (see `crate::frontend`).
    pub fn is_service(&self) -> bool {
        matches!(
            self,
            DatasetFormat::Etcd | DatasetFormat::S3 | DatasetFormat::Queue
        )
    }

    /// Whether the pool keeps it for itself (not listed, made or deleted
    /// by users).
    pub fn is_system(&self) -> bool {
        matches!(self, DatasetFormat::Events | DatasetFormat::Vms)
    }

    /// Whether its contents are its root object's tree, run as a whole by
    /// one writer (see the module docs).
    pub fn is_tree(&self) -> bool {
        !matches!(self, DatasetFormat::Posix)
    }

    /// The kind of its root object.
    pub fn root_kind(&self) -> ObjType {
        match self {
            DatasetFormat::Posix => ObjType::Directory,
            DatasetFormat::Block => ObjType::Volume,
            f if f.is_btree() => ObjType::KvTree,
            _ => unreachable!(),
        }
    }
}

/// A fencing domain: the pool, or one metaslab.
#[derive(Serialize, Deserialize, Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum Domain {
    Pool,
    Metaslab(MetaslabIdx),
}

/// A metaslab's fencing grant, as the cluster recorded it.
#[derive(Serialize, Deserialize, Clone, Copy, Debug, PartialEq, Eq)]
pub enum Grant {
    /// Lent to a writer at this epoch: only that lend's writes are admitted
    /// (a new committer doesn't change it).
    Lent(Epoch),
    /// Back with the committer since this epoch: the pool floor applies too.
    Returned(Epoch),
}

impl Grant {
    pub fn epoch(self) -> Epoch {
        match self {
            Grant::Lent(e) | Grant::Returned(e) => e,
        }
    }
}

/// What learns the cluster's fencing grants as they're applied (a disk
/// server's `crate::fence::ServerFence`): the cluster tells it, so the
/// cluster needs nothing of the storage core.
pub trait FenceObserver: Send + Sync {
    /// The pool epoch and the metaslabs' grants, as of the state applied.
    fn observe_grants(
        &self,
        pool_epoch: Epoch,
        grants: &mut dyn Iterator<Item = (MetaslabIdx, Grant)>,
    );
}

/// 64 random bits drawn once per process from the OS (audit R4c).
#[cfg(not(target_arch = "wasm32"))]
pub fn process_seed() -> u64 {
    static SEED: std::sync::OnceLock<u64> = std::sync::OnceLock::new();
    *SEED.get_or_init(|| {
        use ring::rand::SecureRandom;
        let mut b = [0u8; 8];
        ring::rand::SystemRandom::new()
            .fill(&mut b)
            .expect("the OS random source");
        u64::from_le_bytes(b)
    })
}
