//! Sending and receiving volume snapshots (replication between pools).
//!
//! `send` turns a snapshot into a stream; `recv` makes it a snapshot of a
//! volume in another pool (or this one). A *full* stream carries every
//! written record. An *incremental* one, from an older snapshot of the same
//! volume, carries only what changed in between, and is received onto a
//! volume whose newest snapshot is that older one (matched by GUID: TXG
//! numbers differ between pools).
//!
//! ```text
//!   "RAIDSEND" | version u32 | header length u32 | header (bincode)
//!   frames:  1 | index u64 | length u32 | record bytes    (data)
//!            2 | first u64 | count u64                    (records now holes)
//!            0 | data frames u64 | blake3 of everything before this frame
//! ```
//!
//! The sender walks the two snapshots' trees together. A subtree that's the
//! same block in both is skipped unread, so an incremental costs what
//! changed, not the volume's size. Where the new snapshot has a hole and the
//! old one had records, it sends those records as holes. Receiving writes
//! the records into the volume as ordinary writes, then takes the snapshot
//! under the sender's name and GUID. Received data is only committed for
//! good at the end: if the stream is cut short, corrupt or refused midway,
//! the volume is rolled back to the base snapshot (incremental), or deleted
//! (full).

use crate::blkptr::BlockPtr;
use crate::disk::PoolDevice;
use crate::fs::{FileSystem, FileSystemError};
use crate::object::{Dirty, INDIRECT_FANOUT, Indirect, ObjTree, Sizing, TreeRoot};
use crate::role::{Committer, Role};
use crate::types::DatasetName;
use crate::types::Txg;
use crate::volume::DIRTY_LIMIT_BYTES;
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

const MAGIC: &[u8; 8] = b"RAIDSEND";
const VERSION: u32 = 1;
const END: u8 = 0;
const DATA: u8 = 1;
const HOLE: u8 = 2;
/// Records read from the pool at a time.
const READ_BATCH: usize = 64;
const MAX_HEADER: u32 = 64 << 10;

/// What a stream is of.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct SendHeader {
    /// The dataset's format (its records mean what that format says).
    pub format: crate::dataset::DatasetFormat,
    /// The root object's size and record size.
    pub size_bytes: u64,
    pub volblocksize: u32,
    /// The snapshot's short name and GUID, and the TXG it had where it was
    /// sent from (informative).
    pub snapshot: String,
    pub guid: u64,
    pub txg: Txg,
    /// When it was taken, ns since the epoch.
    pub created_ns: u64,
    /// For an incremental stream: the snapshot it applies on top of.
    pub from: Option<SendBase>,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct SendBase {
    pub snapshot: String,
    pub guid: u64,
}

/// What a send or receive moved.
#[derive(Serialize, Deserialize, Debug, Clone, Default, PartialEq, Eq)]
pub struct StreamReport {
    /// `volume@snapshot` (the received one, on receive).
    pub snapshot: String,
    pub incremental: bool,
    pub data_records: u64,
    pub data_bytes: u64,
    pub hole_records: u64,
}

impl std::fmt::Display for StreamReport {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{} ({}): {} records ({:.1} MiB), {} records now holes",
            self.snapshot,
            if self.incremental {
                "incremental"
            } else {
                "full"
            },
            self.data_records,
            self.data_bytes as f64 / (1u64 << 20) as f64,
            self.hole_records
        )
    }
}

fn io(e: std::io::Error) -> FileSystemError {
    FileSystemError::Unavailable(format!("stream: {}", e))
}

fn bad(why: impl Into<String>) -> FileSystemError {
    FileSystemError::InvalidArgument(format!("send stream: {}", why.into()))
}

/// Writes frames, hashing everything it writes.
struct Out<'a, W> {
    w: &'a mut W,
    hash: blake3::Hasher,
}

impl<W: AsyncWrite + Unpin + Send> Out<'_, W> {
    async fn put(&mut self, b: &[u8]) -> Result<(), FileSystemError> {
        self.hash.update(b);
        self.w.write_all(b).await.map_err(io)
    }
}

/// Reads frames, hashing everything it reads.
struct In<'a, R> {
    r: &'a mut R,
    hash: blake3::Hasher,
}

impl<R: AsyncRead + Unpin + Send> In<'_, R> {
    async fn get(&mut self, n: usize) -> Result<Vec<u8>, FileSystemError> {
        let mut b = vec![0u8; n];
        self.r.read_exact(&mut b).await.map_err(io)?;
        self.hash.update(&b);
        Ok(b)
    }
    async fn u8(&mut self) -> Result<u8, FileSystemError> {
        Ok(self.get(1).await?[0])
    }
    async fn u32(&mut self) -> Result<u32, FileSystemError> {
        Ok(u32::from_le_bytes(self.get(4).await?.try_into().unwrap()))
    }
    async fn u64(&mut self) -> Result<u64, FileSystemError> {
        Ok(u64::from_le_bytes(self.get(8).await?.try_into().unwrap()))
    }
}

/// Data frames waiting to be read from the pool and written out.
struct Pending {
    records: Vec<(u64, BlockPtr)>,
    /// Holes since the last data frame, as (first, count) runs.
    hole: Option<(u64, u64)>,
}

impl<D: PoolDevice, R: Role> FileSystem<D, R> {
    /// Snapshot `name@snap` and its dnode's tree, size and record size.
    async fn snapshot_tree(
        &self,
        name: &DatasetName,
        snap: &str,
    ) -> Result<(crate::inode::SnapRef, TreeRoot, u64, u32), FileSystemError> {
        let id = self.dataset_id(name).await?;
        let head = self.read_dnode(id).await?;
        let Some(h) = head.head() else {
            return Err(FileSystemError::FileNotFound);
        };
        let r = h
            .snapshots
            .iter()
            .find(|s| s.name.split_once('@').map(|(_, s)| s) == Some(snap))
            .cloned()
            .ok_or(FileSystemError::FileNotFound)?;
        let d = self.read_dnode(r.id).await?;
        Ok((r, d.tree, d.size, d.blksz))
    }

    /// Write snapshot `name@snap` to `w` as a stream: everything (`from`
    /// None), or what changed since the older snapshot `name@from`.
    pub async fn send_snapshot<W: AsyncWrite + Unpin + Send>(
        &self,
        name: &DatasetName,
        snap: &str,
        from: Option<&str>,
        w: &mut W,
    ) -> Result<StreamReport, FileSystemError> {
        let (to_ref, to, size, blksz) = self.snapshot_tree(name, snap).await?;
        let base = match from {
            Some(f) => {
                let (r, t, fsize, fblksz) = self.snapshot_tree(name, f).await?;
                if r.txg >= to_ref.txg {
                    return Err(FileSystemError::InvalidArgument(format!(
                        "{}@{} isn't older than {}@{}",
                        name, f, name, snap
                    )));
                }
                if (fsize, fblksz, t.levels) != (size, blksz, to.levels) {
                    return Err(FileSystemError::InvalidArgument(
                        "the snapshots' shapes differ".into(),
                    ));
                }
                Some((r, t))
            }
            None => None,
        };
        let header = SendHeader {
            format: self.dataset_entry(name).await?.format,
            size_bytes: size,
            volblocksize: blksz,
            snapshot: snap.to_string(),
            guid: to_ref.guid,
            txg: to_ref.txg,
            created_ns: self.read_dnode(to_ref.id).await?.attrs.ctime_ns,
            from: base.as_ref().map(|(r, _)| SendBase {
                snapshot: from.unwrap().to_string(),
                guid: r.guid,
            }),
        };
        let mut report = StreamReport {
            snapshot: to_ref.name.clone(),
            incremental: base.is_some(),
            ..Default::default()
        };
        let mut out = Out {
            w,
            hash: blake3::Hasher::new(),
        };
        let h = bincode::serialize(&header)?;
        out.put(MAGIC).await?;
        out.put(&VERSION.to_le_bytes()).await?;
        out.put(&(h.len() as u32).to_le_bytes()).await?;
        out.put(&h).await?;

        // Scratch trees for their indirect caches.
        let mine = ObjTree::new(blksz as usize, Sizing::Fixed, size, to.clone());
        let theirs = ObjTree::new(
            blksz as usize,
            Sizing::Fixed,
            size,
            TreeRoot {
                ptr: None,
                levels: to.levels,
            },
        );
        let mut pending = Pending {
            records: Vec::new(),
            hole: None,
        };
        // (new, old, level, index at that level); index order.
        let mut stack: Vec<(Option<BlockPtr>, Option<BlockPtr>, u8, u64)> = vec![(
            to.ptr.clone(),
            base.as_ref().and_then(|(_, t)| t.ptr.clone()),
            to.levels,
            0,
        )];
        while let Some((x, f, level, index)) = stack.pop() {
            if x == f {
                continue;
            }
            match (x, f) {
                (None, Some(f)) => {
                    // A hole now: every record the old subtree had.
                    for i in self.subtree_leaves(&theirs, f, level, index).await? {
                        self.send_hole(&mut out, &mut pending, &mut report, i)
                            .await?;
                    }
                }
                (Some(x), _) if level == 0 => {
                    if let Some((first, count)) = pending.hole.take() {
                        write_hole(&mut out, first, count).await?;
                    }
                    pending.records.push((index, x));
                    if pending.records.len() >= READ_BATCH {
                        self.send_records(&mut out, &mut pending, &mut report)
                            .await?;
                    }
                }
                (Some(x), f) => {
                    let xi = self.load_indirect(&mine, &x).await?;
                    let fi: Option<std::sync::Arc<Indirect>> = match &f {
                        Some(f) => Some(self.load_indirect(&theirs, f).await?),
                        None => None,
                    };
                    for slot in (0..INDIRECT_FANOUT as usize).rev() {
                        let xc = xi.get(slot).cloned().flatten();
                        let fc = fi.as_ref().and_then(|i| i.get(slot).cloned().flatten());
                        if xc.is_some() || fc.is_some() {
                            stack.push((xc, fc, level - 1, index * INDIRECT_FANOUT + slot as u64));
                        }
                    }
                }
                (None, None) => {}
            }
        }
        self.send_records(&mut out, &mut pending, &mut report)
            .await?;
        if let Some((first, count)) = pending.hole.take() {
            write_hole(&mut out, first, count).await?;
        }
        let digest = out.hash.finalize();
        out.put(&[END]).await?;
        out.w
            .write_all(&report.data_records.to_le_bytes())
            .await
            .map_err(io)?;
        out.w.write_all(digest.as_bytes()).await.map_err(io)?;
        out.w.flush().await.map_err(io)?;
        Ok(report)
    }

    /// The record indices under node `ptr` at `level`, index `index`.
    async fn subtree_leaves(
        &self,
        obj: &ObjTree,
        ptr: BlockPtr,
        level: u8,
        index: u64,
    ) -> Result<Vec<u64>, FileSystemError> {
        let mut out = Vec::new();
        let mut stack = vec![(ptr, level, index)];
        while let Some((p, l, i)) = stack.pop() {
            if l == 0 {
                out.push(i);
                continue;
            }
            let ind = self.load_indirect(obj, &p).await?;
            for (slot, c) in ind.iter().enumerate().rev() {
                if let Some(c) = c {
                    stack.push((c.clone(), l - 1, i * INDIRECT_FANOUT + slot as u64));
                }
            }
        }
        Ok(out)
    }

    async fn send_hole<W: AsyncWrite + Unpin + Send>(
        &self,
        out: &mut Out<'_, W>,
        pending: &mut Pending,
        report: &mut StreamReport,
        index: u64,
    ) -> Result<(), FileSystemError> {
        // Data before it goes out first (frames stay in index order).
        self.send_records(out, pending, report).await?;
        report.hole_records += 1;
        match &mut pending.hole {
            Some((first, count)) if *first + *count == index => *count += 1,
            _ => {
                if let Some((first, count)) = pending.hole.replace((index, 1)) {
                    write_hole(out, first, count).await?;
                }
            }
        }
        Ok(())
    }

    async fn send_records<W: AsyncWrite + Unpin + Send>(
        &self,
        out: &mut Out<'_, W>,
        pending: &mut Pending,
        report: &mut StreamReport,
    ) -> Result<(), FileSystemError> {
        if pending.records.is_empty() {
            return Ok(());
        }
        let records = std::mem::take(&mut pending.records);
        let ptrs: Vec<BlockPtr> = records.iter().map(|(_, p)| p.clone()).collect();
        let datas = self.read_records(&ptrs).await?;
        for ((idx, _), data) in records.iter().zip(datas) {
            out.put(&[DATA]).await?;
            out.put(&idx.to_le_bytes()).await?;
            out.put(&(data.len() as u32).to_le_bytes()).await?;
            out.put(&data).await?;
            report.data_records += 1;
            report.data_bytes += data.len() as u64;
        }
        Ok(())
    }
}

async fn read_header<R: AsyncRead + Unpin + Send>(
    inp: &mut In<'_, R>,
) -> Result<SendHeader, FileSystemError> {
    if inp.get(8).await?.as_slice() != MAGIC {
        return Err(bad("not a send stream"));
    }
    let version = inp.u32().await?;
    if version != VERSION {
        return Err(bad(format!(
            "version {} (this build reads {})",
            version, VERSION
        )));
    }
    let len = inp.u32().await?;
    if len > MAX_HEADER {
        return Err(bad("header too large"));
    }
    Ok(bincode::deserialize(&inp.get(len as usize).await?)?)
}

/// Read a stream's header from `r`: the header, and the bytes read (to
/// send on ahead of the rest).
pub async fn peek_header<R: AsyncRead + Unpin + Send>(
    r: &mut R,
) -> Result<(SendHeader, Vec<u8>), FileSystemError> {
    let mut raw = Vec::new();
    let mut tee = Tee { r, raw: &mut raw };
    let mut inp = In {
        r: &mut tee,
        hash: blake3::Hasher::new(),
    };
    let header = read_header(&mut inp).await?;
    Ok((header, raw))
}

/// Keeps a copy of what's read through it.
struct Tee<'a, R> {
    r: &'a mut R,
    raw: &'a mut Vec<u8>,
}

impl<R: AsyncRead + Unpin> AsyncRead for Tee<'_, R> {
    fn poll_read(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        let before = buf.filled().len();
        let this = &mut *self;
        let r = std::pin::Pin::new(&mut *this.r).poll_read(cx, buf);
        this.raw.extend_from_slice(&buf.filled()[before..]);
        r
    }
}

async fn write_hole<W: AsyncWrite + Unpin + Send>(
    out: &mut Out<'_, W>,
    first: u64,
    count: u64,
) -> Result<(), FileSystemError> {
    out.put(&[HOLE]).await?;
    out.put(&first.to_le_bytes()).await?;
    out.put(&count.to_le_bytes()).await
}

impl<D: PoolDevice> FileSystem<D, Committer> {
    /// Receive a stream from `r` as a snapshot of volume `name`: a full
    /// stream makes a new volume; an incremental one goes onto `name`,
    /// whose newest snapshot must be the stream's base. `force`: roll
    /// `name` back to that snapshot first if it was written since (else
    /// that's refused). The volume must not be running. On any failure the
    /// volume is left as it was (a full receive's volume is deleted).
    pub async fn recv_snapshot<Rd: AsyncRead + Unpin + Send>(
        &self,
        name: &DatasetName,
        r: &mut Rd,
        force: bool,
    ) -> Result<StreamReport, FileSystemError> {
        let mut inp = In {
            r,
            hash: blake3::Hasher::new(),
        };
        let header = read_header(&mut inp).await?;
        crate::types::SnapshotName::try_from(header.snapshot.as_str())?;
        self.recv_begin(name, &header, force, true).await?;
        let result = self.recv_frames(name, &header, &mut inp).await;
        let result = match result {
            Ok(report) => match self
                .create_snapshot_as(
                    name,
                    &header.snapshot,
                    Some(header.guid),
                    Some(header.created_ns),
                )
                .await
            {
                Ok(_) => Ok(report),
                Err(e) => Err(e),
            },
            Err(e) => Err(e),
        };
        if let Err(e) = result {
            // Put the volume back as it was.
            let undo = match header.from {
                Some(_) => self.rollback_volume(name).await,
                None => self.delete_dataset(name).await,
            };
            if let Err(u) = undo {
                eprintln!("recv {}: undoing a failed receive: {}", name, u);
            }
            self.close_dataset(name);
            return Err(e);
        }
        result
    }

    /// Whether a stream with `header` would be received into volume `name`
    /// (changes nothing): what a client asks before sending the rest.
    pub async fn recv_check(
        &self,
        name: &DatasetName,
        header: &SendHeader,
        force: bool,
    ) -> Result<(), FileSystemError> {
        crate::types::SnapshotName::try_from(header.snapshot.as_str())?;
        self.recv_begin(name, header, force, false).await
    }

    /// Check volume `name` for receiving `header`, and (`prepare`) make it
    /// ready: create it, or roll it back with `force`.
    async fn recv_begin(
        &self,
        name: &DatasetName,
        header: &SendHeader,
        force: bool,
        prepare: bool,
    ) -> Result<(), FileSystemError> {
        let Some(base) = &header.from else {
            if self.dataset_entry(name).await.is_ok() {
                return Err(FileSystemError::InvalidArgument(format!(
                    "dataset '{}' exists (a full stream makes a new one)",
                    name
                )));
            }
            if !header.format.is_tree() {
                return Err(FileSystemError::InvalidArgument(format!(
                    "a stream of a {} dataset",
                    header.format.name()
                )));
            }
            if prepare {
                match header.format {
                    f if f.is_btree() => {
                        let bs = header.volblocksize as u64;
                        if bs == 0 || !header.size_bytes.is_multiple_of(bs) {
                            return Err(FileSystemError::InvalidArgument(
                                "a B+tree stream of partial nodes".into(),
                            ));
                        }
                        self.create_btree(
                            name,
                            f,
                            header.volblocksize,
                            header.size_bytes / bs,
                            Default::default(),
                        )
                        .await?;
                    }
                    _ => {
                        self.create_block(name, header.size_bytes, header.volblocksize)
                            .await?;
                    }
                }
            } else if header.volblocksize == 0
                || !header.volblocksize.is_multiple_of(self.volblock_align())
                || header.volblocksize > self.max_volblocksize()
            {
                return Err(FileSystemError::InvalidArgument(format!(
                    "records of {} bytes don't fit this pool (multiples of {} up to {})",
                    header.volblocksize,
                    self.volblock_align(),
                    self.max_volblocksize()
                )));
            }
            return Ok(());
        };
        let id = self.dataset_id(name).await?;
        let head = self.read_dnode(id).await?;
        if head.kind() != header.format.root_kind() {
            return Err(FileSystemError::InvalidArgument(format!(
                "'{}' is a {}, the stream of a {} dataset",
                name,
                head.kind().name(),
                header.format.name()
            )));
        }
        if (head.size, head.blksz) != (header.size_bytes, header.volblocksize) {
            return Err(FileSystemError::InvalidArgument(format!(
                "volume '{}' is {} bytes of {}-byte records, the stream {} of {}",
                name, head.size, head.blksz, header.size_bytes, header.volblocksize
            )));
        }
        let snapshots = head.head().map(|h| h.snapshots.clone()).unwrap_or_default();
        if snapshots.iter().any(|s| s.guid == header.guid) {
            return Err(FileSystemError::AlreadyExists);
        }
        let Some(newest) = snapshots.last() else {
            return Err(FileSystemError::InvalidArgument(format!(
                "volume '{}' has no snapshots; the stream needs @{}",
                name, base.snapshot
            )));
        };
        if newest.guid != base.guid {
            let why = if snapshots.iter().any(|s| s.guid == base.guid) {
                format!(
                    "@{} isn't the newest snapshot of '{}' (destroy the ones after it)",
                    base.snapshot, name
                )
            } else {
                format!("'{}' has no snapshot @{} (by GUID)", name, base.snapshot)
            };
            return Err(FileSystemError::InvalidArgument(why));
        }
        // Acknowledged writes of a writer that's gone, still only in its
        // intent log: the log would be replayed over whatever this
        // receives (rolling back with --force refuses it too).
        if head.log().is_some() {
            return Err(FileSystemError::InvalidArgument(format!(
                "volume '{}' has an intent log (it's running, or its writer's log \
                 is yet to be replayed)",
                name
            )));
        }
        let snap = self.read_dnode(newest.id).await?;
        let dirty = self
            .open_datasets
            .lock()
            .unwrap()
            .get(name)
            .is_some_and(|v| v.is_dirty());
        if head.tree != snap.tree || dirty {
            if !force {
                return Err(FileSystemError::InvalidArgument(format!(
                    "volume '{}' was written since {} (receive with --force to roll it back)",
                    name, newest.name
                )));
            }
            if prepare {
                self.rollback_volume(name).await?;
            }
        }
        Ok(())
    }

    async fn recv_frames<Rd: AsyncRead + Unpin + Send>(
        &self,
        name: &DatasetName,
        header: &SendHeader,
        inp: &mut In<'_, Rd>,
    ) -> Result<StreamReport, FileSystemError> {
        let bs = header.volblocksize as u64;
        let records = header.size_bytes.div_ceil(bs);
        // Records go straight into the dataset's object whatever its format
        // (the stream is records, not block or kv operations), with its
        // operations held off until the stream is in.
        let ds = self.open_dataset(name).await?;
        let _w = ds.ops.write().await;
        // A keyed dataset's intent log carries operations, never records
        // (see `crate::zil`): what a receive writes there is durable when
        // the receive commits, and not before.
        let keyed = header.format.is_btree();
        let mut report = StreamReport {
            snapshot: format!("{}@{}", name, header.snapshot),
            incremental: header.from.is_some(),
            ..Default::default()
        };
        loop {
            let digest = inp.hash.clone().finalize();
            match inp.u8().await? {
                DATA => {
                    let idx = inp.u64().await?;
                    let len = inp.u32().await? as u64;
                    if idx >= records || len > bs {
                        return Err(bad(format!("record {} ({} bytes)", idx, len)));
                    }
                    let mut data = inp.get(len as usize).await?;
                    // A whole record: its tail is zero.
                    let end = (header.size_bytes - idx * bs).min(bs);
                    data.resize(end as usize, 0);
                    if keyed {
                        ds.obj.put_records([(idx, Dirty::Data(data))], None);
                    } else {
                        self.object_modify(&ds.obj, idx * bs, data.len(), Some(&data))
                            .await?;
                    }
                    if self.datasets_dirty_bytes() > DIRTY_LIMIT_BYTES {
                        self.commit_if(|| self.datasets_dirty_bytes() > DIRTY_LIMIT_BYTES)
                            .await?;
                    }
                    report.data_records += 1;
                    report.data_bytes += len;
                }
                HOLE => {
                    let first = inp.u64().await?;
                    let count = inp.u64().await?;
                    if first >= records || count > records - first {
                        return Err(bad(format!("hole {}+{}", first, count)));
                    }
                    let end = ((first + count) * bs).min(header.size_bytes);
                    if keyed {
                        ds.obj
                            .put_records((first..first + count).map(|i| (i, Dirty::Hole)), None);
                    } else {
                        self.object_modify(&ds.obj, first * bs, (end - first * bs) as usize, None)
                            .await?;
                    }
                    report.hole_records += count;
                }
                END => {
                    let mut n = [0u8; 8];
                    inp.r.read_exact(&mut n).await.map_err(io)?;
                    let mut sum = [0u8; 32];
                    inp.r.read_exact(&mut sum).await.map_err(io)?;
                    if u64::from_le_bytes(n) != report.data_records {
                        return Err(bad("record count doesn't match"));
                    }
                    if sum != *digest.as_bytes() {
                        return Err(bad("checksum doesn't match"));
                    }
                    return Ok(report);
                }
                t => return Err(bad(format!("frame type {}", t))),
            }
        }
    }
}

#[cfg(test)]
#[path = "tests/send_tests.rs"]
mod tests;
