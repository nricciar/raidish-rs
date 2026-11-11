//! Metaslab allocator.
//!
//! Allocation and freeing are purely in-memory operations; nothing touches the
//! disk until the transaction group (TXG) is synced by `commit_txg`.
//!
//! On-disk, every metaslab owns two fixed-size space map *slots* (A/B). When a
//! TXG is synced, each dirty metaslab serializes its complete state into its
//! *inactive* slot, tagging every block with the new TXG, before the uberblock
//! for that TXG is written. On load, the valid slot with the highest TXG that is
//! `<=` the committed uberblock TXG wins, so a torn or uncommitted slot write is
//! never observed.
//!
//! The slot payload is two bitmaps, one bit per block in the metaslab:
//! `allocated` followed by `deferred` (freed, but not yet reusable). Bitmaps
//! have a fixed size, so a heavily fragmented metaslab can never overflow its
//! slot.
//!
//! Frees are deferred for `TXG_DEFER_SIZE` TXGs before the space becomes
//! allocatable again, so the previous few uberblocks never reference blocks
//! that have since been overwritten.

use crate::disk::{BLOCK_SIZE, BlockDevice};
use crate::fs::{
    BLOCK_HEADER_SIZE as BLOCK_HEADER, BLOCK_PAYLOAD_SIZE, FileSystem, FileSystemError, Superblock,
};
use crate::inode::Extent;
use crate::raidz::DATA_SHARDS;
use std::collections::{BTreeMap, VecDeque};

/// Number of TXGs a freed block waits before it can be reallocated.
pub const TXG_DEFER_SIZE: u64 = 2;
/// Number of space map slots per metaslab (A/B).
pub const SPACEMAP_SLOTS: u64 = 2;
/// Bounds on metaslab size, in blocks. Both are multiples of `DATA_SHARDS`.
pub const METASLAB_MIN_BLOCKS: u64 = 1026;
pub const METASLAB_MAX_BLOCKS: u64 = 262_143;
/// Aim for roughly this many metaslabs per pool.
pub const METASLAB_TARGET_COUNT: u64 = 128;

const ALIGN: u64 = DATA_SHARDS as u64;

fn align_up(v: u64) -> u64 {
    v.div_ceil(ALIGN) * ALIGN
}

fn align_down(v: u64) -> u64 {
    (v / ALIGN) * ALIGN
}

/// Deterministic placement of space map slots and metaslabs on the device.
#[derive(Debug, Clone, Copy, Default)]
pub struct MetaslabLayout {
    pub metaslab_count: u64,
    pub metaslab_size_blocks: u64,
    pub spacemap_start: u64,
    pub spacemap_slot_blocks: u64,
    pub data_start: u64,
}

impl MetaslabLayout {
    pub fn compute(total_blocks: u64, spacemap_start: u64) -> Self {
        assert!(
            spacemap_start.is_multiple_of(ALIGN),
            "spacemap region must be stripe aligned"
        );

        let size = align_down(
            (total_blocks / METASLAB_TARGET_COUNT).clamp(METASLAB_MIN_BLOCKS, METASLAB_MAX_BLOCKS),
        );
        let slot_blocks = Self::slot_blocks_for(size);
        let per_metaslab = size + SPACEMAP_SLOTS * slot_blocks;
        let metaslab_count = total_blocks.saturating_sub(spacemap_start) / per_metaslab;
        let data_start = spacemap_start + metaslab_count * SPACEMAP_SLOTS * slot_blocks;

        MetaslabLayout {
            metaslab_count,
            metaslab_size_blocks: size,
            spacemap_start,
            spacemap_slot_blocks: slot_blocks,
            data_start,
        }
    }

    pub fn from_superblock(sb: &Superblock) -> Self {
        MetaslabLayout {
            metaslab_count: sb.metaslab_count,
            metaslab_size_blocks: sb.metaslab_size_blocks,
            spacemap_start: sb.spacemap_start,
            spacemap_slot_blocks: sb.spacemap_slot_blocks,
            data_start: sb.data_start,
        }
    }

    fn bitmap_bytes(metaslab_size: u64) -> usize {
        metaslab_size.div_ceil(8) as usize
    }

    fn slot_blocks_for(metaslab_size: u64) -> u64 {
        let bytes = 2 * Self::bitmap_bytes(metaslab_size) as u64;
        align_up(bytes.div_ceil(BLOCK_PAYLOAD_SIZE as u64))
    }

    pub fn slot_lba(&self, ms_idx: usize, slot: u64) -> u64 {
        self.spacemap_start + (ms_idx as u64 * SPACEMAP_SLOTS + slot) * self.spacemap_slot_blocks
    }

    pub fn metaslab_start(&self, ms_idx: usize) -> u64 {
        self.data_start + ms_idx as u64 * self.metaslab_size_blocks
    }

    pub fn metaslab_for_block(&self, lba: u64) -> Option<usize> {
        if lba < self.data_start {
            return None;
        }
        let idx = (lba - self.data_start) / self.metaslab_size_blocks;
        (idx < self.metaslab_count).then_some(idx as usize)
    }
}

#[derive(Debug)]
pub struct MetaslabState {
    pub start_block: u64,
    pub block_count: u64,
    /// Space that can be allocated right now.
    pub free_extents: BTreeMap<u64, u64>,
    /// Space freed during the currently open TXG.
    pub freeing: Vec<Extent>,
    /// Space freed in earlier TXGs, keyed by the committed TXG at which it
    /// becomes allocatable again.
    pub deferred: VecDeque<(u64, Vec<Extent>)>,
    /// State changed since the last synced slot was written.
    pub dirty: bool,
    /// Slot holding the most recently committed state, if any.
    pub active_slot: Option<u64>,
    pub synced_txg: u64,
}

impl MetaslabState {
    pub fn new_empty(start_block: u64, block_count: u64) -> Self {
        let mut free_extents = BTreeMap::new();
        free_extents.insert(start_block, block_count);
        MetaslabState {
            start_block,
            block_count,
            free_extents,
            freeing: Vec::new(),
            deferred: VecDeque::new(),
            dirty: true,
            active_slot: None,
            synced_txg: 0,
        }
    }

    pub fn free_blocks(&self) -> u64 {
        self.free_extents.values().sum()
    }

    pub fn pending_free_blocks(&self) -> u64 {
        let freeing: u64 = self.freeing.iter().map(|e| e.len).sum();
        let deferred: u64 = self
            .deferred
            .iter()
            .flat_map(|(_, v)| v.iter())
            .map(|e| e.len)
            .sum();
        freeing + deferred
    }

    /// Iterate every extent that is not currently allocated (free, freeing or deferred).
    fn released_extents(&self) -> impl Iterator<Item = (u64, u64)> + '_ {
        self.free_extents
            .iter()
            .map(|(&s, &l)| (s, l))
            .chain(self.freeing.iter().map(|e| (e.start, e.len)))
            .chain(
                self.deferred
                    .iter()
                    .flat_map(|(_, v)| v.iter())
                    .map(|e| (e.start, e.len)),
            )
    }

    fn overlaps_released(&self, start: u64, len: u64) -> bool {
        let end = start + len;
        self.released_extents()
            .any(|(s, l)| s < end && start < s + l)
    }

    /// Insert a range into the free map, merging with neighbours.
    fn insert_free(map: &mut BTreeMap<u64, u64>, start: u64, len: u64) {
        if len == 0 {
            return;
        }
        let mut new_start = start;
        let mut new_end = start + len;

        if let Some((&ps, &pl)) = map.range(..=start).next_back()
            && ps + pl >= start
        {
            new_start = ps;
            new_end = new_end.max(ps + pl);
            map.remove(&ps);
        }
        while let Some((&ns, &nl)) = map.range(new_start..).next() {
            if ns > new_end {
                break;
            }
            new_end = new_end.max(ns + nl);
            map.remove(&ns);
        }
        map.insert(new_start, new_end - new_start);
    }

    /// Carve out exactly `want` (stripe-aligned) contiguous blocks.
    fn take_aligned_exact(&mut self, want: u64) -> Option<Extent> {
        let (s, l, aligned) = self.free_extents.iter().find_map(|(&s, &l)| {
            let aligned = align_up(s);
            (aligned + want <= s + l).then_some((s, l, aligned))
        })?;
        self.free_extents.remove(&s);
        if aligned > s {
            self.free_extents.insert(s, aligned - s);
        }
        let tail_start = aligned + want;
        if tail_start < s + l {
            self.free_extents.insert(tail_start, s + l - tail_start);
        }
        self.dirty = true;
        Some(Extent {
            start: aligned,
            len: want,
        })
    }

    /// Carve out a single stripe-aligned extent of at most `want` blocks.
    fn take_aligned(&mut self, want: u64) -> Option<Extent> {
        let found = self.free_extents.iter().find_map(|(&s, &l)| {
            let aligned = align_up(s);
            let end = s + l;
            if aligned >= end {
                return None;
            }
            let avail = align_down(end - aligned);
            (avail > 0).then(|| (s, l, aligned, avail.min(want)))
        })?;

        let (s, l, aligned, take) = found;
        self.free_extents.remove(&s);
        if aligned > s {
            self.free_extents.insert(s, aligned - s);
        }
        let tail_start = aligned + take;
        if tail_start < s + l {
            self.free_extents.insert(tail_start, s + l - tail_start);
        }
        self.dirty = true;
        Some(Extent {
            start: aligned,
            len: take,
        })
    }

    /// Release deferred frees whose wait has elapsed as of committed `txg`.
    fn release_deferred(&mut self, txg: u64) {
        while let Some((release_txg, _)) = self.deferred.front() {
            if *release_txg > txg {
                break;
            }
            let (_, extents) = self.deferred.pop_front().unwrap();
            for e in extents {
                Self::insert_free(&mut self.free_extents, e.start, e.len);
            }
            self.dirty = true;
        }
    }

    /// Serialize as `[allocated bitmap][deferred bitmap]`.
    fn encode(&self) -> Vec<u8> {
        let n = self.block_count;
        let bm = MetaslabLayout::bitmap_bytes(n);
        let mut buf = vec![0u8; 2 * bm];

        let set = |buf: &mut [u8], base: usize, start: u64, len: u64, value: bool| {
            for b in start..start + len {
                let i = base * 8 + (b as usize);
                if value {
                    buf[i / 8] |= 1 << (i % 8);
                } else {
                    buf[i / 8] &= !(1 << (i % 8));
                }
            }
        };

        set(&mut buf, 0, 0, n, true);
        for (&s, &l) in &self.free_extents {
            set(&mut buf, 0, s - self.start_block, l, false);
        }
        let pending = self
            .freeing
            .iter()
            .chain(self.deferred.iter().flat_map(|(_, v)| v.iter()));
        for e in pending {
            set(&mut buf, 0, e.start - self.start_block, e.len, false);
            set(&mut buf, bm, e.start - self.start_block, e.len, true);
        }
        buf
    }

    /// Rebuild state from an encoded slot. Deferred space is held until
    /// committed TXG `release_txg`.
    fn decode(start_block: u64, block_count: u64, bytes: &[u8], release_txg: u64) -> Self {
        let bm = MetaslabLayout::bitmap_bytes(block_count);
        let bit = |i: usize| bytes.get(i / 8).is_some_and(|b| b & (1 << (i % 8)) != 0);

        #[derive(PartialEq, Clone, Copy)]
        enum S {
            Alloc,
            Deferred,
            Free,
        }

        let mut free_extents = BTreeMap::new();
        let mut deferred = Vec::new();
        let mut run: Option<(S, u64)> = None;

        let mut flush = |state: S, from: u64, to: u64| match state {
            S::Free => {
                free_extents.insert(start_block + from, to - from);
            }
            S::Deferred => deferred.push(Extent {
                start: start_block + from,
                len: to - from,
            }),
            S::Alloc => {}
        };

        for b in 0..block_count {
            let state = if bit(b as usize) {
                S::Alloc
            } else if bit(bm * 8 + b as usize) {
                S::Deferred
            } else {
                S::Free
            };
            match run {
                Some((s, _)) if s == state => {}
                Some((s, from)) => {
                    flush(s, from, b);
                    run = Some((state, b));
                }
                None => run = Some((state, b)),
            }
        }
        if let Some((s, from)) = run {
            flush(s, from, block_count);
        }

        let mut deferred_q = VecDeque::new();
        if !deferred.is_empty() {
            deferred_q.push_back((release_txg, deferred));
        }

        MetaslabState {
            start_block,
            block_count,
            free_extents,
            freeing: Vec::new(),
            deferred: deferred_q,
            dirty: false,
            active_slot: None,
            synced_txg: 0,
        }
    }
}

/// Per slot block: every (txg, payload, rebuilt-from-parity block) seen.
type SlotVersions = Vec<Vec<(u64, Vec<u8>, Option<[u8; BLOCK_SIZE]>)>>;

/// Bookkeeping for a metaslab whose slot was written during a TXG sync.
pub(crate) struct SyncedMetaslab {
    ms_idx: usize,
    slot: u64,
    freeing: Vec<Extent>,
}

impl<D: BlockDevice> FileSystem<D> {
    /// Free an extent. The space is not reusable until `TXG_DEFER_SIZE` TXGs
    /// after the TXG in which the free is committed.
    pub(crate) async fn free(&self, start: u64, len: u64) -> Result<(), FileSystemError> {
        if len == 0 {
            return Ok(());
        }
        let ms_idx = match self.layout.metaslab_for_block(start) {
            Some(idx) => idx,
            None => {
                eprintln!("WARNING: free of block {} outside any metaslab", start);
                return Ok(());
            }
        };

        let mut ms = self.metaslabs[ms_idx].write().await;
        if start + len > ms.start_block + ms.block_count {
            eprintln!(
                "WARNING: free {{{}:{}}} crosses metaslab {} boundary",
                start, len, ms_idx
            );
            return Ok(());
        }
        if ms.overlaps_released(start, len) {
            eprintln!(
                "WARNING: attempted to free already-free extent {{{}:{}}}",
                start, len
            );
            return Ok(());
        }
        ms.freeing.push(Extent { start, len });
        ms.dirty = true;
        Ok(())
    }

    /// Return space allocated in the open TXG that was never referenced by
    /// anything committed (e.g. a sync that failed). Unlike `free`, there is
    /// no deferral: no uberblock can point at it.
    pub(crate) async fn unallocate(&self, start: u64, len: u64) {
        if let Some(idx) = self.layout.metaslab_for_block(start) {
            let mut ms = self.metaslabs[idx].write().await;
            MetaslabState::insert_free(&mut ms.free_extents, start, len);
            ms.dirty = true;
        }
    }

    /// Space that can be allocated right now (excludes deferred frees).
    pub(crate) async fn allocatable_blocks(&self) -> u64 {
        let mut total = 0;
        for ms in self.metaslabs.iter() {
            total += ms.read().await.free_blocks();
        }
        total
    }

    /// Allocate one contiguous, stripe-aligned extent of `blocks` (rounded up
    /// to whole stripes).
    pub(crate) async fn allocate_contiguous(&self, blocks: u64) -> Result<Extent, FileSystemError> {
        let want = align_up(blocks);
        for ms in self.metaslabs.iter() {
            if let Some(extent) = ms.write().await.take_aligned_exact(want) {
                return Ok(extent);
            }
        }
        Err(FileSystemError::UnableToAllocateBlocks(want))
    }

    pub(crate) async fn allocate_blocks_stripe_aligned(
        &self,
        blocks_needed: u64,
    ) -> Result<Vec<Extent>, FileSystemError> {
        if blocks_needed == 0 {
            return Ok(Vec::new());
        }

        let mut remaining = align_up(blocks_needed);
        let mut allocated: Vec<(usize, Extent)> = Vec::new();

        for (ms_idx, ms) in self.metaslabs.iter().enumerate() {
            if remaining == 0 {
                break;
            }
            let mut ms = ms.write().await;
            while remaining > 0 {
                match ms.take_aligned(remaining) {
                    Some(extent) => {
                        remaining -= extent.len;
                        allocated.push((ms_idx, extent));
                    }
                    None => break,
                }
            }
        }

        if remaining == 0 {
            return Ok(allocated.into_iter().map(|(_, e)| e).collect());
        }

        // Never committed, so return straight to the free map (no deferral).
        for (ms_idx, extent) in allocated {
            let mut ms = self.metaslabs[ms_idx].write().await;
            MetaslabState::insert_free(&mut ms.free_extents, extent.start, extent.len);
        }
        Err(FileSystemError::UnableToAllocateBlocks(remaining))
    }

    /// Write every dirty metaslab into its inactive slot, tagged with `txg`.
    /// Must be followed by `finish_metaslab_sync` once the uberblock for `txg`
    /// is durable, or `abort_metaslab_sync` if it is not.
    pub(crate) async fn sync_metaslabs(
        &self,
        txg: u64,
    ) -> Result<Vec<SyncedMetaslab>, FileSystemError> {
        let mut synced = Vec::new();
        let mut payloads = Vec::new();

        for (ms_idx, ms) in self.metaslabs.iter().enumerate() {
            let mut ms = ms.write().await;
            if !ms.dirty {
                continue;
            }
            let slot = match ms.active_slot {
                Some(s) => (s + 1) % SPACEMAP_SLOTS,
                None => 0,
            };
            payloads.push((self.layout.slot_lba(ms_idx, slot), ms.encode()));
            let freeing = std::mem::take(&mut ms.freeing);
            ms.dirty = false;
            synced.push(SyncedMetaslab {
                ms_idx,
                slot,
                freeing,
            });
        }

        for (lba, bytes) in payloads {
            if let Err(e) = self.write_slot(lba, txg, &bytes).await {
                self.abort_metaslab_sync(synced).await;
                return Err(e);
            }
        }

        Ok(synced)
    }

    async fn write_slot(&self, lba: u64, txg: u64, bytes: &[u8]) -> Result<(), FileSystemError> {
        let mut chunks = bytes.chunks(BLOCK_PAYLOAD_SIZE);
        for b in 0..self.layout.spacemap_slot_blocks {
            let chunk = chunks.next().unwrap_or(&[]);
            self.write_raw_block_txg(lba + b, txg, chunk).await?;
        }
        Ok(())
    }

    pub(crate) async fn finish_metaslab_sync(&self, txg: u64, synced: Vec<SyncedMetaslab>) {
        for s in synced {
            let mut ms = self.metaslabs[s.ms_idx].write().await;
            ms.active_slot = Some(s.slot);
            ms.synced_txg = txg;
            if !s.freeing.is_empty() {
                ms.deferred.push_back((txg + TXG_DEFER_SIZE, s.freeing));
            }
        }
        for ms in self.metaslabs.iter() {
            ms.write().await.release_deferred(txg);
        }
    }

    pub(crate) async fn abort_metaslab_sync(&self, synced: Vec<SyncedMetaslab>) {
        for s in synced {
            let mut ms = self.metaslabs[s.ms_idx].write().await;
            let mut freeing = s.freeing;
            freeing.append(&mut ms.freeing);
            ms.freeing = freeing;
            ms.dirty = true;
        }
    }

    /// Load every metaslab from the newest valid slot not newer than `uber_txg`.
    pub(crate) async fn load_metaslabs(
        &self,
        uber_txg: u64,
    ) -> Result<Vec<MetaslabState>, FileSystemError> {
        let mut out = Vec::with_capacity(self.layout.metaslab_count as usize);

        for ms_idx in 0..self.layout.metaslab_count as usize {
            let start = self.layout.metaslab_start(ms_idx);
            let count = self.layout.metaslab_size_blocks;

            let mut best: Option<(u64, u64, Vec<u8>)> = None;
            for slot in 0..SPACEMAP_SLOTS {
                if let Some((txg, bytes)) = self
                    .read_slot(self.layout.slot_lba(ms_idx, slot), uber_txg)
                    .await
                    && txg <= uber_txg
                    && best.as_ref().is_none_or(|(t, _, _)| txg > *t)
                {
                    best = Some((txg, slot, bytes));
                }
            }

            let state = match best {
                Some((txg, slot, bytes)) => {
                    let mut st =
                        MetaslabState::decode(start, count, &bytes, uber_txg + TXG_DEFER_SIZE);
                    st.active_slot = Some(slot);
                    st.synced_txg = txg;
                    st
                }
                None => {
                    // Treat as fully allocated: leaking space is safe, double
                    // allocating it is not.
                    eprintln!(
                        "WARNING: metaslab {} has no valid space map slot; treating as full",
                        ms_idx
                    );
                    let mut st = MetaslabState::new_empty(start, count);
                    st.free_extents.clear();
                    st.dirty = false;
                    st
                }
            };
            out.push(state);
        }

        Ok(out)
    }

    /// Read a slot: valid at TXG `t` only if every block is a good label
    /// from TXG `t`. A disk that missed writes while it was away returns
    /// stale but well-formed blocks, so blocks that disagree are also rebuilt
    /// from parity, and the slot is taken at the newest TXG (no newer than
    /// `max_txg`) that every block can reach. Healed blocks are repaired.
    async fn read_slot(&self, lba: u64, max_txg: u64) -> Option<(u64, Vec<u8>)> {
        // versions[b]: every (txg, payload, came-from-parity) seen for block b.
        let mut versions: SlotVersions = Vec::new();
        for b in 0..self.layout.spacemap_slot_blocks {
            let mut v = Vec::new();
            let mut buf = [0u8; BLOCK_SIZE];
            if self.dev.read_block(lba + b, &mut buf).await.is_ok()
                && let Ok((txg, len)) = crate::fs::parse_label(&buf)
            {
                v.push((txg, buf[BLOCK_HEADER..BLOCK_HEADER + len].to_vec(), None));
            }
            versions.push(v);
        }

        let consistent = |versions: &SlotVersions| {
            let first = versions.first()?.first()?.0;
            versions
                .iter()
                .all(|v| v.len() == 1 && v[0].0 == first)
                .then_some(first)
        };
        if let Some(t) = consistent(&versions)
            && t <= max_txg
        {
            return Some((
                t,
                versions.into_iter().flat_map(|v| v[0].1.clone()).collect(),
            ));
        }

        // Disagreement: add every rebuild of every block as an alternative.
        for (b, v) in versions.iter_mut().enumerate() {
            if let Ok(c) = self.dev.reconstruct_candidates(lba + b as u64).await {
                for rebuilt in c.rebuilt.into_iter().flatten() {
                    if let Ok((txg, len)) = crate::fs::parse_label(&rebuilt)
                        && !v.iter().any(|(t, _, _)| *t == txg)
                    {
                        let block: [u8; BLOCK_SIZE] = rebuilt[..].try_into().ok()?;
                        v.push((
                            txg,
                            rebuilt[BLOCK_HEADER..BLOCK_HEADER + len].to_vec(),
                            Some(block),
                        ));
                    }
                }
            }
        }

        let mut txgs: Vec<u64> = versions
            .iter()
            .flat_map(|v| v.iter().map(|(t, _, _)| *t))
            .filter(|&t| t <= max_txg)
            .collect();
        txgs.sort_unstable();
        txgs.dedup();
        for &t in txgs.iter().rev() {
            let picks: Option<Vec<_>> = versions
                .iter()
                .map(|v| v.iter().find(|(txg, _, _)| *txg == t))
                .collect();
            let Some(picks) = picks else { continue };
            let mut bytes = Vec::new();
            for (b, (_, payload, healed)) in picks.into_iter().enumerate() {
                if let Some(block) = healed {
                    eprintln!(
                        "space map: block {} was stale; rebuilt from parity",
                        lba + b as u64
                    );
                    if !self.read_only {
                        let _ = self.dev.repair_block(lba + b as u64, block).await;
                    }
                }
                bytes.extend_from_slice(payload);
            }
            return Some((t, bytes));
        }
        None
    }

    /// Find all blocks that are allocated but not referenced by any live data
    pub async fn find_orphaned_blocks(&self) -> Result<Vec<(u64, u64)>, FileSystemError> {
        let mut live_blocks = std::collections::HashSet::new();
        let mut mark = |extent: &Extent| {
            for i in 0..extent.len() {
                live_blocks.insert(extent.start() + i);
            }
        };

        // Walk the whole tree, not just the root directory.
        let mut pending = vec![self.file_index.read().await.clone()];
        while let Some(index) = pending.pop() {
            for inode_ref in index.files.values() {
                for extent in inode_ref.inode_extents() {
                    mark(&extent);
                }
                let inode = self.read_inode(inode_ref).await?;
                for extent in self.content_extents(&inode.inode_type).await? {
                    mark(&extent);
                }
                if inode.inode_type.is_directory() {
                    pending.push(self.read_file_index(&inode.inode_type).await?);
                }
            }
        }

        for ptr in self.root_ptr.lock().await.iter() {
            mark(&ptr.extent());
        }

        let mut orphaned = Vec::new();

        for ms in self.metaslabs.iter() {
            let ms = ms.read().await;
            let ms_end = ms.start_block + ms.block_count;

            // Everything not free, freeing or deferred is allocated.
            let mut released = BTreeMap::new();
            for (s, l) in ms.released_extents() {
                MetaslabState::insert_free(&mut released, s, l);
            }

            let mut allocated_ranges = Vec::new();
            let mut cursor = ms.start_block;
            for (&s, &l) in &released {
                if cursor < s {
                    allocated_ranges.push((cursor, s - cursor));
                }
                cursor = s + l;
            }
            if cursor < ms_end {
                allocated_ranges.push((cursor, ms_end - cursor));
            }

            for (start, len) in allocated_ranges {
                let mut orphan_start = None;
                let mut orphan_len = 0;

                for block in start..start + len {
                    if !live_blocks.contains(&block) {
                        orphan_start.get_or_insert(block);
                        orphan_len += 1;
                    } else if let Some(os) = orphan_start.take() {
                        orphaned.push((os, orphan_len));
                        orphan_len = 0;
                    }
                }
                if let Some(os) = orphan_start {
                    orphaned.push((os, orphan_len));
                }
            }
        }

        Ok(orphaned)
    }
}

#[cfg(test)]
#[path = "tests/metaslab_tests.rs"]
mod tests;
