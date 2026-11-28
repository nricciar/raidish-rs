//! A B+tree of byte-string keys to byte-string values, kept in one object's
//! records (see `crate::object`): the on-disk format of `kv` datasets.
//!
//! Every node is one record of the object, found by its *node id* (its
//! record index); node 0 is the [`Header`]. Nodes are rewritten in place
//! by id, and the object's tree underneath does the copy-on-write: a
//! commit writes the changed records to new space and rewrites only the
//! indirect blocks above them, so checksums, snapshots, clones, send /
//! receive and the intent log work on a B+tree exactly as on a volume.
//!
//! ```text
//!   record 0: header (root id, height, next id, free ids, entry count)
//!   record r: branch [k1 k2 ...] → children [c0 c1 c2 ...]   (c_i < k_{i+1} <= c_{i+1})
//!   record l: leaf   [(key, value) ...], keys ascending
//!   record i: id list [ids ...] → next id list (a large value's pages, or
//!             spilled free ids)
//!   record o: a page of a large value, raw
//! ```
//!
//! **Atomic operations.** An operation reads what it needs, builds every
//! changed node in memory ([`Txn`]) and applies them all at once under the
//! object's dirty-record lock. A sync, or an intent log flush, takes the
//! dirty records under that same lock, so it never sees half a split.
//! Operations on one tree are serialized by the caller (see
//! `crate::dataset::Dataset::ops`).
//!
//! **The intent log** carries each operation's effect, not its nodes
//! (see `crate::zil::KeyedOp`).
//!
//! **Sizes.** A node holds at most `blksz - 4` encoded bytes. A key is at
//! most 1/16 of that, and so is a value kept inline: larger values (up to
//! [`MAX_VALUE`]) go to overflow pages. So a split always yields two nodes
//! that fit, and a branch fans out at least 16 ways. A node under 1/4 full
//! (except the root) is merged with a sibling, or shares its entries with
//! one.
//!
//! **Node ids.** Every id (a node, a large value's page or id list) comes
//! from [`Txn::alloc`]: a freed one if there is any, else `next_id`. A
//! freed id's record becomes a hole (no space) and the id goes on the
//! header's free list; once that is full, the next freed id instead
//! becomes an id list page holding the whole list, chained from the header
//! (`free_chain`), and the header's list starts over. No freed id is ever
//! dropped, so `next_id` stays at the most ids the tree has used at once,
//! however long values churn (audit S6c-1). The object has a fixed
//! capacity of node ids (its size); a hole costs nothing, so it is set
//! large.

use crate::disk::PoolDevice;
use crate::fs::{FileSystem, FileSystemError};
use crate::object::{Dirty, ObjTree};
use crate::role::{Mutable, Role};
use crate::zil::KeyedOp;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use std::num::NonZeroU64;
#[cfg(not(target_arch = "wasm32"))]
use std::ops::Bound;
use std::sync::Arc;

const MAGIC: u64 = 0x5241_4944_4b56_5431; // "RAIDKVT1"
/// 2: `Header.root` is an `Option<NonZeroU64>`.
/// 3: large values' pages are listed in id lists (`Val::Overflow::index`),
/// and free ids spill into a chain of them (`Header::free_chain`).
const VERSION: u32 = 3;
/// The largest value.
pub const MAX_VALUE: usize = 64 << 20;

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct Header {
    magic: u64,
    version: u32,
    /// The root node (None: the tree is empty).
    pub root: Option<NonZeroU64>,
    /// Levels of branches above the leaves.
    pub height: u32,
    /// The lowest node id never handed out.
    pub next_id: u64,
    /// Freed node ids, for reuse (at most `Limits::max_free`).
    pub free: Vec<u64>,
    /// More freed ids: a chain of id lists (see the module docs).
    pub free_chain: Option<u64>,
    /// Freed ids in all: `free` and those in the chain.
    pub free_total: u64,
    /// Keys in the tree.
    pub entries: u64,
}

impl Default for Header {
    fn default() -> Self {
        Header {
            magic: MAGIC,
            version: VERSION,
            root: None,
            height: 0,
            next_id: 1,
            free: Vec::new(),
            free_chain: None,
            free_total: 0,
            entries: 0,
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub enum Val {
    Inline(#[serde(with = "bytes")] Vec<u8>),
    /// `len` bytes in `ceil(len / blksz)` pages, listed in order in the
    /// chain of id lists from `index`.
    Overflow {
        index: u64,
        len: u64,
    },
}

/// A page of ids: a large value's pages, or spilled free ids.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub(crate) struct IdList {
    pub next: Option<u64>,
    pub ids: Vec<u64>,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub(crate) enum Node {
    Leaf(#[serde(with = "bytes::entries")] Vec<(Vec<u8>, Val)>),
    /// `children.len() == keys.len() + 1`; child `i` holds keys in
    /// `[keys[i - 1], keys[i])`.
    Branch {
        #[serde(with = "bytes::list")]
        keys: Vec<Vec<u8>>,
        children: Vec<u64>,
    },
}

/// Keys and values as serde byte strings rather than sequences of `u8`.
/// bincode encodes both the same way (a u64 length, then the bytes), so
/// the on-disk format is unchanged; but a byte string decodes with one
/// copy, where a `Vec<u8>` decodes a byte at a time through serde's
/// sequence visitor. Nodes are decoded on every read, so this was most of
/// an etcd dataset's CPU.
mod bytes {
    use super::Val;
    use serde::de::{Deserializer, SeqAccess, Visitor};
    use serde::ser::Serializer;
    use serde::{Deserialize, Serialize};
    use std::fmt;

    struct Ref<'a>(&'a [u8]);

    impl Serialize for Ref<'_> {
        fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
            s.serialize_bytes(self.0)
        }
    }

    #[repr(transparent)]
    struct Buf(Vec<u8>);

    impl<'de> Deserialize<'de> for Buf {
        fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
            struct V;
            impl<'de> Visitor<'de> for V {
                type Value = Buf;
                fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
                    f.write_str("a byte string")
                }
                fn visit_bytes<E>(self, v: &[u8]) -> Result<Buf, E> {
                    Ok(Buf(v.to_vec()))
                }
                fn visit_byte_buf<E>(self, v: Vec<u8>) -> Result<Buf, E> {
                    Ok(Buf(v))
                }
                // Formats without byte strings (JSON) write a sequence.
                fn visit_seq<A: SeqAccess<'de>>(self, mut a: A) -> Result<Buf, A::Error> {
                    let mut v = Vec::with_capacity(a.size_hint().unwrap_or(0).min(4096));
                    while let Some(b) = a.next_element()? {
                        v.push(b);
                    }
                    Ok(Buf(v))
                }
            }
            d.deserialize_byte_buf(V)
        }
    }

    pub fn serialize<S: Serializer>(v: &[u8], s: S) -> Result<S::Ok, S::Error> {
        s.serialize_bytes(v)
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<Vec<u8>, D::Error> {
        Buf::deserialize(d).map(|b| b.0)
    }

    /// `Vec<Vec<u8>>` (a branch's keys).
    pub mod list {
        use super::*;

        pub fn serialize<S: Serializer>(v: &[Vec<u8>], s: S) -> Result<S::Ok, S::Error> {
            s.collect_seq(v.iter().map(|k| Ref(k)))
        }

        pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<Vec<Vec<u8>>, D::Error> {
            let v = Vec::<Buf>::deserialize(d)?;
            Ok(v.into_iter().map(|b| b.0).collect())
        }
    }

    /// `Vec<(Vec<u8>, Val)>` (a leaf's entries).
    pub mod entries {
        use super::*;

        pub fn serialize<S: Serializer>(v: &[(Vec<u8>, Val)], s: S) -> Result<S::Ok, S::Error> {
            s.collect_seq(v.iter().map(|(k, val)| (Ref(k), val)))
        }

        pub fn deserialize<'de, D: Deserializer<'de>>(
            d: D,
        ) -> Result<Vec<(Vec<u8>, Val)>, D::Error> {
            let v = Vec::<(Buf, Val)>::deserialize(d)?;
            Ok(v.into_iter().map(|(k, val)| (k.0, val)).collect())
        }
    }
}

fn encode<T: Serialize>(v: &T) -> Result<Vec<u8>, FileSystemError> {
    let body = bincode::serialize(v)?;
    let mut out = Vec::with_capacity(4 + body.len());
    out.extend_from_slice(&(body.len() as u32).to_le_bytes());
    out.extend_from_slice(&body);
    Ok(out)
}

fn decode<T: for<'a> Deserialize<'a>>(buf: &[u8], what: &str) -> Result<T, FileSystemError> {
    let bad = || FileSystemError::InvalidArgument(format!("malformed B+tree {}", what));
    let len = u32::from_le_bytes(buf.get(..4).ok_or_else(bad)?.try_into().unwrap()) as usize;
    let body = buf
        .get(4..len.checked_add(4).ok_or_else(bad)?)
        .ok_or_else(bad)?;
    // What `encode` wrote (`bincode::serialize`'s encoding), all of it.
    use bincode::Options;
    bincode::DefaultOptions::new()
        .with_fixint_encoding()
        .reject_trailing_bytes()
        .deserialize(body)
        .map_err(|_| bad())
}

/// Encoded size of `v` (with its length prefix).
fn size_of<T: Serialize>(v: &T) -> usize {
    4 + bincode::serialized_size(v).unwrap_or(u64::MAX) as usize
}

fn corrupt(m: String) -> FileSystemError {
    FileSystemError::InvalidArgument(format!("B+tree: {}", m))
}

/// Limits for nodes of `blksz` bytes.
#[derive(Debug, Clone, Copy)]
pub(crate) struct Limits {
    pub blksz: usize,
    /// Encoded bytes a node may hold.
    pub cap: usize,
    pub max_key: usize,
    /// Values larger than this go to overflow pages.
    pub max_inline: usize,
    /// Free node ids kept in the header (a spilled list holds as many).
    pub max_free: usize,
    /// Ids an `IdList` page holds.
    pub ids_per_list: usize,
    /// Node ids the object can hold.
    pub capacity: u64,
}

impl Limits {
    pub(crate) fn of(obj: &ObjTree) -> Self {
        let blksz = obj.blksz;
        let cap = blksz - 4;
        Limits {
            blksz,
            cap,
            max_key: cap / 16,
            max_inline: cap / 16,
            max_free: cap / 2 / 9,
            // Its `next` (9 bytes) and length (8), then 8 bytes an id.
            ids_per_list: (cap - 17) / 8,
            capacity: obj.size() / blksz as u64,
        }
    }
}

/// Index of the child of a branch with `keys` that holds `key`.
fn child_for(keys: &[Vec<u8>], key: &[u8]) -> usize {
    keys.partition_point(|k| k.as_slice() <= key)
}

/// Split `items` (with their encoded sizes) where the first part reaches
/// half of the total, keeping at least one item on each side.
fn split_point(sizes: &[usize]) -> usize {
    let total: usize = sizes.iter().sum();
    let mut acc = 0;
    for (i, s) in sizes.iter().enumerate() {
        acc += s;
        if acc * 2 >= total {
            return (i + 1).clamp(1, sizes.len() - 1);
        }
    }
    sizes.len() / 2
}

/// One operation on a tree: reads see its own writes, which are applied
/// together by [`Txn::apply`].
pub struct Txn<'a, D: PoolDevice, R: Role> {
    fs: &'a FileSystem<D, R>,
    obj: &'a ObjTree,
    lim: Limits,
    pub header: Header,
    header_dirty: bool,
    /// Node id → new contents (None: freed).
    writes: BTreeMap<u64, Option<Vec<u8>>>,
    /// What this transaction did, in order: what the intent log carries
    /// for it (see `apply_logged`).
    effects: Vec<KeyedOp>,
}

impl<'a, D: PoolDevice, R: Role> Txn<'a, D, R> {
    /// Start an operation on the tree in `obj` (an object with no records
    /// is an empty tree).
    pub async fn begin(
        fs: &'a FileSystem<D, R>,
        obj: &'a ObjTree,
    ) -> Result<Self, FileSystemError> {
        let header = match fs.object_record(obj, 0).await? {
            None => Header::default(),
            Some(buf) => {
                let h: Header = decode(&buf, "header")?;
                if h.magic != MAGIC || h.version != VERSION {
                    return Err(corrupt("not a B+tree (bad header magic or version)".into()));
                }
                h
            }
        };
        Ok(Txn {
            fs,
            obj,
            lim: Limits::of(obj),
            header,
            header_dirty: false,
            writes: BTreeMap::new(),
            effects: Vec::new(),
        })
    }

    /// Whether it changed nothing (yet).
    #[cfg(not(target_arch = "wasm32"))]
    pub fn is_empty(&self) -> bool {
        self.writes.is_empty() && !self.header_dirty
    }

    /// The longest key this tree takes.
    #[cfg(not(target_arch = "wasm32"))]
    pub fn max_key(&self) -> usize {
        self.lim.max_key
    }

    async fn page(&self, id: u64) -> Result<Option<Arc<Vec<u8>>>, FileSystemError> {
        match self.writes.get(&id) {
            Some(Some(b)) => Ok(Some(Arc::new(b.clone()))),
            Some(None) => Ok(None),
            None => self.fs.object_record(self.obj, id).await,
        }
    }

    pub(crate) async fn node(&self, id: u64) -> Result<Node, FileSystemError> {
        let buf = self
            .page(id)
            .await?
            .ok_or_else(|| corrupt(format!("node {} is missing", id)))?;
        decode(&buf, &format!("node {}", id))
    }

    fn put_node(&mut self, id: u64, n: &Node) -> Result<(), FileSystemError> {
        let buf = encode(n)?;
        if buf.len() > self.lim.blksz {
            return Err(corrupt(format!(
                "node {} would be {} bytes (at most {})",
                id,
                buf.len(),
                self.lim.blksz
            )));
        }
        self.writes.insert(id, Some(buf));
        Ok(())
    }

    /// A node id: a freed one if there is any (the header's list, then
    /// the spilled chain, whose page is itself handed out), else a new one.
    async fn alloc(&mut self) -> Result<u64, FileSystemError> {
        self.header_dirty = true;
        if let Some(id) = self.header.free.pop() {
            self.header.free_total -= 1;
            return Ok(id);
        }
        if let Some(id) = self.header.free_chain {
            let list = self.id_list(id).await?;
            if list.ids.len() > self.lim.max_free {
                return Err(corrupt(format!("free id list {} is too long", id)));
            }
            self.header.free = list.ids;
            self.header.free_chain = list.next;
            return Ok(id);
        }
        let id = self.header.next_id;
        if id >= self.lim.capacity {
            return Err(FileSystemError::NoSpace(format!(
                "the tree's {} node ids are all in use",
                self.lim.capacity
            )));
        }
        self.header.next_id += 1;
        Ok(id)
    }

    /// Free `id`: a hole on the header's free list or, that being full, a
    /// page holding the list (see the module docs).
    fn release(&mut self, id: u64) -> Result<(), FileSystemError> {
        self.header_dirty = true;
        if self.header.free.len() < self.lim.max_free {
            self.writes.insert(id, None);
            self.header.free.push(id);
            self.header.free_total += 1;
            return Ok(());
        }
        let list = IdList {
            next: self.header.free_chain,
            ids: std::mem::take(&mut self.header.free),
        };
        self.writes.insert(id, Some(encode(&list)?));
        self.header.free_chain = Some(id);
        Ok(())
    }

    async fn id_list(&self, id: u64) -> Result<IdList, FileSystemError> {
        let buf = self
            .page(id)
            .await?
            .ok_or_else(|| corrupt(format!("id list {} is missing", id)))?;
        decode(&buf, &format!("id list {}", id))
    }

    fn pages(&self, len: u64) -> u64 {
        len.div_ceil(self.lim.blksz as u64)
    }

    /// A large value's (id lists, pages), checked against its length.
    async fn overflow_ids(
        &self,
        index: u64,
        len: u64,
    ) -> Result<(Vec<u64>, Vec<u64>), FileSystemError> {
        let want = self.pages(len) as usize;
        let (mut lists, mut pages) = (Vec::new(), Vec::new());
        let mut next = Some(index);
        while let Some(id) = next {
            // A chain longer than the value needs: corrupt (or a cycle).
            if lists.len() > want.div_ceil(self.lim.ids_per_list) {
                return Err(corrupt(format!(
                    "value at {}: id list chain too long",
                    index
                )));
            }
            let list = self.id_list(id).await?;
            lists.push(id);
            pages.extend(list.ids);
            next = list.next;
        }
        if pages.len() != want {
            return Err(corrupt(format!(
                "value at {}: {} pages listed, {} bytes need {}",
                index,
                pages.len(),
                len,
                want
            )));
        }
        Ok((lists, pages))
    }

    async fn release_val(&mut self, v: &Val) -> Result<(), FileSystemError> {
        if let Val::Overflow { index, len } = v {
            let (lists, pages) = self.overflow_ids(*index, *len).await?;
            for id in lists.into_iter().chain(pages) {
                self.release(id)?;
            }
        }
        Ok(())
    }

    /// Store `value`: inline, or in pages listed from a chain of id lists.
    async fn make_val(&mut self, value: &[u8]) -> Result<Val, FileSystemError> {
        if value.len() <= self.lim.max_inline {
            return Ok(Val::Inline(value.to_vec()));
        }
        let mut pages = Vec::with_capacity(self.pages(value.len() as u64) as usize);
        for chunk in value.chunks(self.lim.blksz) {
            let id = self.alloc().await?;
            self.writes.insert(id, Some(chunk.to_vec()));
            pages.push(id);
        }
        let mut next = None;
        for ids in pages.chunks(self.lim.ids_per_list).rev() {
            let id = self.alloc().await?;
            let list = IdList {
                next,
                ids: ids.to_vec(),
            };
            self.writes.insert(id, Some(encode(&list)?));
            next = Some(id);
        }
        Ok(Val::Overflow {
            index: next.expect("a large value has pages"),
            len: value.len() as u64,
        })
    }

    #[cfg(not(target_arch = "wasm32"))]
    pub async fn read_val(&self, v: &Val) -> Result<Vec<u8>, FileSystemError> {
        match v {
            Val::Inline(b) => Ok(b.clone()),
            Val::Overflow { index, len } => {
                let (_, pages) = self.overflow_ids(*index, *len).await?;
                let mut out = Vec::with_capacity(*len as usize);
                for id in pages {
                    // An all-zero page is stored as a hole (see
                    // `FileSystem::build_tree`): it reads as zeros.
                    let n = (*len as usize - out.len()).min(self.lim.blksz);
                    let end = out.len() + n;
                    if let Some(page) = self.page(id).await? {
                        out.extend_from_slice(&page[..n.min(page.len())]);
                    }
                    out.resize(end, 0);
                }
                Ok(out)
            }
        }
    }

    /// The path from the root to the leaf that holds `key`: (id, node) per
    /// level, and the child index taken at each branch.
    async fn descend(
        &self,
        root: NonZeroU64,
        key: &[u8],
    ) -> Result<(Vec<(u64, Node)>, Vec<usize>), FileSystemError> {
        let mut path = Vec::new();
        let mut idxs = Vec::new();
        let mut id = root.get();
        for depth in 0..=self.header.height {
            let n = self.node(id).await?;
            match &n {
                Node::Branch { keys, children } if depth < self.header.height => {
                    let i = child_for(keys, key);
                    idxs.push(i);
                    let next = children[i];
                    path.push((id, n));
                    id = next;
                }
                Node::Leaf(_) if depth == self.header.height => {
                    path.push((id, n));
                    return Ok((path, idxs));
                }
                _ => {
                    return Err(corrupt(format!(
                        "node {} is at depth {} of a tree {} high",
                        id, depth, self.header.height
                    )));
                }
            }
        }
        unreachable!()
    }

    #[cfg(not(target_arch = "wasm32"))]
    pub async fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, FileSystemError> {
        let Some(root) = self.header.root else {
            return Ok(None);
        };
        let (mut path, _) = self.descend(root, key).await?;
        let Some((_, Node::Leaf(entries))) = path.pop() else {
            unreachable!()
        };
        match entries.binary_search_by(|(k, _)| k.as_slice().cmp(key)) {
            Ok(i) => Ok(Some(self.read_val(&entries[i].1).await?)),
            Err(_) => Ok(None),
        }
    }

    fn check_sizes(&self, key: &[u8], value: &[u8]) -> Result<(), FileSystemError> {
        if key.is_empty() || key.len() > self.lim.max_key {
            return Err(FileSystemError::InvalidArgument(format!(
                "keys are 1 to {} bytes (this one is {})",
                self.lim.max_key,
                key.len()
            )));
        }
        if value.len() > MAX_VALUE {
            return Err(FileSystemError::InvalidArgument(format!(
                "values are at most {} bytes (this one is {})",
                MAX_VALUE,
                value.len()
            )));
        }
        Ok(())
    }

    /// Write back the changed bottom of `path`, splitting what overflows
    /// and merging (or sharing out) what underflows, up to the root.
    async fn fixup(
        &mut self,
        mut path: Vec<(u64, Node)>,
        idxs: Vec<usize>,
    ) -> Result<(), FileSystemError> {
        let cap = self.lim.cap;
        let mut level = path.len() - 1;
        loop {
            let (id, node) = path[level].clone();
            let size = size_of(&node);
            if level == 0 {
                // The root.
                if size > cap {
                    let (left, sep, right) = split(node);
                    let rid = self.alloc().await?;
                    self.put_node(id, &left)?;
                    self.put_node(rid, &right)?;
                    let root = self.alloc().await?;
                    self.put_node(
                        root,
                        &Node::Branch {
                            keys: vec![sep],
                            children: vec![id, rid],
                        },
                    )?;
                    self.header.root = NonZeroU64::new(root);
                    self.header.height += 1;
                } else {
                    match node {
                        Node::Leaf(e) if e.is_empty() => {
                            self.release(id)?;
                            self.header.root = None;
                            self.header.height = 0;
                        }
                        Node::Branch { keys, children } if keys.is_empty() => {
                            self.release(id)?;
                            self.header.root = NonZeroU64::new(children[0]);
                            self.header.height -= 1;
                        }
                        n => self.put_node(id, &n)?,
                    }
                }
                self.header_dirty = true;
                return Ok(());
            }
            let ci = idxs[level - 1];
            if size > cap {
                let (left, sep, right) = split(node);
                let rid = self.alloc().await?;
                self.put_node(id, &left)?;
                self.put_node(rid, &right)?;
                let Node::Branch { keys, children } = &mut path[level - 1].1 else {
                    unreachable!()
                };
                keys.insert(ci, sep);
                children.insert(ci + 1, rid);
            } else if size < cap / 4 {
                let Node::Branch { keys, children } = path[level - 1].1.clone() else {
                    unreachable!()
                };
                if children.len() < 2 {
                    // An only child: its parent collapses or merges next.
                    self.put_node(id, &node)?;
                } else {
                    let (li, ri) = if ci > 0 { (ci - 1, ci) } else { (ci, ci + 1) };
                    let (lid, rid) = (children[li], children[ri]);
                    let (l, r) = if li == ci {
                        (node, self.node(rid).await?)
                    } else {
                        (self.node(lid).await?, node)
                    };
                    let sep = keys[li].clone();
                    let merged = join(l.clone(), sep, r.clone())?;
                    let Node::Branch { keys, children } = &mut path[level - 1].1 else {
                        unreachable!()
                    };
                    if size_of(&merged) <= cap {
                        self.put_node(lid, &merged)?;
                        self.release(rid)?;
                        keys.remove(li);
                        children.remove(ri);
                    } else {
                        let (nl, nsep, nr) = split(merged);
                        self.put_node(lid, &nl)?;
                        self.put_node(rid, &nr)?;
                        keys[li] = nsep;
                    }
                }
            } else {
                // Fits, and the parent didn't change.
                self.put_node(id, &node)?;
                return Ok(());
            }
            level -= 1;
        }
    }

    /// Up to `limit` entries with keys in `(start, end)`, in key order.
    #[cfg(not(target_arch = "wasm32"))]
    pub async fn range(
        &self,
        start: Bound<&[u8]>,
        end: Bound<&[u8]>,
        limit: usize,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>, FileSystemError> {
        let mut out = Vec::new();
        for (k, v) in self.range_raw(start, end, limit).await? {
            let v = self.read_val(&v).await?;
            out.push((k, v));
        }
        Ok(out)
    }

    /// As `range`, values unread (see `read_val`): large ones cost a read
    /// each, which a scan of keys doesn't need.
    #[cfg(not(target_arch = "wasm32"))]
    pub async fn range_raw(
        &self,
        start: Bound<&[u8]>,
        end: Bound<&[u8]>,
        limit: usize,
    ) -> Result<Vec<(Vec<u8>, Val)>, FileSystemError> {
        let mut out = Vec::new();
        let Some(root) = self.header.root else {
            return Ok(out);
        };
        if limit == 0 {
            return Ok(out);
        }
        let below_end = |k: &[u8]| match end {
            Bound::Included(e) => k <= e,
            Bound::Excluded(e) => k < e,
            Bound::Unbounded => true,
        };
        let after_start = |k: &[u8]| match start {
            Bound::Included(s) => k >= s,
            Bound::Excluded(s) => k > s,
            Bound::Unbounded => true,
        };
        // Branches on the way down, with the next child to visit.
        let mut stack: Vec<(Vec<u64>, usize)> = Vec::new();
        let mut id = root.get();
        // First descent: towards `start`.
        let mut seek = match start {
            Bound::Included(s) | Bound::Excluded(s) => Some(s),
            Bound::Unbounded => None,
        };
        loop {
            match self.node(id).await? {
                Node::Branch { keys, children } => {
                    let i = seek.map_or(0, |s| child_for(&keys, s));
                    id = children[i];
                    stack.push((children, i + 1));
                }
                Node::Leaf(entries) => {
                    for (k, v) in entries {
                        if !after_start(&k) {
                            continue;
                        }
                        if !below_end(&k) {
                            return Ok(out);
                        }
                        out.push((k, v));
                        if out.len() >= limit {
                            return Ok(out);
                        }
                    }
                    seek = None;
                    // The next leaf: up to the nearest branch with children
                    // left, then down its leftmost path.
                    loop {
                        let Some((children, next)) = stack.last_mut() else {
                            return Ok(out);
                        };
                        if *next < children.len() {
                            id = children[*next];
                            *next += 1;
                            break;
                        }
                        stack.pop();
                    }
                }
            }
        }
    }
}

/// Split an overflowing node in two: (left, separator, right).
fn split(n: Node) -> (Node, Vec<u8>, Node) {
    match n {
        Node::Leaf(mut entries) => {
            let sizes: Vec<usize> = entries.iter().map(size_of).collect();
            let m = split_point(&sizes);
            let right = entries.split_off(m);
            let sep = right[0].0.clone();
            (Node::Leaf(entries), sep, Node::Leaf(right))
        }
        Node::Branch {
            mut keys,
            mut children,
        } => {
            // Key `m` moves up; keys and children before it stay left.
            let sizes: Vec<usize> = keys.iter().map(|k| size_of(k) + 8).collect();
            let m = split_point(&sizes).min(keys.len() - 1).max(1);
            let rkeys = keys.split_off(m + 1);
            let sep = keys.pop().unwrap();
            let rchildren = children.split_off(m + 1);
            (
                Node::Branch { keys, children },
                sep,
                Node::Branch {
                    keys: rkeys,
                    children: rchildren,
                },
            )
        }
    }
}

/// Two siblings and the separator between them, as one node.
fn join(l: Node, sep: Vec<u8>, r: Node) -> Result<Node, FileSystemError> {
    match (l, r) {
        (Node::Leaf(mut a), Node::Leaf(b)) => {
            a.extend(b);
            Ok(Node::Leaf(a))
        }
        (
            Node::Branch {
                keys: mut ka,
                children: mut ca,
            },
            Node::Branch {
                keys: kb,
                children: cb,
            },
        ) => {
            ka.push(sep);
            ka.extend(kb);
            ca.extend(cb);
            Ok(Node::Branch {
                keys: ka,
                children: ca,
            })
        }
        _ => Err(corrupt("siblings at different levels".into())),
    }
}

/// What [`check`] found.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct CheckReport {
    pub entries: u64,
    pub nodes: u64,
    pub overflow_pages: u64,
    /// Freed ids, ready for reuse.
    pub free_ids: u64,
    pub errors: Vec<String>,
}

/// Verify the committed tree in `obj` (a fresh `ObjTree` of a dnode's
/// tree): structure, key order, depths, the entry count, that each node
/// and overflow page is used once, that every id below `next_id` is used
/// or free (none lost), and that every record of the object is the header
/// or reachable.
pub(crate) async fn check<D: PoolDevice, R: Role>(
    fs: &FileSystem<D, R>,
    obj: &ObjTree,
) -> CheckReport {
    let mut r = CheckReport::default();
    let t = match Txn::begin(fs, obj).await {
        Ok(t) => t,
        Err(e) => {
            r.errors.push(format!("header: {}", e));
            return r;
        }
    };
    let h = t.header.clone();
    let mut used: BTreeSet<u64> = BTreeSet::new();
    used.insert(0);
    let mut count = 0u64;
    if let Some(root) = h.root {
        // (node, depth, lower bound (incl.), upper bound (excl.))
        type Visit = (u64, u32, Option<Vec<u8>>, Option<Vec<u8>>);
        let mut stack: Vec<Visit> = vec![(root.get(), 0, None, None)];
        while let Some((id, depth, lo, hi)) = stack.pop() {
            if id == 0 || id >= h.next_id {
                r.errors.push(format!("node id {} out of range", id));
                continue;
            }
            if !used.insert(id) {
                r.errors
                    .push(format!("node {} is reached more than once", id));
                continue;
            }
            r.nodes += 1;
            let n = match t.node(id).await {
                Ok(n) => n,
                Err(e) => {
                    r.errors.push(format!("node {}: {}", id, e));
                    continue;
                }
            };
            let in_bounds = |k: &[u8]| {
                lo.as_deref().is_none_or(|l| k >= l) && hi.as_deref().is_none_or(|h| k < h)
            };
            match n {
                Node::Leaf(entries) => {
                    if depth != h.height {
                        r.errors.push(format!(
                            "leaf {} at depth {} (height {})",
                            id, depth, h.height
                        ));
                    }
                    if entries.is_empty() {
                        r.errors.push(format!("leaf {} is empty", id));
                    }
                    for w in entries.windows(2) {
                        if w[0].0 >= w[1].0 {
                            r.errors.push(format!("leaf {}: keys out of order", id));
                        }
                    }
                    for (k, v) in &entries {
                        count += 1;
                        if !in_bounds(k) {
                            r.errors
                                .push(format!("leaf {}: key outside its parent's range", id));
                        }
                        if let Val::Overflow { index, len } = v {
                            let (lists, pages) = match t.overflow_ids(*index, *len).await {
                                Ok(ids) => ids,
                                Err(e) => {
                                    r.errors.push(format!("leaf {}: {}", id, e));
                                    continue;
                                }
                            };
                            for p in lists.iter().chain(&pages) {
                                if *p == 0 || *p >= h.next_id || !used.insert(*p) {
                                    r.errors.push(format!(
                                        "leaf {}: overflow page {} out of range or reused",
                                        id, p
                                    ));
                                }
                            }
                            for p in pages {
                                r.overflow_pages += 1;
                                // A hole is an all-zero page, not a missing one.
                                if let Err(e) = t.page(p).await {
                                    r.errors.push(format!("overflow page {}: {}", p, e));
                                }
                            }
                        }
                    }
                }
                Node::Branch { keys, children } => {
                    if depth >= h.height {
                        r.errors.push(format!(
                            "branch {} at depth {} (height {})",
                            id, depth, h.height
                        ));
                        continue;
                    }
                    if children.len() != keys.len() + 1 || keys.is_empty() {
                        r.errors.push(format!(
                            "branch {}: {} keys, {} children",
                            id,
                            keys.len(),
                            children.len()
                        ));
                        continue;
                    }
                    for w in keys.windows(2) {
                        if w[0] >= w[1] {
                            r.errors.push(format!("branch {}: keys out of order", id));
                        }
                    }
                    for k in &keys {
                        if !in_bounds(k) {
                            r.errors
                                .push(format!("branch {}: key outside its parent's range", id));
                        }
                    }
                    for (i, &c) in children.iter().enumerate() {
                        let clo = if i == 0 {
                            lo.clone()
                        } else {
                            Some(keys[i - 1].clone())
                        };
                        let chi = if i == keys.len() {
                            hi.clone()
                        } else {
                            Some(keys[i].clone())
                        };
                        stack.push((c, depth + 1, clo, chi));
                    }
                }
            }
        }
    }
    r.entries = count;
    if count != h.entries {
        r.errors.push(format!(
            "header counts {} entries, the tree has {}",
            h.entries, count
        ));
    }
    // Free ids: the header's and the spilled chain's, each once, none in
    // use; the chain's pages are records of their own.
    let mut free: Vec<u64> = h.free.clone();
    let mut next = h.free_chain;
    while let Some(id) = next {
        if id == 0 || id >= h.next_id || !used.insert(id) {
            r.errors
                .push(format!("free id list {} out of range or reused", id));
            break;
        }
        match t.id_list(id).await {
            Ok(list) => {
                free.extend(list.ids);
                next = list.next;
            }
            Err(e) => {
                r.errors.push(format!("free id list {}: {}", id, e));
                break;
            }
        }
    }
    let mut seen = BTreeSet::new();
    for f in &free {
        if *f == 0 || *f >= h.next_id || used.contains(f) || !seen.insert(*f) {
            r.errors.push(format!(
                "free node id {} is in use, out of range or listed twice",
                f
            ));
        }
    }
    if free.len() as u64 != h.free_total {
        r.errors.push(format!(
            "header counts {} free ids, the lists have {}",
            h.free_total,
            free.len()
        ));
    }
    r.free_ids = free.len() as u64;
    // Every id below `next_id` is used or free: none is lost.
    let lost = h
        .next_id
        .saturating_sub(used.len() as u64 + seen.len() as u64);
    if lost != 0 && r.errors.is_empty() {
        r.errors.push(format!(
            "{} node ids are neither in use nor free (leaked)",
            lost
        ));
    }
    // Every record must be the header, a node or an overflow page.
    match fs.tree_leaves(&obj.root()).await {
        Ok(leaves) => {
            for (idx, _) in leaves {
                if !used.contains(&idx) {
                    r.errors
                        .push(format!("record {} belongs to nothing (leaked)", idx));
                }
            }
        }
        Err(e) => r.errors.push(format!("listing records: {}", e)),
    }
    r
}

impl<'a, D: PoolDevice, R: Mutable> Txn<'a, D, R> {
    /// Set `key` to `value`. Returns whether it replaced a value.
    pub async fn put(&mut self, key: &[u8], value: &[u8]) -> Result<bool, FileSystemError> {
        self.check_sizes(key, value)?;
        let val = self.make_val(value).await?;
        let Some(root) = self.header.root else {
            let id = self.alloc().await?;
            self.put_node(id, &Node::Leaf(vec![(key.to_vec(), val)]))?;
            self.header.root = NonZeroU64::new(id);
            self.header.height = 0;
            self.header.entries = 1;
            self.effects
                .push(KeyedOp::Put(key.to_vec(), value.to_vec()));
            return Ok(false);
        };
        let (mut path, idxs) = self.descend(root, key).await?;
        let replaced = {
            let Some((_, Node::Leaf(entries))) = path.last_mut() else {
                unreachable!()
            };
            match entries.binary_search_by(|(k, _)| k.as_slice().cmp(key)) {
                Ok(i) => Some(std::mem::replace(&mut entries[i].1, val)),
                Err(i) => {
                    entries.insert(i, (key.to_vec(), val));
                    None
                }
            }
        };
        let was = replaced.is_some();
        match replaced {
            Some(old) => self.release_val(&old).await?,
            None => self.header.entries += 1,
        }
        self.header_dirty = true;
        self.fixup(path, idxs).await?;
        self.effects
            .push(KeyedOp::Put(key.to_vec(), value.to_vec()));
        Ok(was)
    }

    /// Remove `key`. Returns whether it was there.
    pub async fn delete(&mut self, key: &[u8]) -> Result<bool, FileSystemError> {
        let Some(root) = self.header.root else {
            return Ok(false);
        };
        let (mut path, idxs) = self.descend(root, key).await?;
        let removed = {
            let Some((_, Node::Leaf(entries))) = path.last_mut() else {
                unreachable!()
            };
            match entries.binary_search_by(|(k, _)| k.as_slice().cmp(key)) {
                Ok(i) => Some(entries.remove(i).1),
                Err(_) => None,
            }
        };
        let Some(old) = removed else {
            return Ok(false);
        };
        self.release_val(&old).await?;
        self.header.entries -= 1;
        self.header_dirty = true;
        self.fixup(path, idxs).await?;
        self.effects.push(KeyedOp::Delete(key.to_vec()));
        Ok(true)
    }

    /// Apply every change made through this transaction at once, and
    /// queue them for the intent log as one entry: a replay redoes all of
    /// them or none, as a commit has them all or none.
    #[cfg(not(target_arch = "wasm32"))]
    pub fn apply_logged(mut self) -> Result<(), FileSystemError> {
        let op = match self.effects.len() {
            0 => None,
            1 => self.effects.pop(),
            _ => Some(KeyedOp::Batch(std::mem::take(&mut self.effects))),
        };
        self.apply(op)
    }

    /// Apply this operation's writes (all at once; see the module docs),
    /// queuing `op` (its effect) for the intent log if it changed anything
    /// (None: not logged; durable once committed).
    pub fn apply(mut self, op: Option<KeyedOp>) -> Result<(), FileSystemError> {
        if self.header_dirty {
            let h = encode(&self.header)?;
            if h.len() > self.lim.blksz {
                return Err(corrupt("header overflows its record".into()));
            }
            self.writes.insert(0, Some(h));
        }
        if self.writes.is_empty() {
            return Ok(());
        }
        self.obj.put_records(
            self.writes
                .into_iter()
                .map(|(id, b)| (id, b.map_or(Dirty::Hole, Dirty::Data))),
            op,
        );
        Ok(())
    }

    /// Redo `op` (an intent log entry) in this transaction.
    pub(crate) async fn redo(&mut self, op: &KeyedOp) -> Result<(), FileSystemError> {
        match op {
            KeyedOp::Put(k, v) => {
                self.put(k, v).await?;
            }
            KeyedOp::Delete(k) => {
                self.delete(k).await?;
            }
            KeyedOp::Batch(ops) => {
                for op in ops {
                    Box::pin(self.redo(op)).await?;
                }
            }
        }
        Ok(())
    }
}

#[cfg(test)]
#[path = "tests/btree_tests.rs"]
mod tests;
