//! NFSv4 attributes (RFC 8881 §5): bitmaps, and `fattr4` encoded in
//! attribute-number order.

use super::err;
use super::state::LEASE_SECS;
use crate::inode::{ObjType, SetAttrs};
use crate::nfs::backend::Attr;
use crate::nfs::xdr::{Reader, Writer};

pub const SUPPORTED_ATTRS: u32 = 0;
pub const TYPE: u32 = 1;
pub const FH_EXPIRE_TYPE: u32 = 2;
pub const CHANGE: u32 = 3;
pub const SIZE: u32 = 4;
pub const LINK_SUPPORT: u32 = 5;
pub const SYMLINK_SUPPORT: u32 = 6;
pub const NAMED_ATTR: u32 = 7;
pub const FSID: u32 = 8;
pub const UNIQUE_HANDLES: u32 = 9;
pub const LEASE_TIME: u32 = 10;
pub const RDATTR_ERROR: u32 = 11;
pub const ACLSUPPORT: u32 = 13;
pub const CANSETTIME: u32 = 15;
pub const CASE_INSENSITIVE: u32 = 16;
pub const CASE_PRESERVING: u32 = 17;
pub const CHOWN_RESTRICTED: u32 = 18;
pub const FILEHANDLE: u32 = 19;
pub const FILEID: u32 = 20;
pub const FILES_AVAIL: u32 = 21;
pub const FILES_FREE: u32 = 22;
pub const FILES_TOTAL: u32 = 23;
pub const FS_LOCATIONS: u32 = 24;
pub const HOMOGENEOUS: u32 = 26;
pub const MAXFILESIZE: u32 = 27;
pub const MAXLINK: u32 = 28;
pub const MAXNAME: u32 = 29;
pub const MAXREAD: u32 = 30;
pub const MAXWRITE: u32 = 31;
pub const MODE: u32 = 33;
pub const NO_TRUNC: u32 = 34;
pub const NUMLINKS: u32 = 35;
pub const OWNER: u32 = 36;
pub const OWNER_GROUP: u32 = 37;
pub const RAWDEV: u32 = 41;
pub const SPACE_AVAIL: u32 = 42;
pub const SPACE_FREE: u32 = 43;
pub const SPACE_TOTAL: u32 = 44;
pub const SPACE_USED: u32 = 45;
pub const TIME_ACCESS: u32 = 47;
pub const TIME_ACCESS_SET: u32 = 48;
pub const TIME_DELTA: u32 = 51;
pub const TIME_METADATA: u32 = 52;
pub const TIME_MODIFY: u32 = 53;
pub const TIME_MODIFY_SET: u32 = 54;
pub const MOUNTED_ON_FILEID: u32 = 55;
pub const FS_LAYOUT_TYPE: u32 = 62;
pub const LAYOUT_BLKSIZE: u32 = 65;
pub const SUPPATTR_EXCLCREAT: u32 = 75;
/// NFSv4.2: how `change` behaves.
pub const CHANGE_ATTR_TYPE: u32 = 79;

/// Every attribute this server reports.
pub const SUPPORTED: &[u32] = &[
    SUPPORTED_ATTRS,
    TYPE,
    FH_EXPIRE_TYPE,
    CHANGE,
    SIZE,
    LINK_SUPPORT,
    SYMLINK_SUPPORT,
    NAMED_ATTR,
    FSID,
    UNIQUE_HANDLES,
    LEASE_TIME,
    RDATTR_ERROR,
    ACLSUPPORT,
    CANSETTIME,
    CASE_INSENSITIVE,
    CASE_PRESERVING,
    CHOWN_RESTRICTED,
    FILEHANDLE,
    FILEID,
    FILES_AVAIL,
    FILES_FREE,
    FILES_TOTAL,
    FS_LOCATIONS,
    HOMOGENEOUS,
    MAXFILESIZE,
    MAXLINK,
    MAXNAME,
    MAXREAD,
    MAXWRITE,
    MODE,
    NO_TRUNC,
    NUMLINKS,
    OWNER,
    OWNER_GROUP,
    RAWDEV,
    SPACE_AVAIL,
    SPACE_FREE,
    SPACE_TOTAL,
    SPACE_USED,
    TIME_ACCESS,
    TIME_ACCESS_SET,
    TIME_DELTA,
    TIME_METADATA,
    TIME_MODIFY,
    TIME_MODIFY_SET,
    MOUNTED_ON_FILEID,
    FS_LAYOUT_TYPE,
    LAYOUT_BLKSIZE,
    SUPPATTR_EXCLCREAT,
];

/// Every attribute this server reports in minor version `minor`.
pub fn supported(minor: u32) -> Vec<u32> {
    let mut all = SUPPORTED.to_vec();
    if minor >= 2 {
        all.push(CHANGE_ATTR_TYPE);
    }
    all
}

/// Attributes an exclusive create (`EXCLUSIVE4_1`) may set along with
/// its verifier (which is kept in the modification time).
pub const EXCLCREAT: &[u32] = &[SIZE, MODE, OWNER, OWNER_GROUP];

/// Largest READ / WRITE.
pub const MAX_IO: u64 = 1 << 20;

pub fn has(bm: &[u32], n: u32) -> bool {
    bm.get((n / 32) as usize)
        .is_some_and(|w| w & (1 << (n % 32)) != 0)
}

pub fn set(bm: &mut Vec<u32>, n: u32) {
    let i = (n / 32) as usize;
    if bm.len() <= i {
        bm.resize(i + 1, 0);
    }
    bm[i] |= 1 << (n % 32);
}

pub fn bitmap_of(attrs: &[u32]) -> Vec<u32> {
    let mut bm = Vec::new();
    for &a in attrs {
        set(&mut bm, a);
    }
    bm
}

pub fn read_bitmap(r: &mut Reader<'_>) -> crate::nfs::xdr::XdrResult<Vec<u32>> {
    let n = r.u32()? as usize;
    if n > 8 {
        return Err(crate::nfs::xdr::XdrError("bitmap too long"));
    }
    (0..n).map(|_| r.u32()).collect()
}

pub fn write_bitmap(w: &mut Writer, bm: &[u32]) {
    w.u32(bm.len() as u32);
    for &x in bm {
        w.u32(x);
    }
}

fn time(w: &mut Writer, ns: u64) {
    w.u64(ns / 1_000_000_000).u32((ns % 1_000_000_000) as u32);
}

/// What else the attributes may need besides the object's `Attr`.
pub struct Extra<'a> {
    pub fh: &'a [u8],
    /// (total, free) bytes, if any space attribute was asked for.
    pub space: Option<(u64, u64)>,
    /// Every node's address (`fs_locations`).
    pub locations: &'a [String],
    /// The request's minor version.
    pub minor: u32,
    /// Whether pNFS (flexfiles) layouts are handed out.
    pub pnfs: bool,
}

/// Are any of the space attributes (which cost a look at the pool) asked?
pub fn wants_space(bm: &[u32]) -> bool {
    [
        SPACE_AVAIL,
        SPACE_FREE,
        SPACE_TOTAL,
        FILES_AVAIL,
        FILES_FREE,
        FILES_TOTAL,
    ]
    .iter()
    .any(|&a| has(bm, a))
}

/// `fattr4` with whichever of `want` is supported.
pub fn encode(w: &mut Writer, want: &[u32], a: &Attr, x: &Extra<'_>) {
    let mut got = Vec::new();
    let mut v = Writer::new();
    let files = 1u64 << 40;
    for n in supported(x.minor) {
        if !has(want, n) {
            continue;
        }
        match n {
            SUPPORTED_ATTRS => write_bitmap(&mut v, &bitmap_of(&supported(x.minor))),
            CHANGE_ATTR_TYPE => {
                // NFS4_CHANGE_TYPE_IS_MONOTONIC_INCR: it only ever goes up.
                v.u32(0);
            }
            TYPE => {
                v.u32(if a.kind == ObjType::Directory { 2 } else { 1 });
            }
            FH_EXPIRE_TYPE => {
                v.u32(0); // FH4_PERSISTENT
            }
            CHANGE => {
                v.u64(a.change);
            }
            SIZE => {
                v.u64(a.size);
            }
            LINK_SUPPORT | SYMLINK_SUPPORT | NAMED_ATTR | CASE_INSENSITIVE => {
                v.bool(false);
            }
            UNIQUE_HANDLES | CANSETTIME | CASE_PRESERVING | CHOWN_RESTRICTED | HOMOGENEOUS
            | NO_TRUNC => {
                v.bool(true);
            }
            FSID => {
                v.u64(a.fsid).u64(0);
            }
            LEASE_TIME => {
                v.u32(LEASE_SECS);
            }
            RDATTR_ERROR => {
                v.u32(err::OK);
            }
            ACLSUPPORT => {
                v.u32(0);
            }
            FILEHANDLE => {
                v.opaque(x.fh);
            }
            FILEID | MOUNTED_ON_FILEID => {
                v.u64(a.id);
            }
            FILES_AVAIL | FILES_FREE | FILES_TOTAL => {
                v.u64(files);
            }
            FS_LOCATIONS => {
                // fs_root: the root (there's one file system). Then one
                // location: the same server at every node's address.
                v.u32(0);
                if x.locations.is_empty() {
                    v.u32(0);
                } else {
                    v.u32(1).u32(x.locations.len() as u32);
                    for l in x.locations {
                        v.string(l);
                    }
                    v.u32(0); // rootpath: the root
                }
            }
            MAXFILESIZE => {
                v.u64(1 << 62);
            }
            MAXLINK => {
                v.u32(1);
            }
            MAXNAME => {
                v.u32(crate::inode::MAX_NAME_BYTES as u32);
            }
            MAXREAD | MAXWRITE => {
                v.u64(MAX_IO);
            }
            MODE => {
                v.u32(a.mode as u32);
            }
            NUMLINKS => {
                v.u32(a.nlink);
            }
            OWNER => {
                v.string(&a.uid.to_string());
            }
            OWNER_GROUP => {
                v.string(&a.gid.to_string());
            }
            RAWDEV => {
                v.u32(0).u32(0);
            }
            SPACE_AVAIL | SPACE_FREE => {
                v.u64(x.space.map_or(0, |s| s.1));
            }
            SPACE_TOTAL => {
                v.u64(x.space.map_or(0, |s| s.0));
            }
            SPACE_USED => {
                v.u64(a.size.div_ceil(4096) * 4096);
            }
            TIME_ACCESS | TIME_MODIFY => time(&mut v, a.mtime_ns),
            TIME_METADATA => time(&mut v, a.ctime_ns),
            TIME_DELTA => time(&mut v, 1),
            SUPPATTR_EXCLCREAT => write_bitmap(&mut v, &bitmap_of(EXCLCREAT)),
            FS_LAYOUT_TYPE => {
                if x.pnfs {
                    v.u32(1).u32(super::LAYOUT4_FLEX_FILES);
                } else {
                    v.u32(0);
                }
            }
            LAYOUT_BLKSIZE => {
                v.u32(MAX_IO as u32);
            }
            // Settable only.
            TIME_ACCESS_SET | TIME_MODIFY_SET => continue,
            _ => continue,
        }
        set(&mut got, n);
    }
    write_bitmap(w, &got);
    w.opaque(&v.buf);
}

/// A user or group as an NFSv4 owner string: numbers, as `sec=sys`
/// clients send them (and `root` / `nobody`). Anything else: `BADOWNER`.
fn id_of(s: &str) -> Result<u32, u32> {
    let name = s.split('@').next().unwrap_or("");
    match name {
        "root" => Ok(0),
        "nobody" | "nogroup" => Ok(65534),
        n => n.parse().map_err(|_| err::BADOWNER),
    }
}

/// Attributes a client sets (SETATTR, CREATE, OPEN): what to set, the
/// size if given, and the bitmap of what was set. `now` is the server time.
#[derive(Clone)]
pub struct Settable {
    pub set: SetAttrs,
    pub size: Option<u64>,
    pub bitmap: Vec<u32>,
}

pub fn decode(r: &mut Reader<'_>, now: u64) -> Result<Settable, u32> {
    let bm = read_bitmap(r).map_err(|_| err::BADXDR)?;
    let vals = r.opaque(1 << 16).map_err(|_| err::BADXDR)?;
    let mut v = Reader::new(vals);
    let mut out = Settable {
        set: SetAttrs::default(),
        size: None,
        bitmap: Vec::new(),
    };
    let bad = |_| err::BADXDR;
    let settime = |v: &mut Reader<'_>| -> Result<u64, u32> {
        match v.u32().map_err(bad)? {
            0 => Ok(now), // SET_TO_SERVER_TIME4
            1 => {
                let s = v.u64().map_err(bad)?;
                let ns = v.u32().map_err(bad)?;
                if ns >= 1_000_000_000 {
                    return Err(err::INVAL);
                }
                Ok(s.saturating_mul(1_000_000_000) + ns as u64)
            }
            _ => Err(err::BADXDR),
        }
    };
    for word in 0..bm.len() as u32 * 32 {
        if !has(&bm, word) {
            continue;
        }
        match word {
            SIZE => out.size = Some(v.u64().map_err(bad)?),
            MODE => out.set.mode = Some((v.u32().map_err(bad)? & 0o7777) as u16),
            OWNER => out.set.uid = Some(id_of(&v.string(1024).map_err(bad)?)?),
            OWNER_GROUP => out.set.gid = Some(id_of(&v.string(1024).map_err(bad)?)?),
            TIME_ACCESS_SET => {
                settime(&mut v)?; // no access times kept
            }
            TIME_MODIFY_SET => out.set.mtime_ns = Some(settime(&mut v)?),
            n if SUPPORTED.contains(&n) => return Err(err::INVAL), // read-only
            _ => return Err(err::ATTRNOTSUPP),
        }
        set(&mut out.bitmap, word);
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn settable_attributes_decode() {
        let mut vals = Writer::new();
        vals.u64(1234); // size (4)
        vals.u32(0o640); // mode (33)
        vals.string("1000"); // owner (36)
        vals.string("root@example"); // owner_group (37)
        vals.u32(1).u64(5).u32(6); // time_modify_set (54): client time
        let mut w = Writer::new();
        write_bitmap(
            &mut w,
            &bitmap_of(&[SIZE, MODE, OWNER, OWNER_GROUP, TIME_MODIFY_SET]),
        );
        w.opaque(&vals.buf);
        let s = decode(&mut Reader::new(&w.buf), 99).unwrap();
        assert_eq!(s.size, Some(1234));
        assert_eq!(
            (s.set.mode, s.set.uid, s.set.gid, s.set.mtime_ns),
            (Some(0o640), Some(1000), Some(0), Some(5_000_000_006))
        );
        // Read-only, unsupported, and owners we can't map.
        let one = |a: u32, val: &[u8]| {
            let mut w = Writer::new();
            write_bitmap(&mut w, &bitmap_of(&[a]));
            w.opaque(val);
            decode(&mut Reader::new(&w.buf), 0).err()
        };
        assert_eq!(one(FILEID, &[0; 8]), Some(err::INVAL));
        assert_eq!(one(12, &[]), Some(err::ATTRNOTSUPP)); // acl
        let mut s = Writer::new();
        s.string("alice");
        assert_eq!(one(OWNER, &s.buf), Some(err::BADOWNER));
    }

    #[test]
    fn only_supported_attributes_are_returned() {
        let a = Attr {
            id: 7,
            kind: ObjType::File,
            mode: 0o644,
            nlink: 1,
            uid: 5,
            gid: 6,
            size: 10,
            mtime_ns: 3_000_000_001,
            ctime_ns: 4,
            fsid: 9,
            change: 42,
        };
        let mut w = Writer::new();
        // size, change and an acl (12) we don't support.
        encode(
            &mut w,
            &bitmap_of(&[SIZE, CHANGE, 12, TIME_MODIFY]),
            &a,
            &Extra {
                fh: b"fh",
                space: None,
                locations: &[],
                minor: 1,
                pnfs: false,
            },
        );
        let mut r = Reader::new(&w.buf);
        assert_eq!(
            read_bitmap(&mut r).unwrap(),
            bitmap_of(&[CHANGE, SIZE, TIME_MODIFY])
        );
        let vals = r.opaque(1024).unwrap();
        let mut v = Reader::new(vals);
        assert_eq!((v.u64().unwrap(), v.u64().unwrap()), (42, 10));
        assert_eq!((v.u64().unwrap(), v.u32().unwrap()), (3, 1));
    }
}
