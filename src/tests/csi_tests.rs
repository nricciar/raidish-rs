//! Which volume capabilities the driver takes.

use super::*;
use pb::volume_capability::{AccessMode, BlockVolume, MountVolume};

fn cap(mode: Mode, block: bool) -> pb::VolumeCapability {
    pb::VolumeCapability {
        access_mode: Some(AccessMode { mode: mode as i32 }),
        access_type: Some(match block {
            true => AccessType::Block(BlockVolume {}),
            false => AccessType::Mount(MountVolume::default()),
        }),
    }
}

#[test]
fn single_node_modes_take_either_access_type() {
    for mode in [
        Mode::SingleNodeWriter,
        Mode::SingleNodeSingleWriter,
        Mode::SingleNodeMultiWriter,
    ] {
        for block in [false, true] {
            assert!(supported(&cap(mode, block)).is_ok(), "{:?} {}", mode, block);
            assert!(!multi_node(&cap(mode, block)));
        }
    }
}

/// Several nodes at once: a raw device only, never a mounted filesystem.
#[test]
fn many_nodes_only_for_raw_block() {
    let rwx = cap(Mode::MultiNodeMultiWriter, true);
    assert!(supported(&rwx).is_ok());
    assert!(multi_node(&rwx));
    let err = supported(&cap(Mode::MultiNodeMultiWriter, false)).unwrap_err();
    assert!(err.contains("raw block"), "{}", err);
    for mode in [Mode::MultiNodeReaderOnly, Mode::MultiNodeSingleWriter] {
        assert!(supported(&cap(mode, true)).is_err(), "{:?}", mode);
    }
}
