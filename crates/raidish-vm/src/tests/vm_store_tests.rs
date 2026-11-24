//! The VM definition store: what it refuses, change numbers, reopening.

use super::*;
use crate::test_util::{DEFAULT_VOLBLOCKSIZE, TestPool};

fn xml(vols: &[&str]) -> String {
    let disks: String = vols
        .iter()
        .map(|v| {
            format!(
                "<disk type='file' device='disk'><source file='raidish:{}'/></disk>",
                v
            )
        })
        .collect();
    format!("<domain type='kvm'><devices>{}</devices></domain>", disks)
}

async fn store_with_volumes(fs: &Arc<FileSystem<crate::vdev::Pool>>, vols: &[&str]) {
    for v in vols {
        fs.create_block(v, 1 << 20, DEFAULT_VOLBLOCKSIZE)
            .await
            .unwrap();
    }
}

#[tokio::test]
async fn stores_checks_and_reopens() {
    let pool = TestPool::new().await;
    {
        let fs = Arc::new(pool.format().await);
        store_with_volumes(&fs, &["a", "b", "c"]).await;
        fs.sync().await.unwrap();
        let st = VmStore::open(fs.clone()).await.unwrap();
        // Not a dataset users see.
        assert!(fs.list_btree_datasets().await.unwrap().is_empty());

        let one = st.put("one", &xml(&["a", "b"]), true, None).await.unwrap();
        assert_eq!(one.change, 1);
        // Exists.
        assert!(matches!(
            st.put("one", &xml(&["a"]), true, None).await,
            Err(FileSystemError::Conflict(_))
        ));
        // A volume is one VM's.
        let e = st
            .put("two", &xml(&["c", "b"]), true, None)
            .await
            .unwrap_err();
        assert!(e.to_string().contains("VM 'one'"), "{}", e);
        // No such volume.
        assert!(st.put("two", &xml(&["nope"]), true, None).await.is_err());
        // Reaches the host.
        let host = "<domain><devices><disk device='disk'><source file='raidish:c'/></disk>\
                    <hostdev mode='subsystem' type='pci'/></devices></domain>";
        assert!(st.put("two", host, true, None).await.is_err());
        // Bad names.
        assert!(st.put("../x", &xml(&["c"]), true, None).await.is_err());

        // Changing: a stale change number is refused, the current one isn't.
        let two = st.put("two", &xml(&["c"]), true, None).await.unwrap();
        assert_eq!(two.change, 2);
        assert!(matches!(
            st.put("one", &xml(&["a"]), false, Some(7)).await,
            Err(FileSystemError::Conflict(_))
        ));
        let one = st.put("one", &xml(&["a"]), false, Some(1)).await.unwrap();
        assert_eq!(one.change, 3);
        // `b` is free again.
        assert_eq!(st.user_of("b").await.unwrap(), None);
        assert_eq!(st.user_of("c").await.unwrap().as_deref(), Some("two"));

        st.delete("two", Some(2)).await.unwrap();
        assert!(matches!(
            st.delete("two", None).await,
            Err(FileSystemError::FileNotFound)
        ));
        // Defined again: a new number, never an old one.
        let two = st.put("two", &xml(&["c"]), true, None).await.unwrap();
        assert_eq!(two.change, 4);
    }
    let fs = Arc::new(pool.load().await);
    let st = VmStore::open(fs).await.unwrap();
    let all = st.list().await.unwrap();
    assert_eq!(all.keys().collect::<Vec<_>>(), vec!["one", "two"]);
    assert_eq!(all["one"].spec().volumes(), vec!["a"]);
    assert_eq!(all["two"].change, 4);
}

/// Node facts sit next to the definitions without being one.
#[tokio::test]
async fn node_facts_next_to_the_definitions() {
    let pool = TestPool::new().await;
    let fs = Arc::new(pool.format().await);
    store_with_volumes(&fs, &["a"]).await;
    let st = VmStore::open(fs.clone()).await.unwrap();
    st.put("one", &xml(&["a"]), true, None).await.unwrap();
    let mut f = crate::api_types::NodeFacts {
        node: "node-1".into(),
        ..Default::default()
    };
    f.host.cpu_model = "AMD Ryzen 9 7900X3D".into();
    st.put_node(&f).await.unwrap();
    f.host.kvm = true;
    st.put_node(&f).await.unwrap();
    assert!(st.put_node(&Default::default()).await.is_err());
    let nodes = st.nodes().await.unwrap();
    assert_eq!(nodes.len(), 1);
    assert!(nodes["node-1"].host.kvm);
    assert_eq!(
        st.list().await.unwrap().keys().collect::<Vec<_>>(),
        vec!["one"]
    );
}
