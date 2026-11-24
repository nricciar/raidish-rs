//! The node's HTTP API for the file manager: files (`/api/v1/files`,
//! `/api/v1/dir`, see `raidish::files_api`) and volumes (`/api/v1/volumes`,
//! passed on to the committer by whichever node answers).

use crate::session::token;
use gloo_net::http::{Request, RequestBuilder, Response};
pub use raidish_core::inode::DatasetProps;
pub use raidish_types::api_types::{
    CheckView, CreateVolume, DirEntry, FactsView, MoveTo, NewName, NodeFacts, RenameRequest,
    Verdict, VmDef, VmView, VolumeView,
};

use serde::{Serialize, de::DeserializeOwned};

/// `a/b c` → `a/b%20c` (each part encoded, the slashes kept).
pub fn encode_path(path: &str) -> String {
    path.split('/')
        .filter(|p| !p.is_empty())
        .map(|p| String::from(js_sys::encode_uri_component(p)))
        .collect::<Vec<_>>()
        .join("/")
}

fn enc(s: &str) -> String {
    js_sys::encode_uri_component(s).into()
}

fn auth(req: RequestBuilder) -> RequestBuilder {
    match token() {
        Some(t) => req.header("Authorization", &format!("Bearer {}", t)),
        None => req,
    }
}

/// A link that downloads `path` (the token goes in the query: a plain link
/// can't carry a header).
pub fn download_url(path: &str) -> String {
    let mut url = format!("/api/v1/files/{}", encode_path(path));
    if let Some(t) = token() {
        url.push_str(&format!("?access_token={}", enc(&t)));
    }
    url
}

/// The body of a failed answer, or its status.
async fn check(resp: Response) -> Result<Response, String> {
    if resp.ok() {
        return Ok(resp);
    }
    let status = resp.status();
    let text = resp.text().await.unwrap_or_default();
    let text = text.trim();
    Err(if text.is_empty() || text.starts_with('<') {
        format!("HTTP {}", status)
    } else {
        text.to_string()
    })
}

async fn send(req: RequestBuilder) -> Result<Response, String> {
    check(auth(req).send().await.map_err(|e| e.to_string())?).await
}

async fn send_json<B: Serialize>(req: RequestBuilder, body: &B) -> Result<Response, String> {
    let req = auth(req).json(body).map_err(|e| e.to_string())?;
    check(req.send().await.map_err(|e| e.to_string())?).await
}

async fn json<T: DeserializeOwned>(resp: Response) -> Result<T, String> {
    resp.json().await.map_err(|e| e.to_string())
}

// ---- files ----

pub async fn list_dir(path: &str) -> Result<Vec<DirEntry>, String> {
    let p = encode_path(path);
    let url = if p.is_empty() {
        "/api/v1/dir".to_string()
    } else {
        format!("/api/v1/dir/{}", p)
    };
    json(send(Request::get(&url)).await?).await
}

pub async fn mkdir(path: &str) -> Result<(), String> {
    send(Request::put(&format!("/api/v1/dir/{}", encode_path(path))))
        .await
        .map(drop)
}

/// Delete a file or an empty directory.
pub async fn delete(path: &str) -> Result<(), String> {
    send(Request::delete(&format!(
        "/api/v1/files/{}",
        encode_path(path)
    )))
    .await
    .map(drop)
}

/// Delete `path`, and everything under it if it's a directory.
pub async fn delete_tree(path: String, is_dir: bool) -> Result<u64, String> {
    // Depth first, without recursion (async fns can't recurse cheaply).
    let mut removed = 0;
    let mut stack = vec![(path, is_dir, false)];
    while let Some((p, dir, listed)) = stack.pop() {
        if dir && !listed {
            stack.push((p.clone(), true, true));
            for e in list_dir(&p).await? {
                if e.kind == raidish_core::inode::ObjType::Volume {
                    return Err(format!(
                        "{}/{} is a volume: delete it under Volumes",
                        p, e.name
                    ));
                }
                stack.push((format!("{}/{}", p, e.name), e.is_dir(), false));
            }
            continue;
        }
        delete(&p).await?;
        removed += 1;
    }
    Ok(removed)
}

pub async fn rename(from: &str, to: &str) -> Result<(), String> {
    let req = RenameRequest {
        from: from.to_string(),
        to: to.to_string(),
    };
    send_json(Request::post("/api/v1/rename"), &req)
        .await
        .map(drop)
}

// ---- volumes ----

pub async fn volumes() -> Result<Vec<VolumeView>, String> {
    json(send(Request::get("/api/v1/volumes")).await?).await
}

pub async fn create_volume(name: &str, size_bytes: u64) -> Result<(), String> {
    // Checked here too, so a bad name is said before anything is sent.
    let name = raidish_types::types::DatasetName::user(name).map_err(|e| e.to_string())?;
    send_json(
        Request::post("/api/v1/volumes"),
        &CreateVolume { name, size_bytes },
    )
    .await
    .map(drop)
}

pub async fn delete_volume(name: &str) -> Result<(), String> {
    send(Request::delete(&format!("/api/v1/volumes/{}", enc(name))))
        .await
        .map(drop)
}

pub async fn set_props(name: &str, props: &DatasetProps) -> Result<(), String> {
    send_json(
        Request::put(&format!("/api/v1/volumes/{}/props", enc(name))),
        props,
    )
    .await
    .map(drop)
}

pub async fn snapshot(volume: &str, snap: &str) -> Result<(), String> {
    send_json(
        Request::post(&format!("/api/v1/volumes/{}/snapshots", enc(volume))),
        &NewName {
            name: snap.to_string(),
        },
    )
    .await
    .map(drop)
}

pub async fn destroy_snapshot(volume: &str, snap: &str) -> Result<(), String> {
    send(Request::delete(&format!(
        "/api/v1/volumes/{}/snapshots/{}",
        enc(volume),
        enc(snap)
    )))
    .await
    .map(drop)
}

pub async fn clone_snapshot(volume: &str, snap: &str, new: &str) -> Result<(), String> {
    send_json(
        Request::post(&format!(
            "/api/v1/volumes/{}/snapshots/{}/clone",
            enc(volume),
            enc(snap)
        )),
        &NewName {
            name: new.to_string(),
        },
    )
    .await
    .map(drop)
}

pub async fn rollback(volume: &str) -> Result<(), String> {
    send(Request::post(&format!(
        "/api/v1/volumes/{}/rollback",
        enc(volume)
    )))
    .await
    .map(drop)
}

/// `10G`, `512 MiB`, `1.5t`, `4096` → bytes (binary units).
pub fn parse_size(s: &str) -> Option<u64> {
    let s = s.trim();
    let split = s
        .find(|c: char| !(c.is_ascii_digit() || c == '.'))
        .unwrap_or(s.len());
    let (num, unit) = s.split_at(split);
    let n: f64 = num.parse().ok()?;
    let unit = unit.trim().to_ascii_lowercase();
    let unit = unit.trim_end_matches("ib").trim_end_matches('b');
    let mul: u64 = match unit {
        "" => 1,
        "k" => 1 << 10,
        "m" => 1 << 20,
        "g" => 1 << 30,
        "t" => 1 << 40,
        "p" => 1 << 50,
        _ => return None,
    };
    let v = n * mul as f64;
    (v >= 1.0 && v.is_finite()).then_some(v as u64)
}

// ---- VMs ----

pub use raidish_types::vm_def::NewVm;

/// A valid VM name (`raidish_types::types::VmName`).
pub fn valid_vm_name(name: &str) -> bool {
    raidish_types::types::VmName::try_from(name).is_ok()
}

pub async fn vm_definition(name: &str) -> Result<VmDef, String> {
    json(
        send(Request::get(&format!(
            "/api/v1/vms/{}/definition",
            enc(name)
        )))
        .await?,
    )
    .await
}

/// Store VM `name`'s definition. `create`: refuse if there is one;
/// `change`: refuse unless the stored one is that change. The server
/// checks it (disks are raidish volumes no other VM uses, nothing that
/// reaches the host) and says why it refuses.
pub async fn vm_put(
    name: &str,
    xml: &str,
    create: bool,
    change: Option<u64>,
) -> Result<VmDef, String> {
    let mut url = format!("/api/v1/vms/{}?create={}", enc(name), create);
    if let Some(c) = change {
        url.push_str(&format!("&change={}", c));
    }
    let req = auth(Request::put(&url))
        .header("Content-Type", "application/xml")
        .body(xml.to_string())
        .map_err(|e| e.to_string())?;
    json(check(req.send().await.map_err(|e| e.to_string())?).await?).await
}

pub async fn vms() -> Result<Vec<VmView>, String> {
    json(send(Request::get("/api/v1/vms")).await?).await
}

pub async fn vm_check(name: &str) -> Result<CheckView, String> {
    json(send(Request::get(&format!("/api/v1/vms/{}/check", enc(name)))).await?).await
}

pub async fn node_facts() -> Result<FactsView, String> {
    json(send(Request::get("/api/v1/nodes/facts")).await?).await
}

/// `start`, `stop`, `kill`, `restart`, `reboot` or `reset`.
pub async fn vm_action(name: &str, action: &str) -> Result<(), String> {
    send(Request::post(&format!(
        "/api/v1/vms/{}/{}",
        enc(name),
        action
    )))
    .await
    .map(drop)
}

pub async fn vm_move(name: &str, node: &str) -> Result<(), String> {
    send_json(
        Request::post(&format!("/api/v1/vms/{}/move", enc(name))),
        &MoveTo {
            node: node.to_string(),
            live: false,
        },
    )
    .await
    .map(drop)
}

/// Define a new VM (refusing to replace one).
pub async fn vm_create(name: &str, xml: &str) -> Result<(), String> {
    if !valid_vm_name(name) {
        return Err(format!(
            "bad VM name '{}' (letters, digits, '-', '_', '.')",
            name
        ));
    }
    vm_put(name, xml, true, None).await.map(drop)
}

/// Create a VM from the simple form: its disk volume, then its XML (and
/// the volume goes again if the XML can't be stored).
pub async fn vm_create_simple(new: NewVm, disk_bytes: u64) -> Result<(), String> {
    if !valid_vm_name(&new.name) {
        return Err(format!(
            "bad VM name '{}' (letters, digits, '-', '_', '.')",
            new.name
        ));
    }
    if let Some(n) = &new.network
        && !valid_vm_name(n)
    {
        return Err(format!("bad network name '{}'", n));
    }
    create_volume(&new.disk, disk_bytes)
        .await
        .map_err(|e| format!("creating volume {}: {}", new.disk, e))?;
    let xml = raidish_types::vm_def::template(&new);
    if let Err(e) = vm_create(&new.name, &xml).await {
        let _ = delete_volume(&new.disk).await;
        return Err(e);
    }
    Ok(())
}

/// Delete a VM's definition, then the volumes `disks`.
pub async fn vm_delete(name: &str, disks: &[String]) -> Result<(), String> {
    send(Request::delete(&format!("/api/v1/vms/{}", enc(name)))).await?;
    for d in disks {
        delete_volume(d)
            .await
            .map_err(|e| format!("deleting volume {}: {}", d, e))?;
    }
    Ok(())
}

/// The websocket URL of a VM's VNC console (RFB in binary frames).
pub fn console_url(name: &str) -> String {
    let loc = web_sys::window().map(|w| w.location());
    let https = loc
        .as_ref()
        .and_then(|l| l.protocol().ok())
        .is_some_and(|p| p == "https:");
    let host = loc.and_then(|l| l.host().ok()).unwrap_or_default();
    let mut url = format!(
        "{}://{}/api/v1/vms/{}/console",
        if https { "wss" } else { "ws" },
        host,
        enc(name)
    );
    if let Some(t) = token() {
        url.push_str(&format!("?access_token={}", enc(&t)));
    }
    url
}
