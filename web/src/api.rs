//! The node's HTTP API for the file manager: files (`/api/v1/files`,
//! `/api/v1/dir`, see `raidish::files_api`) and volumes (`/api/v1/volumes`,
//! passed on to the committer by whichever node answers).

use crate::session::token;
use gloo_net::http::{Request, RequestBuilder, Response};
use serde::{Deserialize, Serialize, de::DeserializeOwned};

/// A directory entry (`GET /api/v1/dir/<path>`).
#[derive(Deserialize, Clone, Debug, PartialEq)]
pub struct DirEntry {
    pub name: String,
    pub id: u64,
    /// `file`, `dir` or `volume`.
    pub kind: String,
    pub size: u64,
    #[serde(default)]
    pub mode: u16,
    #[serde(default)]
    pub mtime_ns: u64,
}

impl DirEntry {
    pub fn is_dir(&self) -> bool {
        self.kind == "dir"
    }
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Default)]
pub struct VolumeProps {
    pub nodes: Vec<String>,
    /// `nbd` or `nvme`; None: not auto-mounted.
    pub automount: Option<String>,
    pub enabled: bool,
    pub move_to: Option<String>,
}

#[derive(Deserialize, Clone, Debug, PartialEq)]
pub struct SnapshotInfo {
    /// `volume@snapshot`.
    pub name: String,
    pub txg: u64,
    pub guid: u64,
    pub created_ns: u64,
    pub unique_bytes: u64,
    pub clones: Vec<String>,
}

impl SnapshotInfo {
    /// The part after `@`.
    pub fn short(&self) -> &str {
        self.name.split_once('@').map_or(&self.name, |(_, s)| s)
    }
}

#[derive(Deserialize, Clone, Debug, PartialEq)]
pub struct Volume {
    pub name: String,
    pub size_bytes: u64,
    pub volblocksize: u32,
    pub props: VolumeProps,
    pub snapshots: Vec<SnapshotInfo>,
}

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
                if e.kind == "volume" {
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

#[derive(Serialize)]
struct Rename<'a> {
    from: &'a str,
    to: &'a str,
}

pub async fn rename(from: &str, to: &str) -> Result<(), String> {
    send_json(Request::post("/api/v1/rename"), &Rename { from, to })
        .await
        .map(drop)
}

// ---- volumes ----

pub async fn volumes() -> Result<Vec<Volume>, String> {
    json(send(Request::get("/api/v1/volumes")).await?).await
}

#[derive(Serialize)]
struct CreateVolume<'a> {
    name: &'a str,
    size_bytes: u64,
}

#[derive(Serialize)]
struct NewName<'a> {
    name: &'a str,
}

pub async fn create_volume(name: &str, size_bytes: u64) -> Result<(), String> {
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

pub async fn set_props(name: &str, props: &VolumeProps) -> Result<(), String> {
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
        &NewName { name: snap },
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
        &NewName { name: new },
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
