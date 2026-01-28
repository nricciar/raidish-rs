// FIXME: this code does not really work yet, but you can almost connect to it :)
// nvme_tcp.rs - NVMe-TCP Protocol Implementation
use crate::disk::BlockDevice;
use crate::fs::FileSystem;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Mutex;

// NVMe-TCP PDU Types (from NVMe-TCP specification)
const NVME_TCP_PDU_TYPE_IC_REQ: u8 = 0x00;
const NVME_TCP_PDU_TYPE_IC_RESP: u8 = 0x01;
const NVME_TCP_PDU_TYPE_H2C_TERM: u8 = 0x02;
const NVME_TCP_PDU_TYPE_C2H_TERM: u8 = 0x03;
const NVME_TCP_PDU_TYPE_CMD: u8 = 0x04;
const NVME_TCP_PDU_TYPE_RSP: u8 = 0x05;
const NVME_TCP_PDU_TYPE_H2C_DATA: u8 = 0x06;
const NVME_TCP_PDU_TYPE_C2H_DATA: u8 = 0x07;
const NVME_TCP_PDU_TYPE_R2T: u8 = 0x09;

// NVMe Admin & I/O Opcodes
const NVME_ADMIN_IDENTIFY: u8 = 0x06;
const NVME_ADMIN_GET_LOG_PAGE: u8 = 0x02;
const NVME_ADMIN_SET_FEATURES: u8 = 0x09;
const NVME_ADMIN_GET_FEATURES: u8 = 0x0A;
const NVME_ADMIN_CREATE_IO_SQ: u8 = 0x01;
const NVME_ADMIN_CREATE_IO_CQ: u8 = 0x05;
const NVME_ADMIN_KEEP_ALIVE: u8 = 0x18;
const NVME_IO_WRITE: u8 = 0x01;
const NVME_IO_READ: u8 = 0x02;
const NVME_IO_FLUSH: u8 = 0x00;

// Fabric/Discovery specific
const NVME_FABRICS_COMMAND: u8 = 0x7f;
const NVME_FABRICS_TYPE_PROPERTY_SET: u8 = 0x00;
const NVME_FABRICS_TYPE_CONNECT: u8 = 0x01;
const NVME_FABRICS_TYPE_PROPERTY_GET: u8 = 0x04;

// NVMe Status Codes
const NVME_SC_SUCCESS: u16 = 0x00;
const NVME_SC_INVALID_OPCODE: u16 = 0x01;
const NVME_SC_INVALID_FIELD: u16 = 0x02;
const NVME_SC_INTERNAL: u16 = 0xFF;

// Common header for all PDUs
#[repr(C, packed)]
#[derive(Debug, Clone, Copy)]
struct NvmeTcpPduHeader {
    pdu_type: u8,
    flags: u8,
    hlen: u8,
    pdo: u8,
    plen: u32,
}

// Initialize Connection Request
#[repr(C, packed)]
#[derive(Debug, Clone, Copy)]
struct NvmeTcpIcReq {
    header: NvmeTcpPduHeader,
    pfv: u16,
    hpda: u8,
    digest: u8,
    maxr2t: u32,
    rsvd: [u8; 112],
}

// Initialize Connection Response
#[repr(C, packed)]
#[derive(Debug, Clone, Copy)]
struct NvmeTcpIcResp {
    header: NvmeTcpPduHeader,
    pfv: u16,
    cpda: u8,
    digest: u8,
    maxh2cdata: u32,
    rsvd: [u8; 112],
}

// NVMe Command (simplified)
#[repr(C, packed)]
#[derive(Debug, Clone, Copy)]
struct NvmeTcpCmd {
    header: NvmeTcpPduHeader,
    cmd: NvmeCommand,
}

// NVMe Response
#[repr(C, packed)]
#[derive(Debug, Clone, Copy)]
struct NvmeTcpRsp {
    header: NvmeTcpPduHeader,
    cqe: NvmeCompletion,
}

// NVMe Command structure (64 bytes)
#[repr(C, packed)]
#[derive(Debug, Clone, Copy)]
struct NvmeCommand {
    opcode: u8,
    flags: u8,
    cid: u16,
    nsid: u32,
    cdw2: u32,
    cdw3: u32,
    mptr: u64,
    dptr: [u64; 2], // Data pointer
    cdw10: u32,
    cdw11: u32,
    cdw12: u32,
    cdw13: u32,
    cdw14: u32,
    cdw15: u32,
}

// NVMe Completion Queue Entry (16 bytes)
#[repr(C, packed)]
#[derive(Debug, Clone, Copy)]
struct NvmeCompletion {
    result: u32,
    rsvd: u32,
    sqhd: u16,
    sqid: u16,
    cid: u16,
    status: u16,
}

// H2C (Host to Controller) Data PDU
#[repr(C, packed)]
#[derive(Debug, Clone, Copy)]
struct NvmeTcpH2CData {
    header: NvmeTcpPduHeader,
    cccid: u16,
    ttag: u16,
    datao: u32,
    datal: u32,
    rsvd: u32,
}

// C2H (Controller to Host) Data PDU
#[repr(C, packed)]
#[derive(Debug, Clone, Copy)]
struct NvmeTcpC2HData {
    header: NvmeTcpPduHeader,
    cccid: u16,
    rsvd: u16,
    datao: u32,
    datal: u32,
    rsvd2: u32,
}

pub struct NvmeTcpServer<D>
where
    D: BlockDevice + Send + Sync + 'static,
{
    fs: Arc<Mutex<FileSystem<D>>>,
    device_name: String,
    port: u16,
}

struct ControllerState {
    cc: u32,         // Controller Configuration
    csts: u32,       // Controller Status
    connected: bool, // Track if we've completed connection
    reset_in_progress: bool,
}

impl ControllerState {
    fn new() -> Self {
        ControllerState {
            //cc: 0x460001, // Enabled (EN bit set) with reasonable defaults
            cc: 0,
            csts: 0, // Ready (RDY bit set) - ALWAYS READY
            connected: false,
            reset_in_progress: false,
        }
    }

    fn set_cc(&mut self, value: u32) {
        let old_en = self.cc & 0x1;
        let new_en = value & 0x1;

        println!(
            "    set_cc: old_en={}, new_en={}, connected={}",
            old_en, new_en, self.connected
        );

        self.cc = value;

        if !self.connected {
            // Pre-connection: normal enable/disable behavior
            if new_en == 1 {
                self.csts = 0x1; // Set RDY bit
            } else {
                self.csts = 0x0; // Clear RDY bit
            }
        } else {
            // Post-connection (NVMe-oF): controller stays ready
            // According to NVMe-oF spec, writes to CC.EN don't affect readiness
            // The controller is ready as long as the fabric connection is active
            println!("    NVMe-oF mode: Keeping CSTS.RDY=1 (controller stays ready over fabric)");
            self.csts = 0x1;

            // If the kernel writes CC.EN=0, it's probably checking reset behavior
            // But we don't actually disable - we just acknowledge the write
            if new_en == 0 {
                println!("    Note: CC.EN=0 written, but controller remains ready in NVMe-oF");
            }
        }

        println!("    Final CC=0x{:x}, CSTS=0x{:x}", self.cc, self.csts);
    }
}

impl<D: BlockDevice> NvmeTcpServer<D> {
    pub fn new(fs: FileSystem<D>, device_name: String, port: u16) -> Self
    where
        D: BlockDevice + Send + Sync + 'static,
    {
        NvmeTcpServer {
            fs: Arc::new(Mutex::new(fs)),
            device_name,
            port,
        }
    }

    pub async fn run(&self) -> std::io::Result<()>
    where
        D: BlockDevice + Send + Sync + 'static,
    {
        let listener = TcpListener::bind(format!("0.0.0.0:{}", self.port)).await?;
        println!("NVMe-TCP server listening on port {}", self.port);

        let conn_counter = Arc::new(std::sync::atomic::AtomicUsize::new(0));

        loop {
            let (socket, addr) = listener.accept().await?;
            let conn_id = conn_counter.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            println!("[Conn {}] New connection from {}", conn_id, addr);

            let fs = Arc::clone(&self.fs);
            let device_name = self.device_name.clone();

            tokio::spawn(async move {
                if let Err(e) = Self::handle_connection(socket, fs, device_name, conn_id).await {
                    eprintln!("[Conn {}] Connection error: {}", conn_id, e);
                }
                println!("[Conn {}] Connection closed", conn_id);
            });
        }
    }

    async fn handle_connection(
        mut socket: TcpStream,
        fs: Arc<Mutex<FileSystem<D>>>,
        device_name: String,
        conn_id: usize,
    ) -> std::io::Result<()>
    where
        D: BlockDevice + Send + Sync + 'static,
    {
        // Get device size
        let device_size = {
            let fs_lock = fs.lock().await;
            let inode_ref = fs_lock.file_index.files.get(&device_name).ok_or_else(|| {
                std::io::Error::new(
                    std::io::ErrorKind::NotFound,
                    format!("Device '{}' not found", device_name),
                )
            })?;

            let inode = fs_lock.read_inode(&inode_ref).await.unwrap();

            inode.inode_type.size_bytes()
        };

        // Connection initialization
        Self::perform_connection_init(&mut socket).await?;

        println!(
            "[Conn {}] NVMe-TCP connection initialized for device '{}' ({} bytes)",
            conn_id, device_name, device_size
        );

        // Controller state
        let mut ctrl_state = ControllerState::new();

        // Main command processing loop
        loop {
            // Add a timeout to detect hung connections
            let read_result = tokio::time::timeout(
                std::time::Duration::from_secs(30),
                Self::read_pdu_header(&mut socket),
            )
            .await;

            match read_result {
                Ok(Ok(header)) => {
                    let pdu_type = header.pdu_type;
                    println!("[Conn {}] PDU type 0x{:02x}", conn_id, pdu_type);

                    match pdu_type {
                        NVME_TCP_PDU_TYPE_CMD => {
                            Self::handle_command(
                                &mut socket,
                                header,
                                &fs,
                                &device_name,
                                device_size,
                                &mut ctrl_state,
                                conn_id,
                            )
                            .await?;
                        }
                        NVME_TCP_PDU_TYPE_H2C_DATA => {
                            Self::handle_h2c_data(&mut socket, header, &fs, &device_name).await?;
                        }
                        NVME_TCP_PDU_TYPE_H2C_TERM => {
                            println!("Received termination request from host");
                            break;
                        }
                        _ => {
                            eprintln!("Unsupported PDU type: 0x{:02x}", header.pdu_type);
                            // Try to skip the rest of the PDU to stay in sync
                            let skip_bytes = header.plen.saturating_sub(8) as usize;
                            if skip_bytes > 0 {
                                let mut skip_buf = vec![0u8; skip_bytes];
                                socket.read_exact(&mut skip_buf).await?;
                                println!("Skipped {} bytes to resync", skip_bytes);
                            }
                        }
                    }
                }
                Ok(Err(e)) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                    println!("[Conn {}] Connection closed by client", conn_id);
                    break;
                }
                Ok(Err(e)) => {
                    eprintln!("[Conn {}] Error reading PDU: {}", conn_id, e);
                    return Err(e);
                }
                Err(_) => {
                    println!(
                        "[Conn {}] Timeout waiting for PDU - client appears idle",
                        conn_id
                    );
                    break;
                }
            }
        }

        Ok(())
    }

    async fn perform_connection_init(socket: &mut TcpStream) -> std::io::Result<()> {
        // Read IC Request
        let mut ic_req_buf = vec![0u8; std::mem::size_of::<NvmeTcpIcReq>()];
        socket.read_exact(&mut ic_req_buf).await?;

        let ic_req = unsafe { std::ptr::read(ic_req_buf.as_ptr() as *const NvmeTcpIcReq) };

        println!(
            "Received IC Request: pfv={}, hpda={}",
            u16::from_le(ic_req.pfv),
            ic_req.hpda
        );

        // Send IC Response
        let ic_resp = NvmeTcpIcResp {
            header: NvmeTcpPduHeader {
                pdu_type: NVME_TCP_PDU_TYPE_IC_RESP,
                flags: 0,
                hlen: std::mem::size_of::<NvmeTcpIcResp>() as u8,
                pdo: 0,
                plen: (std::mem::size_of::<NvmeTcpIcResp>() as u32).to_le(),
            },
            pfv: 0u16.to_le(),                   // Protocol version 0
            cpda: 0,                             // Controller PDU Data Alignment
            digest: 0,                           // No digest support
            maxh2cdata: (128 * 1024u32).to_le(), // 128KB max transfer
            rsvd: [0; 112],
        };

        let ic_resp_bytes = unsafe {
            std::slice::from_raw_parts(
                &ic_resp as *const _ as *const u8,
                std::mem::size_of::<NvmeTcpIcResp>(),
            )
        };

        socket.write_all(ic_resp_bytes).await?;
        socket.flush().await?;

        println!("Sent IC Response");
        Ok(())
    }

    async fn read_pdu_header(socket: &mut TcpStream) -> std::io::Result<NvmeTcpPduHeader> {
        let mut header_buf = [0u8; 8];
        socket.read_exact(&mut header_buf).await?;

        let header = NvmeTcpPduHeader {
            pdu_type: header_buf[0],
            flags: header_buf[1],
            hlen: header_buf[2],
            pdo: header_buf[3],
            plen: u32::from_le_bytes([header_buf[4], header_buf[5], header_buf[6], header_buf[7]]),
        };

        // Copy fields to avoid packed struct reference issues
        let pdu_type = header.pdu_type;
        let flags = header.flags;
        let hlen = header.hlen;
        let pdo = header.pdo;
        let plen = header.plen;

        println!(
            "Read PDU header: type=0x{:02x}, flags=0x{:02x}, hlen={}, pdo={}, plen={}",
            pdu_type, flags, hlen, pdo, plen
        );

        Ok(header)
    }

    async fn handle_command(
        socket: &mut TcpStream,
        header: NvmeTcpPduHeader,
        fs: &Arc<Mutex<FileSystem<D>>>,
        device_name: &str,
        device_size: u64,
        ctrl_state: &mut ControllerState,
        conn_id: usize,
    ) -> std::io::Result<()>
    where
        D: BlockDevice + Send + Sync + 'static,
    {
        let plen = header.plen as usize;
        let hlen = header.hlen as usize;

        // 1. Read the 64-byte NVMe command first so we have the opcode
        let mut cmd_buf = [0u8; 64];
        socket.read_exact(&mut cmd_buf).await?;
        let cmd = unsafe { std::ptr::read(cmd_buf.as_ptr() as *const NvmeCommand) };

        let opcode = cmd.opcode;
        let cid = u16::from_le(cmd.cid);

        // 2. Consume any remaining PDU data (like the 1024-byte Connect data)
        // plen (total) - 8 (header) - 64 (command) = extra data
        let consumed_so_far = 8 + 64;
        let remaining = plen.saturating_sub(consumed_so_far);

        if remaining > 0 {
            let mut extra_data = vec![0u8; remaining];
            socket.read_exact(&mut extra_data).await?;
            println!(
                "[Conn {}] Consumed {} bytes of extra PDU data",
                conn_id, remaining
            );
        }

        let nsid = u32::from_le(cmd.nsid);

        println!(
            "[Conn {}] Received command: opcode=0x{:02x}, cid={}, nsid={}",
            conn_id, opcode, cid, nsid
        );

        match cmd.opcode {
            NVME_ADMIN_IDENTIFY => {
                Self::handle_identify(socket, cmd, device_size).await?;
            }
            NVME_IO_READ => {
                Self::handle_read(socket, cmd, fs, device_name).await?;
            }
            NVME_IO_WRITE => {
                Self::handle_write_cmd(socket, cmd).await?;
            }
            NVME_IO_FLUSH => {
                Self::send_completion(socket, cmd.cid, NVME_SC_SUCCESS, 0).await?;
            }
            NVME_FABRICS_COMMAND => {
                Self::handle_fabrics_command(socket, cmd, ctrl_state).await?;
            }
            NVME_ADMIN_GET_LOG_PAGE => {
                Self::handle_get_log_page(socket, cmd).await?;
            }
            NVME_ADMIN_SET_FEATURES | NVME_ADMIN_GET_FEATURES => {
                // Minimal implementation - just acknowledge
                Self::send_completion(socket, cmd.cid, NVME_SC_SUCCESS, 0).await?;
            }
            NVME_ADMIN_CREATE_IO_SQ | NVME_ADMIN_CREATE_IO_CQ | NVME_ADMIN_KEEP_ALIVE => {
                // Queue management - acknowledge for now
                Self::send_completion(socket, cmd.cid, NVME_SC_SUCCESS, 0).await?;
            }
            _ => {
                println!("Unsupported opcode: 0x{:02x}", cmd.opcode);
                Self::send_completion(socket, cmd.cid, NVME_SC_INVALID_OPCODE, 0).await?;
            }
        }

        Ok(())
    }

    async fn handle_identify(
        socket: &mut TcpStream,
        cmd: NvmeCommand,
        device_size: u64,
    ) -> std::io::Result<()> {
        let mut identify_data = vec![0u8; 4096];
        let cns = u32::from_le(cmd.cdw10) & 0xFF;

        match cns {
            0x01 => {
                // Identify Controller
                println!("    Identify Controller (CNS=0x01)");

                // VID (PCI Vendor ID) at offset 0-1
                identify_data[0..2].copy_from_slice(&0x1234u16.to_le_bytes());

                // SSVID (PCI Subsystem Vendor ID) at offset 2-3
                identify_data[2..4].copy_from_slice(&0x1234u16.to_le_bytes());

                // Serial Number (SN) at offset 4-23 (20 bytes, ASCII)
                identify_data[4..24].copy_from_slice(b"RAIDISHV10          ");

                // Model Number (MN) at offset 24-63 (40 bytes, ASCII)
                identify_data[24..64].copy_from_slice(b"RAIDISH NVMe-TCP Virtual Device         ");

                // Firmware Revision (FR) at offset 64-71 (8 bytes, ASCII)
                identify_data[64..72].copy_from_slice(b"1.0     ");

                // Recommended Arbitration Burst (RAB) at offset 72
                identify_data[72] = 0;

                // IEEE OUI at offset 73-75
                identify_data[73..76].copy_from_slice(&[0x00, 0x00, 0x00]);

                // Controller Multi-Path I/O (CMIC) at offset 76
                identify_data[76] = 0;

                // Maximum Data Transfer Size (MDTS) at offset 77
                identify_data[77] = 5; // 2^5 = 32 pages = 128KB

                // Controller ID (CNTLID) at offset 78-79
                identify_data[78..80].copy_from_slice(&1u16.to_le_bytes());

                // Version (VER) at offset 80-83 - NVMe 1.4
                identify_data[80..84].copy_from_slice(&0x00010400u32.to_le_bytes());

                // RTD3 Resume Latency (RTD3R) at offset 84-87
                identify_data[84..88].copy_from_slice(&0u32.to_le_bytes());

                // RTD3 Entry Latency (RTD3E) at offset 88-91
                identify_data[88..92].copy_from_slice(&0u32.to_le_bytes());

                // Optional Asynchronous Events Supported (OAES) at offset 92-95
                identify_data[92..96].copy_from_slice(&0x00000200u32.to_le_bytes());

                // Optional Admin Command Support (OACS) at offset 256-257
                identify_data[256..258].copy_from_slice(&0x0004u16.to_le_bytes());

                // Abort Command Limit (ACL) at offset 258
                identify_data[258] = 3; // Support up to 4 aborts

                // Asynchronous Event Request Limit (AERL) at offset 259
                identify_data[259] = 3; // Support up to 4 async events

                // Firmware Updates (FRMW) at offset 260
                identify_data[260] = 0x01; // First slot is read-only

                // Log Page Attributes (LPA) at offset 261
                identify_data[261] = 0;

                // Error Log Page Entries (ELPE) at offset 262
                identify_data[262] = 3; // 4 entries (0-based)

                // Number of Power States (NPSS) at offset 263
                identify_data[263] = 0; // 1 power state (0-based)

                // Keep Alive Support (KAS) at offset 320-321
                // Units are 100 milliseconds. 30 seconds = 300 * 100ms
                identify_data[320..322].copy_from_slice(&300u16.to_le_bytes());

                // Submission Queue Entry Size (SQES) at offset 512
                identify_data[512] = 0x66; // Min=6, Max=6 (64 bytes)

                // Completion Queue Entry Size (CQES) at offset 513
                identify_data[513] = 0x44; // Min=4, Max=4 (16 bytes)

                // Number of Namespaces (NN) at offset 516-519
                identify_data[516..520].copy_from_slice(&1u32.to_le_bytes());

                // Subsystem NQN (SUBNQN) at offset 768-1023 (256 bytes)
                let subnqn = b"nqn.2024-01.com.raidish:nvme:vdisk0";
                let subnqn_len = subnqn.len().min(256);
                identify_data[768..768 + subnqn_len].copy_from_slice(&subnqn[..subnqn_len]);

                // NVMe over Fabrics specific attributes
                // IOCCSZ at offset 2052-2055 (4 bytes)
                identify_data[2052..2056].copy_from_slice(&4u32.to_le_bytes());

                // IORCSZ at offset 2056-2059 (4 bytes)
                identify_data[2056..2060].copy_from_slice(&1u32.to_le_bytes());

                // ICDOFF at offset 2060-2061 (2 bytes)
                identify_data[2060..2062].copy_from_slice(&0u16.to_le_bytes());

                // FCATT at offset 2062 (1 byte)
                identify_data[2062] = 0x01;

                // MSDBD at offset 2063 (1 byte)
                identify_data[2063] = 0;

                println!("    -> CNTLID: 1");
                println!(
                    "    -> SUBNQN: {}",
                    String::from_utf8_lossy(&subnqn[..subnqn_len])
                );
                println!("    -> KAS: 300 (30 seconds)");
                println!("    -> IOCCSZ: 4, IORCSZ: 1");
            }
            0x00 | 0x11 => {
                // Identify Namespace (0x00) or Active NSID List (0x11)
                println!("    Identify Namespace (CNS=0x{:02x})", cns);

                if cns == 0x11 {
                    // Active NSID list - just return namespace 1
                    identify_data[0..4].copy_from_slice(&1u32.to_le_bytes());
                } else {
                    // Namespace 1 details
                    let block_size = 4096u64;
                    let nsze = device_size / block_size;

                    // NSZE (Namespace Size) at offset 0-7
                    identify_data[0..8].copy_from_slice(&nsze.to_le_bytes());

                    // NCAP (Namespace Capacity) at offset 8-15
                    identify_data[8..16].copy_from_slice(&nsze.to_le_bytes());

                    // NUSE (Namespace Utilization) at offset 16-23
                    identify_data[16..24].copy_from_slice(&nsze.to_le_bytes());

                    // NLBAF (Number of LBA Formats) at offset 25
                    identify_data[25] = 0; // 1 format (0-based)

                    // FLBAS (Formatted LBA Size) at offset 26
                    identify_data[26] = 0; // Format 0 is current

                    // LBA Format 0 at offset 128-131
                    // LBADS (LBA Data Size) = 12 (2^12 = 4096 bytes)
                    identify_data[128] = 12;
                }
            }
            _ => {
                println!("    Warning: Unhandled Identify CNS 0x{:02x}", cns);
            }
        }

        let hlen = std::mem::size_of::<NvmeTcpC2HData>();
        let c2h_header = NvmeTcpC2HData {
            header: NvmeTcpPduHeader {
                pdu_type: NVME_TCP_PDU_TYPE_C2H_DATA,
                flags: 0x01, // DATA_LAST
                hlen: hlen as u8,
                pdo: hlen as u8,
                plen: (hlen as u32 + 4096).to_le(),
            },
            cccid: cmd.cid,
            rsvd: 0,
            datao: 0,
            datal: 4096u32.to_le(),
            rsvd2: 0,
        };

        let c2h_bytes =
            unsafe { std::slice::from_raw_parts(&c2h_header as *const _ as *const u8, hlen) };

        socket.write_all(c2h_bytes).await?;
        socket.write_all(&identify_data).await?;
        socket.flush().await?;

        Self::send_completion(socket, u16::from_le(cmd.cid), NVME_SC_SUCCESS, 0u64).await?;

        Ok(())
    }

    async fn handle_read(
        socket: &mut TcpStream,
        cmd: NvmeCommand,
        fs: &Arc<Mutex<FileSystem<D>>>,
        device_name: &str,
    ) -> std::io::Result<()>
    where
        D: BlockDevice + Send + Sync + 'static,
    {
        let slba = u32::from_le(cmd.cdw10) as u64 | ((u32::from_le(cmd.cdw11) as u64) << 32);
        let nlb = (u32::from_le(cmd.cdw12) & 0xFFFF) + 1; // 0-based, so add 1

        let block_size = 4096u64;
        let offset = slba * block_size;
        let length = nlb as u64 * block_size;

        println!(
            "Read: slba={}, nlb={}, offset={}, length={}",
            slba, nlb, offset, length
        );

        let mut buffer = vec![0u8; length as usize];

        {
            let mut fs_lock = fs.lock().await;
            let bytes_read = fs_lock
                .block_read(device_name, offset, &mut buffer)
                .await
                .unwrap();

            if bytes_read < length as usize {
                buffer.truncate(bytes_read);
            }
        }

        // Send C2H Data PDU
        let c2h_header = NvmeTcpC2HData {
            header: NvmeTcpPduHeader {
                pdu_type: NVME_TCP_PDU_TYPE_C2H_DATA,
                flags: 0x01, // Last PDU
                hlen: std::mem::size_of::<NvmeTcpC2HData>() as u8,
                pdo: 0,
                plen: (std::mem::size_of::<NvmeTcpC2HData>() as u32 + buffer.len() as u32).to_le(),
            },
            cccid: cmd.cid,
            rsvd: 0,
            datao: 0,
            datal: (buffer.len() as u32).to_le(),
            rsvd2: 0,
        };

        let c2h_bytes = unsafe {
            std::slice::from_raw_parts(
                &c2h_header as *const _ as *const u8,
                std::mem::size_of::<NvmeTcpC2HData>(),
            )
        };

        socket.write_all(c2h_bytes).await?;
        socket.write_all(&buffer).await?;
        socket.flush().await?;

        // Send completion
        Self::send_completion(socket, cmd.cid, NVME_SC_SUCCESS, 0).await?;

        Ok(())
    }

    async fn handle_write_cmd(socket: &mut TcpStream, cmd: NvmeCommand) -> std::io::Result<()> {
        // For writes, we just acknowledge the command and wait for H2C data
        // The actual write will be handled in handle_h2c_data

        // We could send an R2T (Ready to Transfer) here, but most implementations
        // just wait for the H2C data to arrive

        println!("Write command queued, waiting for data...");
        Ok(())
    }

    async fn handle_h2c_data(
        socket: &mut TcpStream,
        header: NvmeTcpPduHeader,
        fs: &Arc<Mutex<FileSystem<D>>>,
        device_name: &str,
    ) -> std::io::Result<()>
    where
        D: BlockDevice + Send + Sync + 'static,
    {
        // Read H2C data header (rest after common header)
        let mut h2c_buf = [0u8; 16];
        socket.read_exact(&mut h2c_buf).await?;

        let cccid = u16::from_le_bytes([h2c_buf[0], h2c_buf[1]]);
        let datao = u32::from_le_bytes([h2c_buf[4], h2c_buf[5], h2c_buf[6], h2c_buf[7]]);
        let datal = u32::from_le_bytes([h2c_buf[8], h2c_buf[9], h2c_buf[10], h2c_buf[11]]);

        println!(
            "H2C Data: cccid={}, datao={}, datal={}",
            cccid, datao, datal
        );

        // Read the actual data
        let mut data = vec![0u8; datal as usize];
        socket.read_exact(&mut data).await?;

        // We need to track the write command context
        // For simplicity, assume offset is datao (this is a simplification)
        let block_size = 4096u64;
        let offset = (datao as u64 / block_size) * block_size;

        {
            let mut fs_lock = fs.lock().await;
            fs_lock
                .block_write(device_name, offset, &data)
                .await
                .unwrap();
        }

        // Send completion for the write
        Self::send_completion(socket, cccid, NVME_SC_SUCCESS, 0).await?;

        Ok(())
    }

    async fn handle_fabrics_command(
        socket: &mut TcpStream,
        cmd: NvmeCommand,
        ctrl_state: &mut ControllerState,
    ) -> std::io::Result<()> {
        // NVMe Fabrics Command structure:
        // Byte 0: opcode (0x7F)
        // Byte 1: flags
        // Bytes 2-3: cid
        // Byte 4: fctype ← THIS IS THE KEY!
        // Bytes 5-7: reserved (rest of what would be nsid in regular commands)
        // Bytes 8-11: cdw2
        // ...

        // For Fabrics commands, fctype is at byte 4, which is the LOW byte of the nsid field
        let fctype = (u32::from_le(cmd.nsid) & 0xFF) as u8;
        let cdw10 = u32::from_le(cmd.cdw10);
        let cdw11 = u32::from_le(cmd.cdw11);
        // Note: For Fabrics commands, the normal nsid field contains fctype in byte 4
        // We already extracted it above, so we don't use nsid for anything else

        println!(
            "    Fabrics: fctype=0x{:02x}, cdw10=0x{:08x}, cdw11=0x{:08x}",
            fctype, cdw10, cdw11
        );

        match fctype {
            0x00 => {
                // PROPERTY SET
                let offset = cdw11 & 0xFFFFFF;
                let attrib = (cdw11 >> 24) & 0xFF;
                let value = u64::from_le(cmd.dptr[0]);

                println!(
                    "    Property Set: offset=0x{:02x}, value=0x{:x}",
                    offset, value
                );

                match offset {
                    0x14 => {
                        // CC - Controller Configuration (writable)
                        ctrl_state.set_cc(value as u32);
                        Self::send_completion(socket, u16::from_le(cmd.cid), 0, 0).await?;
                    }
                    0x1C | 0x00 | 0x08 => {
                        // CSTS, CAP, VS - all read-only
                        println!("    ERROR: Write to read-only register 0x{:02x}", offset);
                        Self::send_completion(
                            socket,
                            u16::from_le(cmd.cid),
                            NVME_SC_INVALID_FIELD,
                            0,
                        )
                        .await?;
                    }
                    _ => {
                        println!(
                            "    WARNING: Write to unknown offset 0x{:02x}, accepting",
                            offset
                        );
                        Self::send_completion(socket, u16::from_le(cmd.cid), 0, 0).await?;
                    }
                }
            }

            0x04 => {
                // PROPERTY GET
                let offset = cdw11 & 0xFFFFFF;
                let attrib = (cdw11 >> 24) & 0xFF;

                let value: u64 = match offset {
                    0x00 => 0x00000001000203FFu64,  // CAP (64-bit)
                    0x08 => 0x00010400u64,          // VS (32-bit, 1.4.0)
                    0x14 => ctrl_state.cc as u64,   // CC (32-bit)
                    0x1C => ctrl_state.csts as u64, // CSTS (32-bit)
                    _ => {
                        println!(
                            "    Property Get: unknown offset 0x{:02x}, returning 0",
                            offset
                        );
                        0
                    }
                };

                println!(
                    "    Property Get: offset=0x{:02x}, returning 0x{:x}",
                    offset, value
                );
                Self::send_completion(socket, u16::from_le(cmd.cid), 0, value).await?;
            }

            0x01 => {
                // CONNECT
                let recfmt = (cdw10 & 0xFFFF) as u16;
                let qid = (cdw10 >> 16) as u16;

                println!("    Fabrics Connect: RECFMT={}, QID={}", recfmt, qid);

                // Validate RECFMT (must be 0 for NVMe-oF 1.x)
                if recfmt != 0 {
                    println!("    ERROR: Invalid RECFMT={}, expected 0", recfmt);
                    Self::send_completion(socket, u16::from_le(cmd.cid), NVME_SC_INVALID_FIELD, 0)
                        .await?;
                    return Ok(());
                }

                if qid == 0 {
                    // Admin queue connection
                    println!("    Admin queue (QID 0) connecting");
                    // After successful admin queue connection, controller should be enabled and ready
                    // This is different from PCIe NVMe where the host must explicitly enable via CC.EN
                    ctrl_state.cc = 0x00460001; // EN=1, plus reasonable defaults
                    ctrl_state.csts = 0x00000001; // RDY=1 (controller is ready)
                    ctrl_state.connected = true;

                    println!(
                        "    Controller now ENABLED and READY (CC=0x{:x}, CSTS=0x{:x})",
                        ctrl_state.cc, ctrl_state.csts
                    );

                    // Return Controller ID = 1 for admin queue
                    Self::send_completion(socket, u16::from_le(cmd.cid), 0, 0x0001).await?;
                } else {
                    // I/O queue connection
                    println!("    I/O queue (QID {}) connecting", qid);
                    // Return 0xFFFF for I/O queues
                    Self::send_completion(socket, u16::from_le(cmd.cid), 0, 0xFFFF).await?;
                }
            }

            0x05 => {
                // AUTHENTICATION SEND
                println!("    Authentication Send (not implemented)");
                Self::send_completion(socket, u16::from_le(cmd.cid), NVME_SC_INVALID_OPCODE, 0)
                    .await?;
            }

            0x06 => {
                // AUTHENTICATION RECV
                println!("    Authentication Recv (not implemented)");
                Self::send_completion(socket, u16::from_le(cmd.cid), NVME_SC_INVALID_OPCODE, 0)
                    .await?;
            }

            _ => {
                println!("    ERROR: Unknown Fabrics fctype=0x{:02x}", fctype);
                Self::send_completion(socket, u16::from_le(cmd.cid), NVME_SC_INVALID_OPCODE, 0)
                    .await?;
            }
        }

        Ok(())
    }

    async fn handle_get_log_page(socket: &mut TcpStream, cmd: NvmeCommand) -> std::io::Result<()> {
        let lid = (u32::from_le(cmd.cdw10) & 0xFF) as u8;
        let numdl = (u32::from_le(cmd.cdw10) >> 16) & 0xFFFF;
        let numdu = u32::from_le(cmd.cdw11) & 0xFFFF;
        let len = (((numdu as u32) << 16) | numdl) + 1;
        let byte_len = (len * 4) as usize;

        println!("Get Log Page: lid=0x{:02x}, len={} bytes", lid, byte_len);

        // Return empty/zeroed log pages for now
        let log_data = vec![0u8; byte_len];

        // Send C2H Data PDU
        let c2h_header = NvmeTcpC2HData {
            header: NvmeTcpPduHeader {
                pdu_type: NVME_TCP_PDU_TYPE_C2H_DATA,
                flags: 0x01, // Last PDU
                hlen: std::mem::size_of::<NvmeTcpC2HData>() as u8,
                pdo: 0,
                plen: (std::mem::size_of::<NvmeTcpC2HData>() as u32 + log_data.len() as u32)
                    .to_le(),
            },
            cccid: cmd.cid,
            rsvd: 0,
            datao: 0,
            datal: (log_data.len() as u32).to_le(),
            rsvd2: 0,
        };

        let c2h_bytes = unsafe {
            std::slice::from_raw_parts(
                &c2h_header as *const _ as *const u8,
                std::mem::size_of::<NvmeTcpC2HData>(),
            )
        };

        socket.write_all(c2h_bytes).await?;
        socket.write_all(&log_data).await?;
        socket.flush().await?;

        // Send completion
        Self::send_completion(socket, cmd.cid, NVME_SC_SUCCESS, 0).await?;

        Ok(())
    }

    async fn send_completion(
        socket: &mut TcpStream,
        cid: u16,
        status: u16,
        result: u64,
    ) -> std::io::Result<()> {
        let mut rsp = NvmeTcpRsp {
            header: NvmeTcpPduHeader {
                pdu_type: NVME_TCP_PDU_TYPE_RSP,
                flags: 0,
                hlen: 24, // Standard size for Response PDU header
                pdo: 0,
                plen: 24u32.to_le(), // Total PDU length is exactly 24 bytes
            },
            cqe: NvmeCompletion {
                result: (result as u32).to_le(),
                rsvd: ((result >> 32) as u32).to_le(),
                sqhd: 0,
                sqid: 0,
                cid: cid.to_le(),
                status: (status << 1).to_le(),
            },
        };

        let rsp_bytes = unsafe {
            std::slice::from_raw_parts(
                &rsp as *const _ as *const u8,
                24, // Ensure we send exactly 24 bytes
            )
        };

        socket.write_all(rsp_bytes).await?;
        socket.flush().await?;
        println!(
            "    Sent completion: CID={}, Status=0x{:x}, Result=0x{:x}",
            cid, status, result
        );
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_structure_sizes() {
        assert_eq!(std::mem::size_of::<NvmeTcpPduHeader>(), 8);
        assert_eq!(std::mem::size_of::<NvmeCommand>(), 64);
        assert_eq!(std::mem::size_of::<NvmeCompletion>(), 16);
    }
}
