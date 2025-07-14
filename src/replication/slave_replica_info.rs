use std::fmt::Display;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SlaveStore {
    ip: String,
    port: u16,
    // state: String,
    offset: i64,
}

impl SlaveStore {
    pub fn new(ip: String, port: u16) -> Self {
        Self {
            ip,
            port,
            offset: -1,
        }
    }
    pub fn ip(&self) -> String {
        self.ip.to_string()
    }
    pub fn port(&self) -> u16 {
        self.port
    }
    pub fn offset(&self) -> i64 {
        self.offset
    }
}

impl Display for SlaveStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "ip={},port={},offset={}",self.ip, self.port, self.offset)
    }
}

#[derive(Debug, Clone)]
pub struct SlaveReplicaInfo {
    master_host: String,
    master_port: u16,
    master_link_status: String,
    // master_last_io_seconds_ago:2
    // master_sync_in_progress:0
    // slave_read_repl_offset:u64,
    slave_repl_offset: i64
    // replica_full_sync_buffer_size:0
    // replica_full_sync_buffer_peak:0
    // slave_priority:100
    // slave_read_only:1
    // replica_announced:1
}

impl SlaveReplicaInfo {
    pub fn new(
        master_host: String,
        master_port: u16,
        master_link_status: String,
        slave_repl_offset: i64
    ) -> Self {
        SlaveReplicaInfo {
            master_host,
            master_port,
            master_link_status,
            slave_repl_offset
        }
    }
    pub fn master_host(&self) -> String {
        self.master_host.to_string()
    }
    pub fn master_port(&self) -> u16 {
        self.master_port
    }
    pub fn master_link_status(&self) -> String {
        self.master_link_status.to_string()
    }
    pub fn slave_repl_offset(&self) -> i64 {
        self.slave_repl_offset
    }
}

impl Display for SlaveReplicaInfo {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {

        let master_host = format!("master_host:{}",self.master_host);
        
        let master_port = format!("master_port:{}",self.master_port);
        
        let master_link_status = format!("master_link_status:{}",self.master_link_status);
        
        let slave_repl_offset_str = format!("slave_repl_offset:{}",self.slave_repl_offset);

        let full_s = format!("{}\r\n{}\r\n{}\r\n{}",master_host, master_port, master_link_status,slave_repl_offset_str);
        
        write!(f, "{}", full_s)
    }
}