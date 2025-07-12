use std::fmt::Display;

pub struct SlaveReplicaInfo {
    role: String,                       // slave
    master_host: String,                // host or ip
    master_port: u16,
    connected_slaves: u16,              // 1
    slave0: Option<String>,             // ip=172.18.0.2,port=6379,state=online,offset=28,lag=1
    // master_failover_state: String,      // no-failover
    master_replid: String,              // 25fa1487c2a5c02271b570a0ed32eead89c694e0
    // master_replid2: String,             // 0000000000000000000000000000000000000000
    master_repl_offset: i64,
    // second_repl_offset: i64,
    // repl_backlog_active: i64, 
    // repl_backlog_size:   i64,
    // repl_backlog_first_byte_offset: i64,
    // repl_backlog_histlen: i64
}

impl SlaveReplicaInfo {
    pub fn new(
        role: String,
        master_host: String,
        master_port: u16,
        connected_slaves: u16,
        slave0: Option<String>,
        master_replid: String,
        master_repl_offset: i64
    ) -> Self {
        SlaveReplicaInfo {
            role,
            master_host,
            master_port,
            connected_slaves,
            slave0,
            master_replid,
            master_repl_offset
        }
    }
    pub fn master_host(&self) -> String {
        self.master_host.to_string()
    }
    pub fn master_port(&self) -> u16 {
        self.master_port
    }
}

impl Display for SlaveReplicaInfo {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {

        let role = format!("role:{}",self.role);
        // let role_se = format!("${}\r\n{}\r\n",role.len(), role);
        let m_replid = format!("master_replid:{}",self.master_replid);
        // let m_replid_se = format!("${}\r\n{}\r\n",m_replid.len(),m_replid);

        let m_repl_os = format!("master_repl_offset:{}",self.master_repl_offset);
        // let m_repl_os_se = format!("${}\r\n{}\r\n",m_repl_os.len(),m_repl_os);

        let full_s = format!("{}\r\n{}\r\n{}",role,m_replid,m_repl_os);
        
        write!(f, "${}\r\n{}\r\n",full_s.len(),full_s)
    }
}