use std::fmt::Display;

pub struct MasterReplicaInfo {
    role: String,                       // master
    connected_slaves: u16,              // 1
    slave0: Option<String>,             // ip=172.18.0.2,port=6379,state=online,offset=28,lag=1
    // master_failover_state: String,      // no-failover
    // master_replid: String,              // 25fa1487c2a5c02271b570a0ed32eead89c694e0
    // master_replid2: String,             // 0000000000000000000000000000000000000000
    // master_repl_offset: i64,
    // second_repl_offset: i64,
    // repl_backlog_active: i64, 
    // repl_backlog_size:   i64,
    // repl_backlog_first_byte_offset: i64,
    // repl_backlog_histlen: i64
}

impl MasterReplicaInfo {
    pub fn new(
        role: String,
        connected_slaves: u16,
        slave0: Option<String>
    ) -> Self {
        MasterReplicaInfo {
            role,
            connected_slaves,
            slave0
        }
    }
}

impl Display for MasterReplicaInfo {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let role = format!("role:{}",self.role);
        let role_se = format!("${}\r\n{}\r\n",role.len(), role);
        write!(f, "{}",role_se)
    }
}