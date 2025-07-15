#![allow(unused)]

use std::{fmt::Display, sync::Arc};

use tokio::sync::Mutex;

use crate::{errors::RedisErrors, replication::slave_replica_info::{SlaveStore, SlaveReplicaInfo}};


#[derive(Debug)]
pub struct ReplicaInfo {
    role: String,                         // master by default
    slave_info: Option<SlaveReplicaInfo>,
    connected_slaves: u16,                // 1
    slaves: Vec<SlaveStore>,                   // e.g. [ip=172.18.0.2,port=6379,state=online,offset=28,lag=1]
    // master_failover_state: String,     // no-failover
    master_replid: String,                // 25fa1487c2a5c02271b570a0ed32eead89c694e0
    master_repl_offset: i64,              // -1
}

impl ReplicaInfo {
    pub fn new(
        role: String,
        slave_info: Option<SlaveReplicaInfo>,
        connected_slaves: u16,
        slaves: Vec<SlaveStore>,
        master_replid: String,
        master_repl_offset: i64
    ) -> Self {
        Self {
            role,
            slave_info,
            connected_slaves,
            slaves,
            master_replid,
            master_repl_offset
        }
    }
    pub fn role(&self) -> String {
        self.role.to_string()
    } 
    pub fn set_role(&mut self, role: String) {
        self.role = role;
    }
    pub fn slave_info(&mut self) -> Option<SlaveReplicaInfo> {
        self.slave_info.take()
    }
    pub fn set_slave_info(&mut self, slave_info: Option<SlaveReplicaInfo>) {
        self.slave_info = slave_info;
    }
    pub fn connected_slaves(&self) -> u16 {
        self.connected_slaves
    }
    pub fn set_connected_slaves(&mut self, v: u16) {
        self.connected_slaves = v;
    }
    pub fn add_slave(&mut self, ip: String, port: u16) {

        if self.role.eq("master") {
            let slave = SlaveStore::new(ip, port);
            self.slaves.push(slave);
            self.connected_slaves += 1;
        }
        
    }
    pub fn remove_slave(&mut self, ip: String, port: u16) {

        if self.role.eq("master") {
            let slave = SlaveStore::new(ip, port);
            self.slaves.retain(|s| *s != slave);
            self.connected_slaves -= 1;
        }
    }
    pub fn get_slaves(&self) -> Vec<SlaveStore> {
        if self.role.eq("master") {
            return self.slaves.to_vec();
        }
        Vec::new()
    }
}

impl Display for ReplicaInfo {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {

        let mut full_str = String::new();

        let role = format!("role:{}",self.role);
        full_str.push_str(&role);
        full_str.push_str("\r\n");

        if let Some(slave_info) = &self.slave_info {
            full_str.push_str(&slave_info.to_string());
        }

        let connect_slaves = format!("connected_slaves:{}",self.connected_slaves);
        full_str.push_str(&connect_slaves);
        full_str.push_str("\r\n");

        for (i, slave) in self.slaves.iter() .enumerate() {
            let s = format!("slave{i}:{}",slave.to_string());
            full_str.push_str(&s);
            full_str.push_str("\r\n");
        }

        let m_replid = format!("master_replid:{}",self.master_replid);
        full_str.push_str(&m_replid);
        full_str.push_str("\r\n");
        
        let m_repl_os = format!("master_repl_offset:{}",self.master_repl_offset);
        full_str.push_str(&m_repl_os);
        full_str.push_str("\r\n");

    
        write!(f, "${}\r\n{}\r\n",full_str.len(),full_str)
    }
}

type SharedReplicaInfoEnumT = Arc<Mutex<ReplicaInfo>>;

pub async fn init_replica_info(args: &Vec<String>) -> Result<SharedReplicaInfoEnumT, RedisErrors> {
    
    let mut replica_info = ReplicaInfo::new(
        "master".to_string(), 
        None,
        0, 
        vec![], 
        "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb".to_string(), 
        0
    );

    if args.len() > 4 {
        // slave config
        replica_info.set_role(String::from("slave"));
        let mut master_host = String::new();
        let mut master_port = 0;
        if args[3].eq("--replicaof") {
            let host_port = args[4].split_whitespace().collect::<Vec<_>>();
            master_host.push_str(host_port[0]);
            master_port = host_port[1].parse::<u16>()?;
        }
        let slave_replica_info = SlaveReplicaInfo::new(
            master_host, 
            master_port, 
            String::from("up"),
            -1
        );
        replica_info.set_slave_info(Some(slave_replica_info));
    } 
    Ok(Arc::new(Mutex::new(replica_info)))
}
