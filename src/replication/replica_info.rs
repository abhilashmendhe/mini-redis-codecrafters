use std::{fmt::Display, sync::Arc};

use tokio::sync::Mutex;

use crate::{errors::RedisErrors, replication::{master_replica_info::MasterReplicaInfo, slave_replica_info::SlaveReplicaInfo}};


pub enum ReplicaInfo {
    MASTER(MasterReplicaInfo),
    SLAVE(SlaveReplicaInfo)
}

impl Display for ReplicaInfo {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self {
            ReplicaInfo::MASTER(master_replica_info) => write!(f, "{}", master_replica_info),
            ReplicaInfo::SLAVE(slave_replica_info) => write!(f, "{}", slave_replica_info),
        }
    }
}

type SharedReplicaInfoEnumT = Arc<Mutex<ReplicaInfo>>;

pub async fn init_replica_info(args: &Vec<String>) -> Result<SharedReplicaInfoEnumT, RedisErrors> {
    
    let mut role = String::new();
    let connected_slaves = 0;
    let slave0 = None;
    let master_replid = String::from("8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb");
    let master_repl_offset = 0;
    if args.len() > 4 {
        // slave config
        let mut master_host = String::new();
        let mut master_port = 0;
        if args[3].eq("--replicaof") {
            role.push_str("slave");
            let host_port = args[4].split_whitespace().collect::<Vec<_>>();
            master_host.push_str(host_port[0]);
            master_port = host_port[1].parse::<u16>()?;
        }
        let rep_info = SlaveReplicaInfo::new(
                                String::from("slave"),
                                master_host,
                                master_port,
                                0,
                                slave0,
                                master_replid,
                                master_repl_offset
                            );
        Ok(Arc::new(Mutex::new(ReplicaInfo::SLAVE(rep_info))))

    } else {
        // master config
        role.push_str("master");
        let master_rep_info = MasterReplicaInfo::new(
                        role,
                        connected_slaves,
                        slave0,
                        master_replid,
                        master_repl_offset
                    );
        Ok(Arc::new(Mutex::new(ReplicaInfo::MASTER(master_rep_info))))
    }
}