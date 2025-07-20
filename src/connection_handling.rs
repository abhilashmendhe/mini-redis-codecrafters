use std::{collections::{HashMap, VecDeque}, net::SocketAddr, sync::Arc, time::Duration};

use tokio::sync::{mpsc, Mutex};

use crate::{errors::RedisErrors, transactions::commands::CommandTransactions};

pub type ClientId = u16;
// pub type SenderChannelT = mpsc::UnboundedSender<(SocketAddr, Vec<u8>)>;
pub type RecvChannelT = mpsc::UnboundedReceiver<(SocketAddr, Vec<u8>)>;
// pub type SharedConnectionHashMapT = Arc<Mutex<HashMap<ClientId, (SenderChannelT, bool)>>>;

pub struct ConnectionStruct {
    pub tx_sender: mpsc::UnboundedSender<(SocketAddr, Vec<u8>)>,
    pub flag: bool,
    pub command_trans: VecDeque<CommandTransactions>,
}
pub type SharedConnectionHashMapT = Arc<Mutex<HashMap<ClientId, ConnectionStruct>>>;

impl ConnectionStruct {
    pub fn new(
        tx_sender: mpsc::UnboundedSender<(SocketAddr, Vec<u8>)>,
        flag: bool,
        command_trans: VecDeque<CommandTransactions>,
    ) -> Self {
        Self {
            tx_sender,
            flag,
            command_trans
        }
    }
    // pub fn set_commands_trans(&mut self, command_trans: VecDeque<String>) {
    //     self.command_trans = command_trans;
    // }
    pub fn mut_get_command_vec(&mut self) -> &mut VecDeque<CommandTransactions> {
        &mut self.command_trans
    }
    pub fn get_command_vec(&self) -> &VecDeque<CommandTransactions> {
        &self.command_trans
    }

}

pub async fn init_connection_channel() -> SharedConnectionHashMapT {
    Arc::new(Mutex::new(HashMap::new()))
}

pub async fn _read_connections_simul(conns: SharedConnectionHashMapT) {
    loop {
        tokio::time::sleep(Duration::from_millis(2000)).await;
        // let conns1 = Arc::clone(&conns);
        let conn_gaurd = conns.lock().await;
        
        for (k, _conn_struct) in conn_gaurd.iter() {
            println!("key: {}, flag: {}", k, _conn_struct.flag);
        }
        std::mem::drop(conn_gaurd);
        println!();
    }
}

pub async fn _periodic_ack_slave(
    conns: SharedConnectionHashMapT,
    sock_addr: SocketAddr
) -> Result<(), RedisErrors>{
    println!("Will start periodic ACK to replica from master");
    loop {
        tokio::time::sleep(Duration::from_millis(3000)).await;
        // let conns1 = Arc::clone(&conns);
        let conn_gaurd = conns.lock().await;
        
        for (conn_port, conn_struct) in conn_gaurd.iter() {
            // println!("key: {}, flag: {}", k, _v.1);
            let flag = conn_struct.flag;
            let tx_sender = conn_struct.tx_sender.clone();
            println!("Replica port: {}. isAlive: {}", conn_port, flag);
            if flag {
                println!("Sending REPLCONF GETACK to {}", sock_addr);
                tx_sender.send((sock_addr, b"*3\r\n$8\r\nREPLCONF\r\n$6\r\nGETACK\r\n$1\r\n*\r\n".to_vec()))?;

                tokio::time::sleep(Duration::from_millis(2000)).await;

                tx_sender.send((sock_addr, b"*1\r\n$4\r\nPING\r\n".to_vec()))?;
                tx_sender.send((sock_addr, b"*3\r\n$8\r\nREPLCONF\r\n$6\r\nGETACK\r\n$1\r\n*\r\n".to_vec()))?;

            }
        }
        std::mem::drop(conn_gaurd);
        println!();
    }
    // Ok(())
}