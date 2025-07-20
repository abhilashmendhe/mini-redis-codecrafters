use crate::{basics::{all_types::SharedMapT, basic_ops::{get, set}}, errors::RedisErrors, transactions::transac_ops::incr_ops};

#[derive(Debug,Clone)]
#[allow(unused)]
pub enum CommandTransactions {
    Get {
        key: String
    },
    Set {
        key: String,
        value: String,
        px: Option<String>
    },
    Incr {
        key: String,
    },
    Multi,
}

pub async fn handle_transaction_commands(
    command_transactions: CommandTransactions,
    kv_map: SharedMapT
) -> Result<String, RedisErrors>{

    match command_transactions {
        CommandTransactions::Get { key } => {
            let form = get(key, kv_map).await;
            println!("{}", form);
            Ok(form)
        },
        CommandTransactions::Set { key, value, px } => {
            let form = set(key, value, px, kv_map).await?;
            println!("{}", form);
            Ok(form)
        },
        CommandTransactions::Incr { key } => {
            let form = incr_ops(key, kv_map).await?;
            println!("{}", form);
            Ok(form)
        },
        CommandTransactions::Multi => {
            Ok("+OK\r\n".to_string())
        },
    }
}