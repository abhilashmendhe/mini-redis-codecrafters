use crate::{basics::all_types::SharedRDBStructT, errors::RedisErrors};

pub async fn get_command_handler(
    get_str: &str,
    rdb: SharedRDBStructT,
) -> Result<String, RedisErrors> {
    let mut output = String::new();
    let rdb_gaurd = rdb.lock().await;
    match get_str {
        "dir" => {
            output.push_str("*2\r\n");

            output.push('$');
            output.push_str("3\r\ndir\r\n");

            output.push('$');
            output.push_str(&rdb_gaurd.dirpath().await.len().to_string());
            output.push_str("\r\n");
            output.push_str(&rdb_gaurd.dirpath().await);
            output.push_str("\r\n");
        }
        "rdbfilename" => {
            output.push_str("*2\r\n");

            output.push('$');
            output.push_str("10\r\ndbfilename\r\n");

            output.push('$');
            output.push_str(&rdb_gaurd.rdb_filepath().await.len().to_string());
            output.push_str("\r\n");
            output.push_str(&rdb_gaurd.rdb_filepath().await);
            output.push_str("\r\n");
        }
        "appendonly" => {
            output.push_str("*2\r\n");

            output.push('$');
            output.push_str("10\r\nappendonly\r\n");

            output.push('$');
            output.push_str(&rdb_gaurd.appendonly().await.len().to_string());
            output.push_str("\r\n");
            output.push_str(&rdb_gaurd.appendonly().await);
            output.push_str("\r\n");
        }
        "appenddirname" => {
            output.push_str("*2\r\n");

            output.push('$');
            output.push_str("13\r\nappenddirname\r\n");

            output.push('$');
            output.push_str(&rdb_gaurd.appenddirname().await.len().to_string());
            output.push_str("\r\n");
            output.push_str(&rdb_gaurd.appenddirname().await);
            output.push_str("\r\n");
        }
        "appendfilename" => {
            output.push_str("*2\r\n");

            output.push('$');
            output.push_str("14\r\nappendfilename\r\n");

            output.push('$');
            output.push_str(&rdb_gaurd.appendfilename().await.len().to_string());
            output.push_str("\r\n");
            output.push_str(&rdb_gaurd.appendfilename().await);
            output.push_str("\r\n");
        }
        "appendfsync" => {
            output.push_str("*2\r\n");

            output.push('$');
            output.push_str("11\r\nappendfsync\r\n");

            output.push('$');
            output.push_str(&rdb_gaurd.appendfsync().await.len().to_string());
            output.push_str("\r\n");
            output.push_str(&rdb_gaurd.appendfsync().await);
            output.push_str("\r\n");
        }
        _ => {
            output.push_str("-1\r\n");
        }
    };
    Ok(output)
}
