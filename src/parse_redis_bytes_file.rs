use crate::errors::RedisErrors;

pub async fn parse_multi_commands(buffer: &[u8]) ->Result<Vec<Vec<String>>, RedisErrors> {

    let mut ind = 0;
    let mut multi_command_vec = vec![];
    while ind < buffer.len() {

        if buffer[ind] == 42 {
            ind += 1;

            let start_int = ind;
            while buffer[ind] != 13 && buffer[ind+1] != 10 {
                ind += 1;
            }
            let end_int = ind;
            // println!("{}", String::from_utf8_lossy(&buffer[start_int..end_int]));
            let cmd_size = String::from_utf8_lossy(&buffer[start_int..end_int])
                                .parse::<u16>()?;
            // println!("Cmd size: {}", cmd_size);
            ind += 2;
            let mut cmsize = cmd_size + cmd_size;
            let mut cmd = vec![];
            while cmsize > 0 {

                let start_char = ind;
                while buffer[ind] != 13 && buffer[ind+1] != 10 {
                    ind += 1;
                }
                let end_char = ind;
                // println!("{}",String::from_utf8_lossy(&buffer[start_char..end_char]));
                cmd.push(String::from_utf8_lossy(&buffer[start_char..end_char]).to_string());
                cmsize-=1;
                ind += 2;   
            }         
            multi_command_vec.push(cmd);
            ind -= 1;
        }
        ind += 1;
    }
    Ok(multi_command_vec)
}

pub async fn parse_recv_bytes(buffer: &[u8]) -> Result<Vec<String>, RedisErrors>{
    
    // 1. parse the first line that tells how many args were passed
    let mut ind = 0;
    while ind < buffer.len() {
        if buffer[ind] == 13 && buffer[ind + 1] == 10 {
            break;
        }
        ind+=1;
    }

    let _num_args = String::from_utf8_lossy(&buffer[1..ind]);
    // println!("{}",_num_args);
    ind += 2;
    let mut prev_ind = ind;
    
    let mut cmds = vec![];

    // 2. parse the remaining content
    while ind < buffer.len() - 1 {
        if buffer[ind] == 13 && buffer[ind + 1] == 10 {
            let arg_size = String::from_utf8_lossy(&buffer[prev_ind+1..ind])
                            .parse::<usize>()?;
            let cmd_arg = String::from_utf8_lossy(&buffer[(ind+2)..(ind+2+arg_size)]);
            cmds.push(cmd_arg.to_string());
            // println!("val ->{}", String::from_utf8_lossy(&buffer[(ind+2)..(ind+2+arg_size)]));            
            ind = ind + 2 + arg_size ;
            prev_ind = ind + 2;
        }
        ind += 1;
    }
    Ok(cmds)
}

