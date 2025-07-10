use std::{fs, sync::Arc};
use tokio::sync::Mutex;

use crate::{errors::RedisErrors, rdb_persistence::{lzf_alg::decompress, rdb_persist::RDB}, redis_key_value_struct::{insert, SharedMapT, ValueStruct}};

/*
    0 = String Encoding
    1 = List Encoding
    2 = Set Encoding
    3 = Sorted Set Encoding
    4 = Hash Encoding
    9 = Zipmap Encoding
    10 = Ziplist Encoding
    11 = Intset Encoding
    12 = Sorted Set in Ziplist Encoding
    13 = Hashmap in Ziplist Encoding (Introduced in RDB version 4)
    14 = List in Quicklist encoding (Introduced in RDB version 7)
*/

pub async fn read_rdb_file(rdb: Arc<Mutex<RDB>>, kv_map: SharedMapT) -> Result<(), RedisErrors> {

    let rdb_gaurd = rdb.lock().await;
    let path = rdb_gaurd.rdb_filepath().await;
    // println!("{path}");
    let fbytes = fs::read(path)?;
    let f_len = fbytes.len();
    let mut i = 0;


    // loop until FE(254) to get the db number
    while i < f_len {
        if fbytes[i] == 254 && fbytes[i+1] < 16 && fbytes[i+2] == 251 {
            // println!("\nFound FE at position: {}", i);
            let _db_index = fbytes[i+1];
            i += 3;
            // println!("\nIn DBINDEX: {}",_db_index);
            // (Count) total keys (w/wo expiry)
            let all_kv_count = length_decoding(&mut i, &fbytes);
            
            // (Count) keys with expiry
            let _exp_kv_count = length_decoding(&mut i, &fbytes);

            // println!("all kvs: {}",all_kv_count);
            // println!("exp kvs: {}",_exp_kv_count);
            let kv_map_clone = Arc::clone(&kv_map);
            extract_key_pairs( &mut i, &fbytes, all_kv_count+1, kv_map_clone).await?;
        } 
        if fbytes[i] == 255 {
            i+=1;
            // println!("EndOfFile");
            let _checksum_bytes = &fbytes[i..];
            break;
        }
        i+=1;
    }
    Ok(())
}

async fn extract_key_pairs(
    ind: &mut usize, 
    fbytes: &Vec<u8>, 
    mut kv_count: usize,
    kv_map: SharedMapT
) -> Result<(), RedisErrors> {

    let mut kv_vec = vec![];

    while kv_count > 0 {
        if fbytes[*ind] == 0 { //String
            *ind += 1; 
            kv_count -= 1;
        }
        if fbytes[*ind] == 195 {
        
            let key = call_lzf_decoding(ind, &fbytes);
            // println!("Key: {}",key);
            kv_vec.push(key);

        } else if fbytes[*ind] == 252 {

            let xtime_ms = call_time_decode(ind, fbytes);
            // println!("{}",xtime_ms);
            kv_vec.push(format!("utx({})",xtime_ms));
        } else if fbytes[*ind] >= 192 && fbytes[*ind] < 195 {
            // Extract integers
            let num = call_extract_ints(ind, &fbytes);
            // println!("num: {}",num);
            kv_vec.push(format!("{}",num));
            // *ind += 1;
        } else if fbytes[*ind] >= 254 {
            *ind-=1;
            break;
        } else {
            let lenk = fbytes[*ind];
            *ind += 1;
            // println!("lenk:{}",lenk);
            // println!("{:?}",String::from_utf8_lossy(&fbytes[*ind..(*ind+lenk as usize)]));
            kv_vec.push(String::from_utf8_lossy(&fbytes[*ind..(*ind+lenk as usize)]).to_string());
            *ind += lenk as usize;
            
        }

        if kv_vec.len() >= 2 {
            if kv_vec[0].starts_with("utx") {
                if kv_vec.len() == 3 {
                    let utx = &kv_vec[0];
                    let pxat_str = &utx[4..utx.len()-1];
                    let pxat = pxat_str.parse::<u128>()?;
                    let key = &kv_vec[1];
                    let value = &kv_vec[2];
                    let value_struct = ValueStruct::new(value.to_string(),Some(pxat),Some(pxat));
                    // tokio::runtime::Runtime::new()?.block_on(
                    insert(key.to_string(), value_struct, kv_map.clone()).await;
                    // );
                    kv_vec.clear();
                }
            } else {
                let key = &kv_vec[0];
                let value = &kv_vec[1];
                let value_struct = ValueStruct::new(value.to_string(),None, None);
                // tokio::runtime::Runtime::new()?.block_on(
                insert(key.to_string(), value_struct, kv_map.clone()).await;
                // );
                kv_vec.clear();
            }
        }
    }
    
    Ok(())
}

fn call_extract_ints(ind: &mut usize, fbytes: &Vec<u8>) -> usize {
    let mut number = 0;

    if fbytes[*ind] == 192 {
        *ind += 1;
        number = fbytes[*ind] as usize;
        *ind += 1;

    } else if fbytes[*ind] == 193 {
        *ind += 1;
        // println!("{:?}", &fbytes[*ind..(*ind+2)]);
        number = compute_hex_arr_to_decimal(&fbytes[*ind..(*ind+2)]) as usize;
        *ind += 2;
    } else if fbytes[*ind] == 194 {
        *ind += 1;
        // println!("{:?}", &fbytes[*ind..(*ind+4)]);
        number = compute_hex_arr_to_decimal(&fbytes[*ind..(*ind+4)]) as usize;
        *ind += 4;
    }
    number
}

fn call_time_decode(ind: &mut usize, fbytes: &Vec<u8>) -> usize {
    // println!("There is a time");
    *ind += 1;
    let xtime_ms = compute_hex_arr_to_decimal(&fbytes[*ind..(*ind+8)]);
    // let d = UNIX_EPOCH + Duration::from_secs(xtime_ms as u64);
    *ind += 8;
    xtime_ms as usize
}

fn compute_hex_arr_to_decimal( bytes: &[u8]) -> u128 {
    let mut two_five_six = 1 as u128;
    let mut result= 0_u128;
    for bn in bytes {
        result += (*bn as u128) * two_five_six;
        two_five_six *= 256;
    }
    result
}

fn call_lzf_decoding(ind: &mut usize, fbytes: &Vec<u8>) -> String {

    *ind += 1;
    let clen = length_decoding( ind, &fbytes);
    let _ac_len = length_decoding(ind, &fbytes);
    let key_bytes = &fbytes[*ind..*ind+clen];

    // decompress from lzf
    let key = decompress(key_bytes);
    *ind+=clen;
    key
}

fn length_decoding(i:&mut usize, fbytes: &Vec<u8>) -> usize {

    // Check for 80(5 bytes) or 128 || 40(2 bytes) or 64 || < 64
    let mut key_count = 0;
    if fbytes[*i] == 128 {
        let vb = &fbytes[*i..*i+5];
        let mut pow16 = 16 * 16 * 16 * 16 * 16 * 16;
        let mut ind = 1;
        while ind < vb.len() {
            let r = vb[ind] as usize * pow16;
            key_count += r;
            pow16 /= 16 * 16;
            ind += 1;
        }
        *i+=5;
    } 
    else if fbytes[*i] > 63 && fbytes[*i] < 128 {
        let n1 = ((&fbytes[*i] - 64) as usize) << 8;
        let n2 = fbytes[*i+1] as usize;
        key_count = n1 + n2;
        *i+=2;
    } else {
        key_count = fbytes[*i] as usize;
        *i+=1;
    }
    key_count
}
