use std::collections::BTreeSet;

use crate::{basics::{all_types::SharedMapT, kv_ds::ValueStruct}, errors::RedisErrors, sorted_sets::SortedSetValues};

pub async fn zadd(
    key: &str,
    set_values: &Vec<String>,
    kv_ds: SharedMapT
) -> Result<String, RedisErrors> {

    let mut form = String::new();
    // println!("In zadd func");
    
    {
        let mut kv_gaurd = kv_ds.lock().await;
        if let Some(vs) = kv_gaurd.get_mut(key) {
            // println!("key : {} already there!", key);
            let res_form = match vs.mut_value() {
                crate::basics::kv_ds::Value::SORTED_SET(sorted_set) => {
                    insert_values_in_sorted_set(
                        &set_values, sorted_set
                    )?
                },
                _ => {
                    "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n".to_string()
                }
            };
            form.push_str(&res_form);
        } else {
            // insert fres key-value for sorted
            let mut sorted_set = BTreeSet::<SortedSetValues>::new();
            let res = insert_values_in_sorted_set(
                &set_values, &mut sorted_set
            )?;
            
            let vs = ValueStruct::new(crate::basics::kv_ds::Value::SORTED_SET(sorted_set), None,None);
            kv_gaurd.insert(key.to_string(), vs);

            form.push_str(&res);
        };
    }
    // println!("form: {form}");
    Ok(form)
}

fn insert_values_in_sorted_set(
    set_values: &Vec<String>,
    sorted_set: &mut BTreeSet<SortedSetValues>
) -> Result<String, RedisErrors> {

    let mut form = String::new();
    let mut ind = 0;
    let mut count = 0;
    while ind < set_values.len() {

        let v1 = set_values[ind].parse::<f64>()?;
        let v2 = &set_values[ind+1];
        let sorted_values = SortedSetValues { v1, v2: v2.to_string() };
        
        if sorted_set.insert(sorted_values) {
            count+=1;
        }
        ind += 2;
    }
    form.push_str(&format!(":{}\r\n",count));

    Ok(form)
}