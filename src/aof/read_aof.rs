use crate::errors::RedisErrors;

pub fn read_aof(
    appendonly_file: String
) -> Result<(), RedisErrors> {
    let file = std::fs::read(appendonly_file)?;
    println!("{:?}",file);
    Ok(())
}