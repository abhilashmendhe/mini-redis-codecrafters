use crate::errors::RedisErrors;

pub fn read_aof(
    manifest_file_path: String
) -> Result<(), RedisErrors> {
    let file = std::fs::read_to_string(manifest_file_path)?;
    println!("{:?}",file);
    Ok(())
}