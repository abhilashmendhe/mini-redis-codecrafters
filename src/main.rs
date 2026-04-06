use codecrafters_redis::{errors::RedisErrors, run_node};

#[tokio::main]
async fn main() -> Result<(), RedisErrors> {

    run_node::run_redis_node().await?;
    // let score = encode_coords(15.087269,37.502669);
    // println!("Encode score: {}", score);

    // let (lat, long) = decode_coords(score as u64);
    // println!("lat: {}, long: {}",lat,long);

    Ok(())
}