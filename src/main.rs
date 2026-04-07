use codecrafters_redis::{errors::RedisErrors, run_node};

#[tokio::main]
async fn main() -> Result<(), RedisErrors> {

    run_node::run_redis_node().await?;
    Ok(())
}