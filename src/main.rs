use codecrafters_redis::{errors::RedisErrors, run_node};

#[tokio::main]
async fn main() -> Result<(), RedisErrors> {

    run_node::run_redis_node().await?;
    Ok(())
}


// fn main() {
//     let a = -1 as isize;
//     println!("a(isize): {}", a);
//     println!("a(usize): {}", a as usize);
//     let a = -5 as isize;
//     println!("a(isize): {}", a);
//     println!("a(usize): {}", a as usize);
    
// }