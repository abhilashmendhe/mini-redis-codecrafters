use std::{fmt::Display, sync::Arc};

use rand::RngCore;
use sha1::Digest;
use tokio::{sync::Mutex, time::Instant};

use crate::errors::RedisErrors;

#[derive(Debug)]
pub struct ListenerInfo {
    bind_ipv4: String,
    port: u16,
}

impl ListenerInfo {
    pub fn new(bind_ipv4: String, port: u16) -> Self {
        ListenerInfo { bind_ipv4, port }
    }
    pub fn bind_ipv4(&self) -> String {
        String::from(&self.bind_ipv4)
    }
    pub fn port(&self) -> u16 {
        self.port
    }
}

impl Display for ListenerInfo {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut s1 = String::from("listener0:name=tcp,bind=");
        s1.push_str(&self.bind_ipv4());
        s1.push(',');
        s1.push_str("port=");
        s1.push_str(&self.port().to_string());
        write!(f, "${}\r\n{}\r\n", s1.len(), s1)
    }
}

#[derive(Debug)]
pub struct SystemInfo {
    os: String,
    arch_bits: String,
    compiler: Option<String>,            
    compiler_version: Option<String>,   // "gcc" version by default or check "rustc" version
    multiplexing_api: Option<String>,    // "epoll" by default or if rust set "tokio"
}

impl SystemInfo {
    pub fn new() -> Result<Self, RedisErrors> {
        let mut os = String::from(std::env::consts::OS);
        let arch_bits = String::from(std::env::consts::ARCH);

        let mut compiler = None;
        let mut compiler_version = None;
        let mut multiplexing_api = None;

        if os.eq("linux") {
            // read /etc/os-release
            let os_info_content = std::fs::read_to_string("/etc/os-release")?;
            for cn in os_info_content.lines() {
                if cn.starts_with("NAME") || cn.starts_with("name") {
                    let name = cn.split("=").collect::<Vec<_>>();
                    os = format!("Linux {}", name[1]);
                } else if cn.contains("version") || cn.contains("VERSION") {
                    let version = cn.split("=").collect::<Vec<_>>();
                    os.push(' ');
                    os.push_str(version[1]);
                    break;
                }
            }
            // println!("{}",os_info_content);
            let mut check = false;
            // check if rustc exists
            let rustc_out = std::process::Command::new("rustc")
                                        .arg("--version")
                                        .output()?;
            
            if rustc_out.status.success() {
                let str_out = String::from_utf8_lossy(&rustc_out.stdout).to_string();
                let spl_out = str_out.split_whitespace().collect::<Vec<_>>();
                compiler = Some(String::from("rustc"));
                compiler_version = Some(spl_out[1].to_string());
                multiplexing_api = Some("tokio".to_string());
            } else {
                check = true;
            }

            if check {
                // check if gcc exists
                let gcc_out = std::process::Command::new("gcc")
                                        .arg("--version")
                                        .output()?;
                if gcc_out.status.success() {
                    let str_out = String::from_utf8_lossy(&gcc_out.stdout).to_string();
                    let spl_out = str_out.split_whitespace().collect::<Vec<_>>();
                    compiler = Some(String::from("gcc"));
                    if spl_out.len() > 2 {
                        compiler_version = Some(spl_out[3].to_string());
                    } else {
                        compiler_version = Some(spl_out[1].to_string());
                    }
                    multiplexing_api = Some("epoll".to_string());
                }
            }
        }
        Ok(SystemInfo { os, arch_bits, compiler, compiler_version, multiplexing_api })
    }
}

impl Display for SystemInfo {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut s = String::from("");
        let os_se = format!("os:{}",&self.os);
        s.push_str(format!("${}\r\n{}\r\n", os_se.len(), os_se).as_str());
        let arch_se = format!("arch_bits:{}",&self.arch_bits);
        s.push_str(format!("${}\r\n{}\r\n",arch_se.len(), arch_se).as_str());
        let mut count = 2;
        if !self.compiler.is_none() && !self.compiler_version.is_none() && !self.multiplexing_api.is_none() {
            let comp = self.compiler.as_ref().unwrap();
            let comp_ver = self.compiler_version.as_ref().unwrap();
            let comp_se = format!("{}:{}",comp, comp_ver);
            s.push_str(format!("${}\r\n{}\r\n", comp_se.len(), comp_se).as_str());
            let multi_se= format!("multiplexing_api:{}",self.multiplexing_api.as_ref().unwrap());
            s.push_str(format!("${}\r\n{}\r\n", multi_se.len(), multi_se).as_str());
            count = 4;
        }
        write!(f, "{}={}", count, s)
    }
}

#[derive(Debug)]
pub struct ServerTimeInfo {
    // Set the current epoch time in micro seconds and make it fixed until the server is closed.
    start_server_time: Instant, 
}

impl ServerTimeInfo {
    pub fn new() -> Self {
        let now_time_instant = Instant::now();

        ServerTimeInfo {
            start_server_time: now_time_instant,
        }
    }
    pub fn start_server_time(&self) -> Instant {
        self.start_server_time
    }
}

impl Display for ServerTimeInfo {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let current_time_usec = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_micros();
        /*
            server_time_usec:1752207149648553
            uptime_in_seconds:33
            uptime_in_days:0
         */
        
        let elapsed_secs = self.start_server_time.elapsed().as_secs();
        let stusec = format!("server_time:usec:{}",current_time_usec);
        let stusec_se = format!("${}\r\n{}\r\n",stusec.len(), stusec);

        
        let uptime_secs = format!("uptime_in_seconds:{}",elapsed_secs);
        let uptime_secs_se = format!("${}\r\n{}\r\n",uptime_secs.len(), uptime_secs);

        let uptime_days = format!("uptime_in_days:{}",elapsed_secs/86_400);
        let uptime_days_se = format!("${}\r\n{}\r\n",uptime_days.len(), uptime_days);

        write!(f, "{}{}{}",stusec_se,uptime_secs_se,uptime_days_se)
    }
}


#[derive(Debug)]
pub struct ServerInfo {
    pub redis_version: String,
    pub redis_mode: String, // Standalone by default, or ("sentinel", "cluster")
    pub redis_build_id: String, // "09cf28d8d5444152" by default
    pub system_info: SystemInfo,
    pub process_id: u32,
    pub run_id: String, // it is generated randomly but we will hardcode to "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb"
    pub tcp_port: u16, 
    pub server_time_info: ServerTimeInfo,
    pub listener_info: ListenerInfo
}

fn generate_run_id(uxtime: u128, pid: u32) -> String {
    let mut hasher = sha1::Sha1::new();

    // Random entropy
    let mut random_bytes = [0u8; 32];
    rand::thread_rng().fill_bytes(&mut random_bytes);
    hasher.update(&random_bytes);

    // Timestamp
    let now = uxtime.to_le_bytes();
    hasher.update(&now);

    // PID
    let pid = pid.to_le_bytes();
    hasher.update(&pid);

    let digest = hasher.finalize();

    // Convert to hex
    format!("{:x}",digest)
}

impl Display for ServerInfo {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {

        let redis_vers = format!("redis_version:{}",self.redis_version);
        let redis_vers_se = format!("${}\r\n{}\r\n",redis_vers.len(), redis_vers);

        let redis_build_id = format!("redis_build_id:{}",self.redis_build_id);
        let redis_build_id_se = format!("${}\r\n{}\r\n",redis_build_id.len(),redis_build_id);

        let redis_mode = format!("redis_mode:{}",self.redis_mode);
        let redis_mode_se = format!("${}\r\n{}\r\n",redis_mode.len(),redis_mode);

        let system_info_binding = self.system_info.to_string();
        let sys_info_vec = system_info_binding.split("=").collect::<Vec<_>>();
        let count = 11 + sys_info_vec[0].parse::<i32>().unwrap();
        let sys_info_se = sys_info_vec[1].to_string();


        let process_id = format!("process_id:{}",self.process_id);
        let process_id_se = format!("${}\r\n{}\r\n",process_id.len(),process_id);

        let run_id = format!("run_id:{}",self.run_id);
        let run_id_se = format!("${}\r\n{}\r\n",run_id.len(),run_id);

        let tcp_port = format!("tcp_port:{}",self.tcp_port);
        let tcp_port_se = format!("${}\r\n{}\r\n",tcp_port.len(),tcp_port);

        let stime_se = self.server_time_info.to_string();
        // write!(f, "*1\r\n{}", self.listener_info)

        let listener_se = self.listener_info.to_string();

        write!(f, 
            "*{}\r\n{}{}{}{}{}{}{}{}{}$0\r\n\r\n", 
            count,
            redis_vers_se, 
            redis_build_id_se, 
            redis_mode_se, 
            sys_info_se,
            process_id_se, 
            run_id_se, 
            tcp_port_se, 
            stime_se, 
            listener_se
        )
    }
}

type SharedServerInfoT = Arc<Mutex<ServerInfo>>;

pub async fn init_sever_info(args: &Vec<String>) -> Result<SharedServerInfoT, RedisErrors> {

    let redis_version = String::from("7.0.2");
    let redis_mode = String::from("Standalone");
    let redis_build_id = String::from("09cf28d8d5444152");
    let process_id = std::process::id();
    let system_info = SystemInfo::new()?;
    let mut run_id = String::from("8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb");
    let server_time_info = ServerTimeInfo::new();
    let mut tcp_port = 6379;
    let mut bind_ipv4 = String::from("127.0.0.1");
    if args.len() > 4 {
        if args[1].eq("--bind") {
            bind_ipv4 = String::from(&args[2]);
        }
        if args[3].eq("--port") {
            tcp_port = args[4].parse::<u16>()?;
            // if tcp_port != 6379 {
                // Generate different hash id which run_id
            run_id = generate_run_id(server_time_info.start_server_time().elapsed().as_micros(), process_id);
            // }
        }
    }
    if args.len() > 2 {
        if args[1].eq("--port") {
            tcp_port = args[2].parse::<u16>()?;
            // if tcp_port != 6379 {
                // Generate different hash id which run_id
            run_id = generate_run_id(server_time_info.start_server_time().elapsed().as_micros(), process_id);
            // }
        }
    }

    let listener_info = ListenerInfo::new(bind_ipv4, tcp_port);

    let server_info = ServerInfo { 
        redis_version, 
        redis_mode, 
        redis_build_id, 
        system_info, 
        process_id, 
        run_id, 
        tcp_port, 
        server_time_info, 
        listener_info 
    };
    Ok(Arc::new(Mutex::new(server_info)))
}
