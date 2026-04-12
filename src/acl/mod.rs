
pub mod acl_whoami;
pub mod acl_getuser;
pub mod acl_setuser;

#[derive(Debug)]
pub struct Acl {
    pub flags: AclFlags,
    pub passwords: Vec<String>
}

#[derive(Debug)]
pub struct AclFlags {
    pub on: bool, 
    pub nopass: bool 
}
impl AclFlags {
    pub fn set_nopass(&mut self, flag: bool) {
        self.nopass = flag;
    } 
}

impl Acl {
    pub fn new() -> Self {
        Acl {
            flags: AclFlags { on: false, nopass: true },
            passwords: vec![]
        }
    }
    pub fn mut_passwords(&mut self) -> &mut Vec<String> {
        &mut self.passwords
    }
    pub fn set_acl_flags_nopass(&mut self, f: bool) {
        self.flags.set_nopass(f);
    }
}

impl std::fmt::Display for Acl {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        
        // concat acl flags
        let mut acl_flag_out = String::new();
        let mut acl_flag_count = 0;
        let acl_flags = &self.flags;
        if acl_flags.on {
            acl_flag_out.push('$');
            acl_flag_out.push('2');
            acl_flag_out.push_str("\r\n");
            acl_flag_out.push_str("on");
            acl_flag_out.push_str("\r\n");
            acl_flag_count += 1;
        } 
        if acl_flags.nopass {
            acl_flag_out.push('$');
            acl_flag_out.push('6');
            acl_flag_out.push_str("\r\n");
            acl_flag_out.push_str("nopass");
            acl_flag_out.push_str("\r\n");
            acl_flag_count += 1;
        }

        // concat password fields
        let mut acl_pass_out = String::new();
        let mut acl_pass_count = 0;
        let passwords = &self.passwords;
        
        for pass in passwords {
            acl_pass_out.push('$');
            acl_pass_out.push_str(&pass.len().to_string());
            acl_pass_out.push_str("\r\n");
            acl_pass_out.push_str(pass);
            acl_pass_out.push_str("\r\n");
            acl_pass_count += 1;
        }

        write!(f, "*4\r\n$5\r\nflags\r\n*{}\r\n{}$9\r\npasswords\r\n*{}\r\n{}", acl_flag_count, acl_flag_out, acl_pass_count, acl_pass_out)
    }
}