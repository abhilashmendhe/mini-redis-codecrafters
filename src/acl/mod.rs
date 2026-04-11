pub mod acl_whoami;
pub mod acl_getuser;

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

impl Acl {
    pub fn new() -> Self {
        Acl {
            flags: AclFlags { on: false, nopass: true },
            passwords: vec![]
        }
    }
}

impl std::fmt::Display for Acl {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        todo!()
    }
}