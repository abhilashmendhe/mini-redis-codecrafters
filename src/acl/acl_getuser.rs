pub async fn acl_get_user(
    _user_name: &str
) -> String {
    "*2\r\n$5\r\nflags\r\n*1\r\n$6\r\nnopass\r\n".to_string()
}