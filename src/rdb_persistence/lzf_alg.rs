pub fn decompress(byte_vec: &[u8]) -> String {

    let len = byte_vec.len();
    let mut ind = 0;
    let mut s = String::new();

    while ind < len {
        if byte_vec[ind] < 32 {
            
            let take_bytes_len = byte_vec[ind] as usize + 1;
            let take_bytes = &byte_vec[ind+1..ind+1+take_bytes_len];
            s.push_str(&String::from_utf8_lossy(&take_bytes));
            ind = ind + take_bytes_len;

        } else {
            let control_byte = byte_vec[ind] as usize;
            let mut length = control_byte>>5;
            if length >= 7 {
                ind += 1;
                length += byte_vec[ind] as usize;
            }
            length += 2;

            let offset = ((control_byte & 31) << 8) + byte_vec[ind+1] as usize + 1;
            
            let s_len = s.len();
            if length > offset {
                let s_to_copy = &s.clone()[s_len-offset..];
                let mut dividend = length/offset;
                let remainder = length%offset;
                while dividend > 0 {
                    s.push_str(s_to_copy);
                    dividend -= 1;
                }
                s.push_str(&s_to_copy[..remainder]);
                // break;
            } else {
                let take_back_bytes = &s.clone()[s_len-offset..(length+s_len-offset)];
                s.push_str(&take_back_bytes);
            }
            ind += 1;
        }
        ind += 1;
    }
    s
}