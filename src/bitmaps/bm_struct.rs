#[derive(Debug, Clone)]
pub struct BitMap {
    pub offsets: Vec<u8>,
    pub count: u64,
}

impl BitMap {
    pub fn new() -> Self {
        let offsets = vec![0 as u8; 8192];
        Self { offsets, count: 0 }
    }
    pub fn set_offset_bit(&mut self, offset: usize, bit_value: u8) {
        // 1. get the offset position
        let offset_arr_pos = offset / 8;
        let offset_bit_pos = offset % 8;

        let offset_num = &self.offsets[offset_arr_pos];
        
    }
}
