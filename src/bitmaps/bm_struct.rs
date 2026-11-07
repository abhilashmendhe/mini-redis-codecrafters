#[derive(Debug, Clone)]
pub struct BitMap {
    pub offsets: Vec<u8>,
    pub count: u64,
    pub max_ind: usize,
}

impl Default for BitMap {
    fn default() -> Self {
        Self {
            offsets: Default::default(),
            count: Default::default(),
            max_ind: Default::default(),
        }
    }
}

/*
    How am I storing the values?

    1. For now I've a fixed offsets array that stores all 0 values of u8 size.
    2. To set the bit, I am
        a. First fetching the offset number from the `offsets` array.
        b. Then setting the bit value of that offset number.

    e.g. offset = 134, and bit_value = 1

    1. Get the offset number position from the `offsets` array.
        a. Offset number position from the array: 134 // 8 = 16.
        b. The offset number is offsets[16].
    2. Now get the bit position,
        a. 134 % 8 = 6.
        b. So the bit position is 6 from 8 bit number.

    [offset num 0, offset num 1, offset num 2,..., offset num 16,... offset num 8192]

    set 6th bit of offset num (pos 16) = 0b00100000
*/

impl BitMap {
    pub fn new() -> Self {
        let offsets = vec![0 as u8; 8192];
        Self {
            offsets,
            count: 0,
            max_ind: 0,
        }
    }
    pub fn set_offset_bit(&mut self, offset: usize, bit_value: u8) -> u8 {
        // 1. get the offset position
        let offset_arr_pos = offset / 8;
        let offset_bit_pos = offset % 8;

        // 1.1 set max_ind
        self.max_ind = self.max_ind.max(offset_arr_pos);

        // 2. get offset number
        let offset_num = &mut self.offsets[offset_arr_pos];

        // 2.1 check the bit value of offset number which is the return value.
        let offset_bit_val = if *offset_num & 1 << offset_bit_pos == 0 {
            //
            if bit_value == 1 {
                *offset_num = *offset_num | (1 << offset_bit_pos);
            }
            0 as u8
        } else {
            if bit_value == 0 {
                *offset_num = *offset_num & !(1 << offset_bit_pos);
            }
            1
        };
        offset_bit_val
    }
    pub fn get_offset_bit(&self, offset_pos: usize) -> u8 {
        // 1. get the offset position
        let offset_arr_pos = offset_pos / 8;
        let offset_bit_pos = offset_pos % 8;
        // println!("{} - {}", offset_arr_pos, offset_bit_pos);
        // 2. get offset number
        let offset_num = &self.offsets[offset_arr_pos];
        // println!("{} - {:b}", offset_num, offset_num);
        (offset_num >> offset_bit_pos) & 1
    }
}
