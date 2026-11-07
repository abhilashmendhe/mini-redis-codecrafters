use crate::bitmaps::bm_struct::BitMap;

#[derive(Debug, Clone)]
pub struct StringAndBitmap {
    value: String,
    bitmap: BitMap,
}

impl Default for StringAndBitmap {
    fn default() -> Self {
        Self {
            value: Default::default(),
            bitmap: BitMap::new(),
        }
    }
}

impl StringAndBitmap {
    pub fn set_value(&mut self, value: &str) {
        self.value = value.to_string();
    }
    pub fn value(&self) -> String {
        self.value.clone()
    }
    pub fn mut_value(&mut self) -> &mut str {
        &mut self.value
    }
    pub fn set_bitmap(&mut self, bitmap: BitMap) {
        self.bitmap = bitmap;
    }
    pub fn bitmap(&self) -> BitMap {
        self.bitmap.clone()
    }
    pub fn mut_bitmap(&mut self) -> &mut BitMap {
        &mut self.bitmap
    }
}
