pub struct L16Frame {
    pub left: Vec<i16>,
    pub right: Vec<i16>,
}

pub fn encodel16(data: &[f32]) -> L16Frame {
    let mut left = Vec::with_capacity(data.len() / 2);
    let mut right = Vec::with_capacity(data.len() / 2);

    for chunk in data.chunks_exact(2) {
        left.push((chunk[0].clamp(-1.0, 1.0) * i16::MAX as f32) as i16);
        right.push((chunk[1].clamp(-1.0, 1.0) * i16::MAX as f32) as i16);
    }

    L16Frame { left, right }
}
