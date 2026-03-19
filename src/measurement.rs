pub struct Measurement {
    min: i16,
    max: i16,
    sum: i64,
    pub count: usize,
}

impl Measurement {
    #[inline(always)]
    pub fn empty() -> Self {
        Self {
            min: i16::MAX,
            max: i16::MIN,
            sum: 0,
            count: 0,
        }
    }

    #[inline(always)]
    pub fn new(value: i16) -> Self {
        Self {
            min: value,
            max: value,
            sum: value as i64,
            count: 1,
        }
    }

    #[inline(always)]
    pub fn new_from(other: &Measurement) -> Self {
        Self {
            min: other.min,
            max: other.max,
            sum: other.sum,
            count: other.count,
        }
    }

    #[inline(always)]
    pub fn add(&mut self, value: i16) {
        self.sum += value as i64;
        self.count += 1;

        if value < self.min {
            self.min = value;
        }
        if value > self.max {
            self.max = value;
        }
    }

    #[inline(always)]
    pub fn merge(&mut self, other: &Measurement) {
        self.sum += other.sum;
        self.count += other.count;

        if other.min < self.min {
            self.min = other.min;
        }
        if other.max > self.max {
            self.max = other.max;
        }
    }

    #[inline(always)]
    pub fn avg(&self) -> f32 {
        // Calculate raw average
        let avg = (self.sum as f32) / (self.count as f32) / 10.0;

        // Round to one decimal place
        (avg * 10.0).round() / 10.0
    }
}

pub struct FinalMeasurement {
    pub min: f32,
    pub max: f32,
    pub avg: f32,
}

impl FinalMeasurement {
    #[inline(always)]
    pub fn new(min: f32, max: f32, avg: f32) -> Self {
        Self { min, max, avg }
    }
}

#[inline(always)]
fn int_to_float(value: i64) -> f32 {
    (value as f32) / 10.0
}

impl Into<FinalMeasurement> for Measurement {
    #[inline(always)]
    fn into(self) -> FinalMeasurement {
        FinalMeasurement::new(
            int_to_float(self.min as i64),
            int_to_float(self.max as i64),
            self.avg(), // avg now returns properly scaled f32
        )
    }
}
