pub struct Measurement {
    min: f32,
    max: f32,
    sum: f32,
    pub count: usize,
}

impl Measurement {
    pub fn new(value: f32) -> Self {
        Self {
            min: value,
            max: value,
            sum: value,
            count: 1,
        }
    }

    pub fn add(&mut self, value: f32) {
        self.sum += value;
        self.count += 1;

        if value < self.min {
            self.min = value;
        }
        if value > self.max {
            self.max = value;
        }
    }

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

    pub fn avg(&self) -> f32 {
        self.sum / self.count as f32
    }
}

pub struct FinalMeasurement {
    pub min: f32,
    pub max: f32,
    pub avg: f32,
}
impl FinalMeasurement {
    pub fn new(min: f32, max: f32, avg: f32) -> Self {
        Self { min, max, avg }
    }
}

impl Into<FinalMeasurement> for Measurement {
    fn into(self) -> FinalMeasurement {
        FinalMeasurement::new(self.min, self.max, self.avg())
    }
}
