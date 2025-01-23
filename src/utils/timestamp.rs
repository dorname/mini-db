use std::time::{SystemTime, UNIX_EPOCH};

pub fn get_timestamp() -> u32 {
    let start = SystemTime::now();
    let since_the_epoch = start
        .duration_since(UNIX_EPOCH)
        .expect("Time went backwards");
    since_the_epoch.as_secs() as u32
}

pub fn get_timestamp_to_vec() -> Vec<u8> {
    let start = SystemTime::now();
    let since_the_epoch = start
        .duration_since(UNIX_EPOCH)
        .expect("Time went backwards");
    (since_the_epoch.as_secs() as u32).to_be_bytes().to_vec()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_timestamp() {
        println!("{:?}", get_timestamp().to_be_bytes());
    }

    #[test]
    fn test_timestamp_to_be() {
        println!("{:?}", get_timestamp_to_vec());
    }
}
