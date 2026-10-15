tonic::include_proto!("pubsub");

impl From<uuid::Uuid> for Uuid {
    fn from(value: uuid::Uuid) -> Self {
        let (hi, lo) = value.as_u64_pair();

        Self { hi, lo }
    }
}

impl From<Uuid> for uuid::Uuid {
    fn from(value: Uuid) -> Self {
        match value {
            Uuid { hi, lo } => Self::from_u64_pair(hi, lo),
        }
    }
}
