pub mod pubsub {
    pub mod server {
        tonic::include_proto!("pubsub.server");
    }

    pub mod client {
        tonic::include_proto!("pubsub.client");
    }

    pub mod common {
        tonic::include_proto!("pubsub.common");
    }
}

use pubsub::*;

impl From<uuid::Uuid> for common::Uuid {
    fn from(value: uuid::Uuid) -> Self {
        let (hi, lo) = value.as_u64_pair();

        Self { hi, lo }
    }
}

impl From<common::Uuid> for uuid::Uuid {
    fn from(value: common::Uuid) -> Self {
        match value {
            common::Uuid { hi, lo } => Self::from_u64_pair(hi, lo),
        }
    }
}
