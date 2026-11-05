use std::path::Path;

use pubsub_proto::pubsub as proto;
use uuid::Uuid;

use crate::error::BuilderError;

pub async fn load_or_create_uuid(state_dir: &Path) -> Result<Uuid, BuilderError> {
    let path = state_dir.join("uuid");

    match tokio::fs::read_to_string(&path).await {
        Ok(contents) => Ok(Uuid::parse_str(contents.trim())?),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
            let uuid = Uuid::new_v4();
            tokio::fs::create_dir_all(state_dir).await?;
            tokio::fs::write(&path, uuid.to_string()).await?;
            log::info!("generated new builder uuid {}", uuid);
            Ok(uuid)
        }
        Err(err) => Err(err.into()),
    }
}

pub fn current_system() -> String {
    let os = match std::env::consts::OS {
        "macos" => "darwin",
        os => os,
    };

    format!("{}-{}", std::env::consts::ARCH, os)
}

pub fn builder_information(
    uuid: Uuid,
    system: String,
    store_uri: String,
) -> Result<proto::common::BuilderInformation, BuilderError> {
    let sysinfo = sysinfo::System::new_with_specifics(
        sysinfo::RefreshKind::nothing()
            .with_memory(sysinfo::MemoryRefreshKind::nothing().with_ram()),
    )?;

    Ok(proto::common::BuilderInformation {
        uuid: Some(uuid.into()),
        system,
        smt_core_count: std::thread::available_parallelism()?.get() as i32,
        total_memory: sysinfo.total_memory(),
        store_uri,
    })
}
