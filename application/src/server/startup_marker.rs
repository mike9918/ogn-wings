//! Durable admission markers live outside server-controlled files. Persist before
//! launching a process; clear only when readiness or an actual stop is observed.
use std::{
    io::Write,
    path::{Path, PathBuf},
};

pub struct Marker(PathBuf);
impl Marker {
    pub fn new(root: &Path, id: uuid::Uuid) -> Self {
        Self(root.join(".ogn-starting").join(id.to_string()))
    }
    pub fn exists(&self) -> bool {
        self.0.exists()
    }
    pub fn persist(&self) -> std::io::Result<()> {
        let parent = self.0.parent().unwrap();
        std::fs::create_dir_all(parent)?;
        let mut file = std::fs::File::create(&self.0)?;
        file.write_all(b"starting\n")?;
        file.sync_all()?;
        #[cfg(unix)]
        std::fs::File::open(parent)?.sync_all()?;
        Ok(())
    }
    pub fn clear(&self) -> std::io::Result<()> {
        match std::fs::remove_file(&self.0) {
            Ok(()) => {
                #[cfg(unix)]
                std::fs::File::open(self.0.parent().unwrap())?.sync_all()?;
                Ok(())
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(e),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn survives_reconstruction_and_clear_is_idempotent() {
        let root = std::env::temp_dir().join(format!(
            "ogn-marker-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let id = uuid::Uuid::from_u128(1);
        let m = Marker::new(&root, id);
        assert!(!m.exists());
        m.persist().unwrap();
        let recovered = Marker::new(&root, id);
        assert!(recovered.exists());
        assert!(!Marker::new(&root, uuid::Uuid::from_u128(2)).exists());
        recovered.clear().unwrap();
        recovered.clear().unwrap();
        assert!(!m.exists());
        std::fs::remove_dir_all(root).unwrap();
    }
}
