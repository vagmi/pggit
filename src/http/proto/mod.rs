//! In-process implementation of the git smart HTTP protocol.
//!
//! Replaces the subprocess `git http-backend` flow with a direct
//! gix-packetline + libgit2 path. Each handler buffers the request body,
//! runs the protocol synchronously inside a `spawn_blocking` task, and
//! streams response bytes back through a `tokio::io::duplex` so the outgoing
//! pack flows out as it's produced.

pub(crate) mod advert;
pub(crate) mod receive_pack;
pub(crate) mod sideband;
pub(crate) mod upload_pack;

/// The two services we serve. Used by the advertisement and to reject the
/// wrong service on POST endpoints.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Service {
    UploadPack,
    ReceivePack,
}

impl Service {
    pub(crate) fn name(self) -> &'static str {
        match self {
            Service::UploadPack => "git-upload-pack",
            Service::ReceivePack => "git-receive-pack",
        }
    }
}
