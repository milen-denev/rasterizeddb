use rustls::{client::WantsClientCert, ClientConfig, ConfigBuilder, WantsVerifier};

/// Methods for configuring roots
///
/// This adds methods (gated by crate features) for easily configuring
/// TLS server roots a rustls ClientConfig will trust.
pub trait ConfigBuilderExt {

    /// This configures the webpki roots, which are Mozilla's set of
    /// trusted roots as packaged by webpki-roots.
    fn with_webpki_roots(self) -> ConfigBuilder<ClientConfig, WantsClientCert>;
}


impl ConfigBuilderExt for ConfigBuilder<ClientConfig, WantsVerifier> {
    fn with_webpki_roots(self) -> ConfigBuilder<ClientConfig, WantsClientCert> {
        let mut roots = rustls::RootCertStore::empty();
        roots.extend(
            webpki_roots::TLS_SERVER_ROOTS
                .iter()
                .cloned(),
        );
        self.with_root_certificates(roots)
    }
}