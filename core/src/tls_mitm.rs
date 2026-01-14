use rcgen::{CertificateParams, DistinguishedName, DnType, KeyPair, SanType};
use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use rustls::{ClientConfig, RootCertStore, ServerConfig};
use std::sync::Arc;

pub struct TlsMitmManager {
    ca_cert: rcgen::Certificate,
    ca_key: KeyPair,
    root_cert_store: Arc<RootCertStore>,
}

impl TlsMitmManager {
    pub fn new(
        ca_cert_pem: &str,
        ca_key_pem: &str,
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let ca_key = KeyPair::from_pem(ca_key_pem)?;

        // Reconstruct CA certificate object from key (for signing)
        // Note: Real MitM would want to match the exact identity of the provided CA cert.
        // For MVP, we use a default identity.
        let mut params = CertificateParams::default();
        params.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
        params.distinguished_name = DistinguishedName::new();
        params
            .distinguished_name
            .push(DnType::CommonName, "Deja Proxy CA");
        let ca_cert = params.self_signed(&ca_key)?;

        let mut root_cert_store = RootCertStore::empty();
        let mut reader = std::io::BufReader::new(ca_cert_pem.as_bytes());
        for cert in rustls_pemfile::certs(&mut reader) {
            root_cert_store.add(cert?)?;
        }

        Ok(Self {
            ca_cert,
            ca_key,
            root_cert_store: Arc::new(root_cert_store),
        })
    }

    pub fn generate_server_config(
        &self,
        host: &str,
    ) -> Result<Arc<ServerConfig>, Box<dyn std::error::Error + Send + Sync>> {
        let mut params = CertificateParams::default();
        params.distinguished_name = DistinguishedName::new();
        params.distinguished_name.push(DnType::CommonName, host);
        params.subject_alt_names = vec![SanType::DnsName(host.to_string().try_into()?)];

        let key_pair = KeyPair::generate()?;
        let cert = params.signed_by(&key_pair, &self.ca_cert, &self.ca_key)?;

        let cert_chain = vec![CertificateDer::from(cert.der().to_vec())];
        let key_der = PrivateKeyDer::Pkcs8(key_pair.serialize_der().into());

        let config = ServerConfig::builder()
            .with_no_client_auth()
            .with_single_cert(cert_chain, key_der)?;

        Ok(Arc::new(config))
    }

    pub fn client_config(&self) -> Arc<ClientConfig> {
        let mut config = ClientConfig::builder()
            .with_root_certificates(self.root_cert_store.clone())
            .with_no_client_auth();

        config.alpn_protocols = vec![b"http/1.1".to_vec()];
        Arc::new(config)
    }
}
