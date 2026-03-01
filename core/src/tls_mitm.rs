use rcgen::{CertificateParams, DistinguishedName, DnType, KeyPair, SanType};
use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use rustls::{ClientConfig, RootCertStore, ServerConfig};
use std::sync::Arc;

pub struct TlsMitmManager {
    ca_cert: rcgen::Certificate,
    ca_key: KeyPair,
    /// The original CA certificate DER bytes from the PEM file
    ca_cert_der: CertificateDer<'static>,
    root_cert_store: Arc<RootCertStore>,
}

impl TlsMitmManager {
    pub fn new(
        ca_cert_pem: &str,
        ca_key_pem: &str,
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let ca_key = KeyPair::from_pem(ca_key_pem)?;

        // Parse the original CA certificate DER bytes from PEM
        let mut reader = std::io::BufReader::new(ca_cert_pem.as_bytes());
        let ca_certs: Vec<CertificateDer<'static>> =
            rustls_pemfile::certs(&mut reader).collect::<Result<Vec<_>, _>>()?;
        let ca_cert_der = ca_certs
            .into_iter()
            .next()
            .ok_or("No CA certificate found in PEM")?;

        eprintln!(
            "[TLS-MITM] Loaded original CA cert, DER length: {}",
            ca_cert_der.len()
        );

        // Load the existing CA certificate using rcgen's CertificateParams::from_ca_cert_pem
        // Then create a self-signed Certificate that can be used as an issuer
        let ca_params = CertificateParams::from_ca_cert_pem(ca_cert_pem)
            .map_err(|e| format!("Failed to load CA cert params: {:?}", e))?;
        let ca_cert = ca_params
            .self_signed(&ca_key)
            .map_err(|e| format!("Failed to create CA certificate: {:?}", e))?;

        eprintln!(
            "[TLS-MITM] Loaded CA certificate using rcgen::CertificateParams::from_ca_cert_pem"
        );

        let mut root_cert_store = RootCertStore::empty();
        // Load system/public roots so we can talk to real upstreams
        root_cert_store.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());

        // Add our custom CA for upstream validation (use the original CA cert)
        root_cert_store.add(ca_cert_der.clone())?;

        Ok(Self {
            ca_cert,
            ca_key,
            ca_cert_der,
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

        let san = if let Ok(ip) = host.parse::<std::net::IpAddr>() {
            SanType::IpAddress(ip)
        } else {
            SanType::DnsName(host.to_string().try_into()?)
        };
        params.subject_alt_names = vec![san];

        let key_pair = KeyPair::generate()?;
        let cert = params.signed_by(&key_pair, &self.ca_cert, &self.ca_key)?;

        // DON'T include the CA cert in the chain - clients should already have it
        // in their trust store.
        let cert_chain = vec![CertificateDer::from(cert.der().to_vec())];
        let key_der = PrivateKeyDer::Pkcs8(key_pair.serialize_der().into());

        eprintln!(
            "[TLS-MITM] Generated server cert for host: {}, chain length: {}",
            host,
            cert_chain.len()
        );

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
