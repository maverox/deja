use rcgen::{CertificateParams, DistinguishedName, DnType, KeyPair, SanType};
use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use rustls::{ClientConfig, RootCertStore, ServerConfig};
use std::sync::Arc;
use x509_parser::prelude::*;

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

        // Parse the original certificate to extract its subject DN
        // This ensures server certificates have matching issuer fields
        let (_, parsed_cert) = X509Certificate::from_der(ca_cert_der.as_ref())
            .map_err(|e| format!("Failed to parse CA certificate: {:?}", e))?;

        // Build rcgen CertificateParams with the SAME subject as the original CA
        let mut params = CertificateParams::default();
        params.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained);

        // Copy subject from original certificate
        let mut dn = DistinguishedName::new();
        for rdn in parsed_cert.subject().iter() {
            for attr in rdn.iter() {
                if let Ok(value) = attr.as_str() {
                    match attr.attr_type().to_id_string().as_str() {
                        "2.5.4.6" => dn.push(DnType::CountryName, value), // C
                        "2.5.4.8" => dn.push(DnType::StateOrProvinceName, value), // ST
                        "2.5.4.10" => dn.push(DnType::OrganizationName, value), // O
                        "2.5.4.3" => dn.push(DnType::CommonName, value),  // CN
                        "2.5.4.7" => dn.push(DnType::LocalityName, value), // L
                        "2.5.4.11" => dn.push(DnType::OrganizationalUnitName, value), // OU
                        _ => {}                                           // Skip unknown OIDs
                    }
                }
            }
        }
        params.distinguished_name = dn;

        let ca_cert = params.self_signed(&ca_key)?;

        let mut root_cert_store = RootCertStore::empty();
        // Load system/public roots so we can talk to real upstreams
        root_cert_store.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());

        // Add our custom CA for upstream validation
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
        params.subject_alt_names = vec![SanType::DnsName(host.to_string().try_into()?)];

        let key_pair = KeyPair::generate()?;
        let cert = params.signed_by(&key_pair, &self.ca_cert, &self.ca_key)?;

        // Include both the server cert AND the original CA cert in the chain
        let cert_chain = vec![
            CertificateDer::from(cert.der().to_vec()),
            self.ca_cert_der.clone(),
        ];
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
