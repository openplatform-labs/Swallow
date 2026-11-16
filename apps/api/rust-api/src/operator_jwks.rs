//! Fetch a configured, TLS-authenticated public key set for the pinned issuer.
use std::time::Duration;

#[derive(Clone)]
pub(crate) struct RemoteJwks {
    url: reqwest::Url,
    client: reqwest::Client,
    policy: serde_json::Value,
}
impl RemoteJwks {
    pub fn new(url:&str, config:&str, ca:Option<&str>) -> Result<Self,()> {
        let url=reqwest::Url::parse(url).map_err(|_|())?;
        if url.scheme()!="https" || url.host_str().is_none() || !url.username().is_empty() || url.password().is_some() || url.fragment().is_some() {return Err(());}
        let mut policy:serde_json::Value=serde_json::from_str(config).map_err(|_|())?;
        let object=policy.as_object_mut().ok_or(())?;
        object.remove("keys");object.remove("jwks");
        let mut builder=reqwest::Client::builder().no_proxy().redirect(reqwest::redirect::Policy::none()).timeout(Duration::from_secs(10));
        if let Some(ca)=ca {
            let certs=reqwest::Certificate::from_pem_bundle(ca.as_bytes()).map_err(|_|())?;
            if certs.is_empty() {return Err(());}
            for cert in certs {builder=builder.add_root_certificate(cert);}
        }
        Ok(Self{url,client:builder.build().map_err(|_|())?,policy})
    }
    pub async fn fetch(&self)->Result<String,()> {
        let mut response=self.client.get(self.url.clone()).send().await.map_err(|_|())?;
        if response.status()!=reqwest::StatusCode::OK || response.content_length().is_some_and(|n| n>16384) {return Err(());}
        let mut bytes=Vec::new();
        while let Some(chunk)=response.chunk().await.map_err(|_|())? {
            if bytes.len().saturating_add(chunk.len())>16384 {return Err(());}
            bytes.extend_from_slice(&chunk);
        }
        let jwks:serde_json::Value=serde_json::from_slice(&bytes).map_err(|_|())?;
        let mut policy=self.policy.clone();policy.as_object_mut().ok_or(())?.insert("jwks".into(),jwks);
        let value=serde_json::to_string(&policy).map_err(|_|())?;
        if value.len()>16384 {return Err(());}
        Ok(value)
    }
}

#[cfg(test)]
mod tests {
    use super::RemoteJwks;
    const CONFIG:&str=r#"{"issuer":"https://issuer.test","audience":"api","keys":[]}"#;
    #[test]
    fn rejects_unsafe_or_incomplete_urls_and_bad_ca() {
        for url in ["http://localhost/keys","https://user:pass@example.test/keys","https://example.test/keys#fragment","not-a-url"] {
            assert!(RemoteJwks::new(url,CONFIG,None).is_err(),"{url}");
        }
        assert!(RemoteJwks::new("https://example.test/keys",CONFIG,Some("bad certificate")).is_err());
        assert!(RemoteJwks::new("https://example.test/keys",CONFIG,None).is_ok());
    }
}
