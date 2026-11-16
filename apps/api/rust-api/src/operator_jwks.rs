//! Fetch a configured, TLS-authenticated public key set for the pinned issuer.
use std::time::Duration;

#[derive(Clone)]
enum Source {
    Fixed(reqwest::Url),
    Discovery { url:reqwest::Url, issuer:String, allowed_origin:reqwest::Url },
}
#[derive(Clone)]
pub(crate) struct RemoteJwks {
    source: Source,
    client: reqwest::Client,
    policy: serde_json::Value,
}
fn secure_url(value:&str)->Result<reqwest::Url,()> {
    let url=reqwest::Url::parse(value).map_err(|_|())?;
    if url.scheme()!="https" || url.host_str().is_none() || !url.username().is_empty() || url.password().is_some() || url.fragment().is_some() {return Err(());}
    Ok(url)
}
impl RemoteJwks {
    pub fn new(url:&str, config:&str, ca:Option<&str>) -> Result<Self,()> {
        let url=secure_url(url)?;
        let mut policy:serde_json::Value=serde_json::from_str(config).map_err(|_|())?;
        let object=policy.as_object_mut().ok_or(())?;
        object.remove("keys");object.remove("jwks");
        let mut builder=reqwest::Client::builder().no_proxy().redirect(reqwest::redirect::Policy::none()).timeout(Duration::from_secs(10));
        if let Some(ca)=ca {
            let certs=reqwest::Certificate::from_pem_bundle(ca.as_bytes()).map_err(|_|())?;
            if certs.is_empty() {return Err(());}
            for cert in certs {builder=builder.add_root_certificate(cert);}
        }
        Ok(Self{source:Source::Fixed(url),client:builder.build().map_err(|_|())?,policy})
    }
    pub fn discover(config:&str, ca:Option<&str>, origin:Option<&str>)->Result<Self,()> {
        let policy:serde_json::Value=serde_json::from_str(config).map_err(|_|())?;
        let issuer=policy.get("issuer").and_then(|v|v.as_str()).ok_or(())?.to_owned();
        let mut url=secure_url(&issuer)?;
        if url.query().is_some() {return Err(());}
        let allowed_origin=match origin {Some(value)=>{let candidate=secure_url(value)?;if candidate.path()!="/" || candidate.query().is_some(){return Err(());}candidate},None=>url.clone()};
        let path=format!("{}/.well-known/openid-configuration",url.path().trim_end_matches('/'));
        url.set_path(&path);
        let mut result=Self::new(url.as_str(),config,ca)?;
        result.source=Source::Discovery{url,issuer,allowed_origin};
        Ok(result)
    }
    async fn get_json(&self,url:reqwest::Url)->Result<serde_json::Value,()> {
        let mut response=self.client.get(url).send().await.map_err(|_|())?;
        if response.status()!=reqwest::StatusCode::OK || response.content_length().is_some_and(|n| n>16384) {return Err(());}
        let mut bytes=Vec::new();
        while let Some(chunk)=response.chunk().await.map_err(|_|())? {
            if bytes.len().saturating_add(chunk.len())>16384 {return Err(());}
            bytes.extend_from_slice(&chunk);
        }
        serde_json::from_slice(&bytes).map_err(|_|())
    }
    pub async fn fetch(&self)->Result<String,()> {
        let url=match &self.source {
            Source::Fixed(url)=>url.clone(),
            Source::Discovery{url,issuer,allowed_origin}=>{
                let metadata=self.get_json(url.clone()).await?;
                if metadata.get("issuer").and_then(|v|v.as_str())!=Some(issuer.as_str()) {return Err(());}
                let uri=secure_url(metadata.get("jwks_uri").and_then(|v|v.as_str()).ok_or(())?)?;
                if uri.origin()!=allowed_origin.origin() {return Err(());}
                uri
            }
        };
        let jwks=self.get_json(url).await?;
        let mut policy=self.policy.clone();policy.as_object_mut().ok_or(())?.insert("jwks".into(),jwks);
        let value=serde_json::to_string(&policy).map_err(|_|())?;
        if value.len()>16384 {return Err(());}
        Ok(value)
    }
}

#[cfg(test)]
mod tests {
    use super::RemoteJwks;
    const CONFIG:&str=r#"{"issuer":"https://issuer.test/tenant","audience":"api","keys":[]}"#;
    #[test]
    fn rejects_unsafe_or_incomplete_urls_and_bad_ca() {
        for url in ["http://localhost/keys","https://user:pass@example.test/keys","https://example.test/keys#fragment","not-a-url"] {assert!(RemoteJwks::new(url,CONFIG,None).is_err(),"{url}");}
        assert!(RemoteJwks::new("https://example.test/keys",CONFIG,Some("bad certificate")).is_err());
        assert!(RemoteJwks::new("https://example.test/keys",CONFIG,None).is_ok());
        assert!(RemoteJwks::discover(CONFIG,None,Some("http://localhost")).is_err());
        assert!(RemoteJwks::discover(CONFIG,None,Some("https://example.test/path")).is_err());
        assert!(RemoteJwks::discover(CONFIG,None,None).is_ok());
        assert!(RemoteJwks::discover(r#"{"issuer":"http://127.0.0.1","audience":"api"}"#,None,None).is_err());
        assert!(RemoteJwks::discover(r#"{"issuer":"https://issuer.test/path?x=1","audience":"api"}"#,None,None).is_err());
    }
}
