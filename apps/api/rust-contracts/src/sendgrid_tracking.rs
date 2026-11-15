//! Bounded SendGrid tracking settings shared by trigger validation and delivery.
use serde_json::{Map, Value};

/// Validate the supported SDK camelCase input and convert it to SendGrid wire names.
pub fn wire(value: &Value) -> Result<Value, ()> {
    let settings=value.as_object().ok_or(())?;
    let mut output=Map::new();
    for (kind, value) in settings {
        let wire_kind=match kind.as_str() {
            "clickTracking"=>"click_tracking", "openTracking"=>"open_tracking",
            "subscriptionTracking"=>"subscription_tracking", "ganalytics"=>"ganalytics", _=>return Err(())
        };
        let fields=value.as_object().ok_or(())?;
        let mut mapped=Map::new();
        for (key,value) in fields {
            let (wire_key,valid)=match (kind.as_str(),key.as_str()) {
                (_,"enable")=>("enable",value.is_boolean()),
                ("clickTracking","enableText")=>("enable_text",value.is_boolean()),
                ("openTracking"|"subscriptionTracking","substitutionTag")=>("substitution_tag",value.as_str().is_some_and(|s|s.len()<=4096)),
                ("subscriptionTracking","text"|"html")=>(key.as_str(),value.as_str().is_some_and(|s|s.len()<=4096)),
                ("ganalytics","utmSource")=>("utm_source",value.as_str().is_some_and(|s|s.len()<=4096)),
                ("ganalytics","utmMedium")=>("utm_medium",value.as_str().is_some_and(|s|s.len()<=4096)),
                ("ganalytics","utmTerm")=>("utm_term",value.as_str().is_some_and(|s|s.len()<=4096)),
                ("ganalytics","utmContent")=>("utm_content",value.as_str().is_some_and(|s|s.len()<=4096)),
                ("ganalytics","utmCampaign")=>("utm_campaign",value.as_str().is_some_and(|s|s.len()<=4096)),
                _=>return Err(())
            };
            if !valid {return Err(());}
            mapped.insert(wire_key.to_owned(),value.clone());
        }
        output.insert(wire_kind.to_owned(),Value::Object(mapped));
    }
    Ok(Value::Object(output))
}
