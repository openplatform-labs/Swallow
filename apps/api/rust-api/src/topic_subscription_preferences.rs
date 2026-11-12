//! Subscription preference selection and persistence for API-key topic APIs.
use crate::{ApiError, error_at, validation_error};
use axum::http::StatusCode;
use serde_json::{Value, json};
use tokio_postgres::Client;

#[derive(Clone)]
struct Filter {
    ids: Vec<String>,
    tags: Vec<String>,
    enabled: Option<Value>,
    condition: Option<Value>,
}
#[derive(Clone)]
pub struct Workflow {
    id: String,
    identifier: String,
    metadata: Value,
    default_enabled: bool,
    filter: Option<Filter>,
}

fn unsupported(path: &str) -> ApiError {
    error_at(
        StatusCode::NOT_IMPLEMENTED,
        "Subscription preference option not implemented",
        "Not Implemented",
        path,
    )
}
fn invalid(path: &str, msg: &str) -> ApiError {
    let mut v = validation_error(vec![msg]);
    v.1.0["path"] = json!(path);
    v
}
fn names(value: Option<&Value>) -> Option<Vec<String>> {
    match value {
        None | Some(Value::Null) => Some(vec![]),
        Some(Value::Array(a)) => a.iter().map(|v| v.as_str().map(str::to_owned)).collect(),
        _ => None,
    }
}
fn id_is_object_id(id: &str) -> bool {
    id.len() == 24 && id.bytes().all(|b| b.is_ascii_hexdigit())
}
fn workflow_dto(id: &str, identifier: &str, metadata: &Value) -> Value {
    let mut result = json!({"id":id,"identifier":identifier,"name":metadata.get("name").and_then(Value::as_str).unwrap_or(""),"critical":metadata.get("critical").and_then(Value::as_bool).unwrap_or(false),"tags":metadata.get("tags").cloned().unwrap_or(json!([])),"severity":metadata.get("severity").and_then(Value::as_str).unwrap_or("none")});
    if let Some(data) = metadata.get("data") {
        result["data"] = data.clone();
    }
    result
}

/// Project a stored preference using the workflow when it is still present.
pub(crate) fn stored_dto(
    subscription_identifier: &str,
    workflow_internal_id: &str,
    workflow_identifier: Option<&str>,
    metadata: Option<&Value>,
    preferences: &Value,
) -> Value {
    let enabled = preferences
        .pointer("/all/enabled")
        .filter(|v| !v.is_null())
        .cloned()
        .unwrap_or(json!(true));
    let mut result = json!({"subscriptionId":subscription_identifier,"enabled":enabled});
    if let Some(condition) = preferences.pointer("/all/condition") {
        result["condition"] = condition.clone();
    }
    if let (Some(identifier), Some(metadata)) = (workflow_identifier, metadata) {
        result["workflow"] = workflow_dto(workflow_internal_id, identifier, metadata);
    }
    result
}
fn matches_id(w: &Workflow, id: &str) -> bool {
    if id_is_object_id(id) {
        w.id.eq_ignore_ascii_case(id)
    } else {
        w.identifier == id
    }
}
fn matches_tags(w: &Workflow, tags: &[String]) -> bool {
    w.metadata
        .get("tags")
        .and_then(Value::as_array)
        .is_some_and(|a| {
            tags.iter()
                .any(|tag| a.iter().any(|v| v.as_str() == Some(tag)))
        })
}

/// Resolve requested workflow ids and tags in the authenticated environment.
/// None means the field was omitted/null; Some(empty) is an explicit reset.
pub async fn resolve(
    db: &Client,
    env: &str,
    value: Option<&Value>,
    path: &str,
) -> Result<Option<Vec<Workflow>>, ApiError> {
    let Some(value) = value.filter(|v| !v.is_null()) else {
        return Ok(None);
    };
    let Some(items) = value.as_array() else {
        return Err(invalid(path, "preferences must be an array"));
    };
    if items.is_empty() {
        return Ok(Some(vec![]));
    }
    let mut filters = Vec::new();
    for item in items {
        if item.is_string() || item.is_number() || item.is_boolean() {
            return Err(invalid(
                path,
                "preferences.each value in nested property preferences must be either object or array",
            ));
        }
        let Some(obj) = item.as_object() else {
            return Err(unsupported(path));
        };
        let (ids, tags) = if let Some(filter) = obj.get("filter") {
            let Some(filter) = filter.as_object() else {
                return Err(unsupported(path));
            };
            (
                names(filter.get("workflowIds")).ok_or_else(|| unsupported(path))?,
                names(filter.get("tags")).ok_or_else(|| unsupported(path))?,
            )
        } else if let Some(id) = obj.get("workflowId").and_then(Value::as_str) {
            (vec![id.to_owned()], vec![])
        } else {
            return Err(unsupported(path));
        };
        if obj
            .get("enabled")
            .is_some_and(|v| !v.is_null() && !v.is_boolean())
        {
            return Err(unsupported(path));
        }
        filters.push(Filter {
            ids,
            tags,
            enabled: obj.get("enabled").cloned(),
            condition: obj.get("condition").cloned(),
        });
    }
    let rows=db.query("SELECT w.identifier,w.inbox_metadata,w.active,COALESCE(p.all_enabled,true) FROM novu_rust_workflows w LEFT JOIN novu_rust_workflow_preferences p ON p.environment_id=w.environment_id AND p.workflow_identifier=w.identifier WHERE w.environment_id=$1 ORDER BY w.identifier",&[&env]).await.map_err(|_|error_at(StatusCode::INTERNAL_SERVER_ERROR,"Internal server error","Internal Server Error",path))?;
    let mut all = Vec::new();
    for row in rows {
        let identifier: String = row.get(0);
        let metadata: Option<Value> = row.get(1);
        let active: bool = row.get(2);
        let requested_by_identifier = filters.iter().any(|f| {
            f.ids
                .iter()
                .any(|v| !id_is_object_id(v) && v == &identifier)
        });
        let tags_requested = filters.iter().any(|f| !f.tags.is_empty()) && active;
        let internal_id_requested = filters
            .iter()
            .any(|f| f.ids.iter().any(|id| id_is_object_id(id)));
        let Some(metadata) = metadata else {
            if requested_by_identifier || tags_requested || internal_id_requested {
                return Err(unsupported(path));
            } else {
                continue;
            }
        };
        let Some(id) = metadata
            .get("id")
            .and_then(Value::as_str)
            .map(str::to_owned)
        else {
            if requested_by_identifier || tags_requested || internal_id_requested {
                return Err(unsupported(path));
            } else {
                continue;
            }
        };
        if tags_requested && metadata.get("tags").and_then(Value::as_array).is_none() {
            return Err(unsupported(path));
        }
        let mut w = Workflow {
            id,
            identifier,
            metadata,
            default_enabled: row.get(3),
            filter: None,
        };
        let selected = filters.iter().any(|f| {
            f.ids.iter().any(|id| matches_id(&w, id)) || (active && matches_tags(&w, &f.tags))
        });
        if !selected {
            continue;
        }
        w.filter = filters
            .iter()
            .find(|f| {
                if !f.tags.is_empty() {
                    matches_tags(&w, &f.tags)
                } else {
                    f.ids.iter().any(|id| matches_id(&w, id))
                }
            })
            .cloned();
        all.push(w);
    }
    // Original resolves internal IDs before external identifiers, then tags.
    all.sort_by_key(|w| {
        if filters.iter().any(|f| {
            f.ids
                .iter()
                .any(|id| id_is_object_id(id) && matches_id(w, id))
        }) {
            0
        } else if filters.iter().any(|f| {
            f.ids
                .iter()
                .any(|id| !id_is_object_id(id) && matches_id(w, id))
        }) {
            1
        } else {
            2
        }
    });
    Ok(Some(all))
}
fn stored(w: &Workflow) -> Value {
    let mut all = json!({"enabled":w.default_enabled,"readOnly":false});
    if let Some(filter) = w.filter.as_ref() {
        if let Some(enabled) = filter.enabled.as_ref() {
            all["enabled"] = enabled.clone();
        }
        if let Some(condition) = filter.condition.as_ref() {
            all["condition"] = condition.clone();
        }
    }
    json!({"all":all})
}
fn dto(w: &Workflow, identifier: &str, prefs: &Value) -> Value {
    let enabled = prefs
        .pointer("/all/enabled")
        .filter(|v| !v.is_null())
        .cloned()
        .unwrap_or(json!(true));
    let mut result = json!({"workflow":workflow_dto(&w.id,&w.identifier,&w.metadata),"subscriptionId":identifier,"enabled":enabled});
    if let Some(condition) = prefs.pointer("/all/condition") {
        result["condition"] = condition.clone();
    }
    result
}

/// Insert preferences only for a newly created subscription.
pub async fn insert_new(
    db: &Client,
    env: &str,
    subscriber: &str,
    subscription: &str,
    identifier: &str,
    workflows: &[Workflow],
) -> Result<Vec<Value>, tokio_postgres::Error> {
    let mut response = Vec::new();
    for workflow in workflows {
        let prefs = stored(workflow);
        let created = db.query_opt("INSERT INTO novu_rust_subscription_preferences(environment_id,subscriber_internal_id,subscription_internal_id,workflow_internal_id,preferences) VALUES($1,$2,$3,$4,$5) ON CONFLICT DO NOTHING RETURNING preferences",&[&env,&subscriber,&subscription,&workflow.id,&prefs]).await?;
        let actual: Value = match created {
            Some(row) => row.get(0),
            None => db.query_one("SELECT preferences FROM novu_rust_subscription_preferences WHERE environment_id=$1 AND subscriber_internal_id=$2 AND subscription_internal_id=$3 AND workflow_internal_id=$4 AND context_keys_hash='DEFAULT_CONTEXT'",&[&env,&subscriber,&subscription,&workflow.id]).await?.get(0),
        };
        response.push(dto(workflow, identifier, &actual));
    }
    Ok(response)
}
/// Existing create requests only read preferences for the selected workflows.
pub async fn fetch(
    db: &Client,
    env: &str,
    subscriber: &str,
    subscription: &str,
    identifier: &str,
    workflows: &[Workflow],
) -> Result<Vec<Value>, tokio_postgres::Error> {
    let ids: Vec<String> = workflows.iter().map(|w| w.id.clone()).collect();
    let rows=db.query("SELECT workflow_internal_id,preferences FROM novu_rust_subscription_preferences WHERE environment_id=$1 AND subscriber_internal_id=$2 AND subscription_internal_id=$3 AND workflow_internal_id=ANY($4) ORDER BY created_at,workflow_internal_id,context_keys_hash",&[&env,&subscriber,&subscription,&ids]).await?;
    Ok(rows
        .iter()
        .filter_map(|row| {
            let id: String = row.get(0);
            let w = workflows.iter().find(|w| w.id == id)?;
            let prefs: Value = row.get(1);
            Some(dto(w, identifier, &prefs))
        })
        .collect())
}
/// Match Novu's separate delete and individual create operations. Failure can
/// leave no preferences or a partially recreated set, as in the reference.
pub async fn replace(
    db: &Client,
    env: &str,
    subscriber: &str,
    subscription: &str,
    identifier: &str,
    workflows: &[Workflow],
) -> Result<Vec<Value>, tokio_postgres::Error> {
    db.execute("DELETE FROM novu_rust_subscription_preferences WHERE environment_id=$1 AND subscriber_internal_id=$2 AND subscription_internal_id=$3",&[&env,&subscriber,&subscription]).await?;
    insert_new(db, env, subscriber, subscription, identifier, workflows).await
}
