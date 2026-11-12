#!/usr/bin/env python3
"""Compare bounded MongoDB dotted-path semantics with the Rust PostgreSQL function.

Set NOVU_REFERENCE_MONGO_CONTAINER and NOVU_RUST_PG_CONTAINER to disposable
containers. The script replaces a uniquely named temporary MongoDB collection;
the PostgreSQL database must already have migrations 0008 and 0009 applied.
It tests filter matching only, not API validation, webhooks or Socket.IO.
"""
import json
import os
import subprocess
import uuid

mongo = os.environ['NOVU_REFERENCE_MONGO_CONTAINER']
postgres = os.environ['NOVU_RUST_PG_CONTAINER']
collection = 'compat_array_' + uuid.uuid4().hex
docs = [
    {'_id': 'missing'},
    {'_id': 'null_data', 'data': None},
    {'_id': 'empty', 'data': {'orders': []}},
    {'_id': 'mix', 'data': {'orders': [{'status': 'open'}, {}]}},
    {'_id': 'only', 'data': {'orders': [{'status': 'open'}]}},
    {'_id': 'null', 'data': {'orders': [{'status': None}]}},
    {'_id': 'primitive', 'data': {'orders': [1]}},
    {'_id': 'str', 'data': {'orders': 'hello'}},
    {'_id': 'nullparent', 'data': {'orders': None}},
    {'_id': 'nested_array', 'data': {'orders': [[{'status': 'open'}]]}},
    {'_id': 'numeric_key', 'data': {'orders': [{'0': {'status': 'indexed'}}, {'status': 'open'}]}},
    {'_id': 'array_values', 'data': {'orders': [{'status': ['open', None]}]}},
    {'_id': 'nested_values', 'data': {'orders': [{'status': [['open']]}]}},
    {'_id': 'empty_status', 'data': {'orders': [{'status': []}]}},
    {'_id': 'two_status', 'data': {'orders': [{'status': 'open'}, {'status': 'done'}]}},
    {'_id': 'object', 'data': {'orders': {'status': 'open'}}},
    {'_id': 'dot_key', 'data': {'orders': {'0': {'status': 'open'}}}},
]
# Each case is the normalized query used by Rust and its Mongo equivalent.
cases = []
for path in [('orders', 'status'), ('orders', '0', 'status'), ('orders', '3', 'status'), ('orders', '00', 'status')]:
    for group in [[None], ['open'], ['indexed'], [None, 'open']]:
        cases.append((list(path), group))
js = f'''const c=db.getSiblingDB("novu-array-test").getCollection("{collection}");
try {{ c.insertMany({json.dumps(docs)}); const cases={json.dumps(cases)};
print(JSON.stringify(cases.map(([path,group])=>c.find({{["data."+path.join(".")]:group.length===1?group[0]:{{$in:group}}}},{{_id:1}}).toArray().map(x=>x._id).sort())));
}} finally {{ c.drop(); }}'''
raw = subprocess.check_output(['docker', 'exec', mongo, 'mongosh', '--quiet', '--eval', js], text=True)
expected = json.loads(raw.splitlines()[0])

def literal(value):
    return "'" + json.dumps(value, ensure_ascii=True).replace("'", "''") + "'::jsonb"

values = ','.join('(' + "'" + doc['_id'] + "'," + (literal(doc['data']) if 'data' in doc else 'NULL::jsonb') + ')' for doc in docs)
parts = []
for index, (path, group) in enumerate(cases):
    normalized = [{'path': path, 'groups': [group]}]
    parts.append(f"SELECT {index} AS idx, COALESCE(json_agg(id ORDER BY id), '[]'::json) AS ids FROM docs WHERE novu_rust_inbox_data_matches(data,{literal(normalized)})")
sql = f"WITH docs(id,data) AS (VALUES {values}), results AS ({' UNION ALL '.join(parts)}) SELECT json_agg(ids ORDER BY idx) FROM results"
raw = subprocess.check_output(['docker', 'exec', postgres, 'psql', '-U', 'postgres', '-d', 'novu_compat', '-At', '-v', 'ON_ERROR_STOP=1', '-c', sql], text=True)
actual = json.loads(raw)
for case, left, right in zip(cases, expected, actual):
    if left != right:
        raise SystemExit(f'{case}: mismatch\n  mongo={left!r}\n  postgres={right!r}')
print(f'{len(cases)} MongoDB/PostgreSQL array path cases match across {len(docs)} documents')
