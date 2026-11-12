#!/usr/bin/env python3
"""Compare a bounded context CRUD flow on disposable reference and Rust APIs.

Requires NOVU_REFERENCE_URL, NOVU_RUST_URL and NOVU_TEST_API_KEY. Both servers
must use equivalent isolated environments and API keys. Creates and deletes a
synthetic contexts. Bridge URLs and full validation are outside this test.
"""
import json
import os
import urllib.error
import urllib.request
import urllib.parse
import time
import uuid

bases = [os.environ['NOVU_REFERENCE_URL'].rstrip('/'), os.environ['NOVU_RUST_URL'].rstrip('/')]
key = os.environ['NOVU_TEST_API_KEY']
identifier = 'compare-' + uuid.uuid4().hex
path = '/v2/contexts/tenant/' + identifier
page_ids = [identifier + '-' + str(i) for i in range(4)]

def request(base, method, target, body=None, authorization=True):
    headers = {'Content-Type': 'application/json'}
    if authorization:
        headers['Authorization'] = 'ApiKey ' + key
    req = urllib.request.Request(base + target, data=json.dumps(body).encode() if body is not None else None, headers=headers, method=method)
    try:
        response = urllib.request.urlopen(req, timeout=15)
    except urllib.error.HTTPError as error:
        response = error
    with response:
        raw = response.read()
        status = response.status
    result = json.loads(raw) if raw else None
    if isinstance(result, dict):
        # Independent databases assign their own timestamps.
        result.pop('timestamp', None)
        data = result.get('data')
        records = data if isinstance(data, list) else [data]
        for record in records:
            if isinstance(record, dict):
                record.pop('createdAt', None)
                record.pop('updatedAt', None)
    return status, result

def compare(name, method, target, body=None, expected=None, authorization=True):
    results = [request(base, method, target, body, authorization) for base in bases]
    assert results[0] == results[1], (name, results)
    if expected is not None:
        assert results[0][0] == expected, (name, results)
    print(name + ': match')
    return results[0]

try:
    compare('create', 'POST', '/v2/contexts', {'type':'tenant', 'id':identifier, 'data':{'old':True}}, 201)
    compare('get', 'GET', path, expected=200)
    compare('duplicate', 'POST', '/v2/contexts', {'type':'tenant', 'id':identifier}, 409)
    _, updated = compare('replace', 'PATCH', path, {'data':{'nested':{'count':2}}}, 200)
    assert updated['data']['data'] == {'nested':{'count':2}}
    compare('get_after_update', 'GET', path, expected=200)
    compare('delete', 'DELETE', path, expected=204)
    compare('get_after_delete', 'GET', path, expected=404)
    for item in page_ids:
        compare('page_create', 'POST', '/v2/contexts', {'type':'tenant', 'id':item}, 201)
        time.sleep(0.02)
    query = '?type=tenant&search=' + urllib.parse.quote(identifier) + '&limit=2&orderDirection=ASC'
    pages = [request(base, 'GET', '/v2/contexts' + query) for base in bases]
    assert all(status == 200 for status, _ in pages), pages
    for page in (body for _, body in pages):
        assert page['totalCount'] == 4 and len(page['data']) == 2 and page['next'], page
    assert pages[0][1]['data'] == pages[1][1]['data'], pages
    cursors = [body['next'] for _, body in pages]
    pages2 = [request(base, 'GET', '/v2/contexts' + query + '&after=' + urllib.parse.quote(cursor)) for base, cursor in zip(bases, cursors)]
    assert all(status == 200 for status, _ in pages2), pages2
    assert pages2[0][1]['data'] == pages2[1][1]['data'], pages2
    assert all(body['previous'] and body['totalCount'] == 4 for _, body in pages2), pages2
    print('list_cursor: match')
    previous_cursors = [body['previous'] for _, body in pages2]
    backs = [request(base, 'GET', '/v2/contexts' + query + '&before=' + urllib.parse.quote(cursor)) for base, cursor in zip(bases, previous_cursors)]
    assert all(status == 200 for status, _ in backs), backs
    assert backs[0][1]['data'] == backs[1][1]['data'] == pages[0][1]['data'], backs
    included = [request(base, 'GET', '/v2/contexts' + query + '&includeCursor=true&after=' + urllib.parse.quote(cursor)) for base, cursor in zip(bases, cursors)]
    assert included[0][1]['data'] == included[1][1]['data'], included
    print('list_before_and_include: match')
    compare('list_search_case', 'GET', '/v2/contexts?search=' + urllib.parse.quote(identifier.upper()) + '&id=' + urllib.parse.quote(page_ids[1]), expected=200)
    compare('list_search_literal', 'GET', '/v2/contexts?search=%5B', expected=200)
    compare('list_missing_cursor', 'GET', '/v2/contexts?after=64a000000000000000000099', expected=200)
    compare('list_conflicting_cursors', 'GET', '/v2/contexts?after=64a000000000000000000099&before=64a000000000000000000099', expected=400)
finally:
    for base in bases:
        for item in page_ids:
            try:
                request(base, 'DELETE', '/v2/contexts/tenant/' + item)
            except (OSError, ValueError):
                pass
        try:
            request(base, 'DELETE', path)
        except (OSError, ValueError):
            pass
