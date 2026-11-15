import json,os,subprocess,urllib.error,urllib.request
origin=os.environ['TEST_API']; token=os.environ['TEST_JWT']; admin=os.environ['DATABASE_URL']; psql=os.environ['TEST_PSQL']
def get(query,credential=token):
    headers={'Authorization':'Bearer '+credential} if credential else {}
    try:
        with urllib.request.urlopen(urllib.request.Request(origin+'/v1/experimental/operator/resend-cancel-list?'+query,headers=headers),timeout=5) as r:return r.status,json.loads(r.read())
    except urllib.error.HTTPError as e:return e.code,json.loads(e.read())
def sql(q):subprocess.run([psql,admin,'-v','ON_ERROR_STOP=1','-c',q],check=True,capture_output=True)
query='environmentId=cancel-api-env&graceSeconds=60'
status,page=get(query);assert status==200 and len(page['data'])==50 and page['next'],(status,page)
assert 'private' not in json.dumps(page)
status,second=get(query+'&after='+page['next']);assert status==200 and len(second['data'])==5 and second['next'] is None
rows=page['data']+second['data'];ids=[x['requestId'] for x in rows]
assert ids==sorted(ids) and len(set(ids))==55 and all(x['overdue'] for x in rows)
assert get(query,None)[0]==401
assert get('environmentId=cancel-api-other&graceSeconds=60')[0] in (401,403)
assert get('environmentId=cancel-api-env&graceSeconds=0')[0]==400
assert get(query+'&after=invalid')[0]==400
assert get(query+'&graceSeconds=60')[0]==400
sql("DELETE FROM public.novu_rust_operator_jwt_resend_cancel_list_grants WHERE environment_id='cancel-api-env'")
assert get(query)[0] in (401,403)
sql("INSERT INTO public.novu_rust_operator_jwt_resend_cancel_list_grants VALUES ('https://issuer.test','fixture','console','cancel-api-env'); UPDATE public.novu_rust_resend_cancel_operators SET can_read=false WHERE environment_id='cancel-api-env' AND database_principal='cancel_api_fixture'")
assert get(query)[0] in (401,403)
