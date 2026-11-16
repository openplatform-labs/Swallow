"""Synthetic signed callback against a real API and disposable database."""
import base64,hashlib,hmac,json,os,subprocess,time,urllib.error,urllib.request
origin=os.environ['TEST_API']; token=os.environ['TEST_JWT']
secret=base64.b64decode('dGVzdC1zZWNyZXQtaXMtMzItYnl0ZXMtbG9uZyEh')
def health():
    req=urllib.request.Request(origin+'/v1/experimental/operator/resend-cancel-evidence-health?environmentId=cancel-api-env',headers={'Authorization':'Bearer '+token})
    with urllib.request.urlopen(req,timeout=5) as res:return json.loads(res.read())
def post(kind,event_id,valid=True,org='org'):
    body=json.dumps({'type':kind,'data':{'email_id':'evidence-private-id','to':['fixture@example.test']}},separators=(',',':')).encode()
    timestamp=str(int(time.time()));mac=hmac.new(secret,(event_id+'.'+timestamp+'.').encode()+body,hashlib.sha256).digest()
    signature='v1,'+base64.b64encode(mac if valid else bytes(len(mac))).decode()
    req=urllib.request.Request(origin+'/webhooks/organizations/'+org+'/environments/cancel-api-env/email/resend',data=body,method='POST',headers={'svix-id':event_id,'svix-timestamp':timestamp,'svix-signature':signature,'Content-Type':'application/json'})
    try:
        with urllib.request.urlopen(req,timeout=5) as response:return response.status
    except urllib.error.HTTPError as e:return e.code
assert health()=={'conflicting':0,'ambiguous':0}
assert post('email.opened','signed-fixture',False)==401
assert health()=={'conflicting':0,'ambiguous':0}
assert post('email.opened','signed-fixture',org='wrong-org') ==409
assert health()=={'conflicting':0,'ambiguous':0}
assert post('email.opened','signed-fixture')==204
assert health()=={'conflicting':1,'ambiguous':0}
assert post('email.opened','signed-fixture')==204
assert post('email.clicked','signed-fixture')==409
assert health()=={'conflicting':1,'ambiguous':0}

alert_env={**os.environ,'DATABASE_URL':'postgres://cancel_api_fixture@127.0.0.1:'+os.environ['TEST_DB_PORT']+'/postgres','NOVU_POSTGRES_ALLOW_INSECURE':'true','SWALLOW_ALERT_RESEND_ENABLED':'false','SWALLOW_ALERT_RESEND_CANCEL_EVIDENCE_ENABLED':'true','SWALLOW_ALERT_SCOPED_DELIVERY':'true','SWALLOW_ALERT_COOLDOWN_POLICY':'new-condition','SWALLOW_ALERT_CONDITIONS_V3':'true','SWALLOW_ALERT_SLACK_WEBHOOK':'https://hooks.slack.com/services/A/B/C','SWALLOW_ALERT_SLACK_TARGET_ID':'blocked-evidence'}
alert=subprocess.run([os.environ['TEST_ALERT'],'cancel-api-env','300','120','60'],env=alert_env,text=True,capture_output=True,timeout=15)
assert alert.returncode==2 and 'requires operator review' in alert.stderr,(alert.returncode,alert.stdout,alert.stderr)
