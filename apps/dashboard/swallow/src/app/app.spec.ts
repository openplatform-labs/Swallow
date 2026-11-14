import { TestBed } from '@angular/core/testing';
import { afterEach, describe, expect, it, vi } from 'vitest';
import { App } from './app';
import { OperatorOidc } from './operator-oidc';

describe('Swallow console', () => {
  afterEach(() => { vi.restoreAllMocks(); vi.useRealTimers(); });

  it('pages scoped workflow audit entries and retains only the displayed metadata', async () => {
    const row=(n:number)=>({id:`00000000-0000-0000-0000-${n.toString().padStart(12,'0')}`,workflowIdentifier:'welcome',resource:'email',issuer:'issuer',subject:'alice',clientId:'console',databasePrincipal:'api',reason:'ticket',recordedAt:'today',previousActive:true,newActive:false,previousTitleBytes:1,newTitleBytes:2,previousContentBytes:3,newContentBytes:4,previousProvider:null,newProvider:null,secret:'not displayed'});
    const first=Array.from({length:50},(_,i)=>row(i+1)),second=[row(51)];
    const mock=vi.spyOn(globalThis,'fetch').mockResolvedValueOnce(new Response(JSON.stringify({data:first,hasMore:true,next:first[49].id}))).mockResolvedValueOnce(new Response(JSON.stringify({data:second,hasMore:false,next:null}))).mockResolvedValueOnce(new Response(JSON.stringify({data:first,hasMore:true,next:first[49].id})));
    const app=TestBed.createComponent(App).componentInstance;app.setSection('workflows');app.workflowAuthMode='jwt';app.workflowEnvironment='env-a';app.workflowIdentifier='welcome';app.workflowKey='aaa.bbb.ccc';
    await app.loadWorkflowAudit();expect(app.workflowAuditRows().length).toBe(50);expect(JSON.stringify(app.workflowAuditRows())).not.toContain('not displayed');
    await app.loadWorkflowAudit('next');expect(app.workflowAuditPage()).toBe(2);expect(new URL(mock.mock.calls[1][0] as string).searchParams.get('after')).toBe(first[49].id);
    await app.loadWorkflowAudit('previous');expect(app.workflowAuditPage()).toBe(1);app.setSection('overview');expect(app.workflowAuditRows()).toEqual([]);expect(app.workflowKey).toBe('');
  });

  it('rejects unauthorized audit and ignores late audit responses after credential changes', async () => {
    let finish!:(response:Response)=>void;vi.spyOn(globalThis,'fetch').mockResolvedValueOnce(new Response('',{status:401})).mockReturnValueOnce(new Promise(done=>{finish=done;}));
    const app=TestBed.createComponent(App).componentInstance;app.setSection('workflows');app.workflowAuthMode='jwt';app.workflowEnvironment='env-a';app.workflowIdentifier='welcome';app.workflowKey='aaa.bbb.ccc';
    await app.loadWorkflowAudit();expect(app.workflowAuditStatus()).toBe('error');const pending=app.loadWorkflowAudit();app.workflowKey='other.token.value';app.clearWorkflow();finish(new Response(JSON.stringify({data:[],hasMore:false,next:null})));await pending;expect(app.workflowAuditStatus()).toBe('idle');expect(app.workflowAuditRows()).toEqual([]);
  });

  it('reads and writes workflow and email provider using scoped operator JWT', async () => {
    const data={workflowIdentifier:'welcome',channel:'email',title:'title',content:'old',active:true};
    const fetchMock=vi.spyOn(globalThis,'fetch').mockResolvedValueOnce(new Response(JSON.stringify({data}))).mockResolvedValueOnce(new Response(JSON.stringify({data:{...data,content:'new'}}))).mockResolvedValueOnce(new Response(JSON.stringify({data:{workflowIdentifier:'welcome',provider:'sendgrid'}}))).mockResolvedValueOnce(new Response(JSON.stringify({data:{workflowIdentifier:'welcome',provider:'resend'}})));
    const app=TestBed.createComponent(App).componentInstance;app.setSection('workflows');app.workflowAuthMode='jwt';app.workflowEnvironment='env-a';app.workflowIdentifier='welcome';app.workflowKey='aaa.bbb.ccc';
    await app.loadWorkflow();expect(app.workflowStatus()).toBe('loaded');app.workflowContent='new';app.workflowReason='ticket';app.workflowConfirmed=true;await app.saveWorkflow();expect(app.workflowStatus()).toBe('saved');
    const read=new URL(fetchMock.mock.calls[0][0] as string),write=new URL(fetchMock.mock.calls[1][0] as string);expect(read.searchParams.get('resource')).toBe('email');expect(write.searchParams.toString()).toBe('environmentId=env-a');expect(fetchMock.mock.calls[1][1]?.headers).toEqual({Authorization:'Bearer aaa.bbb.ccc','Content-Type':'application/json'});
    expect(JSON.parse(fetchMock.mock.calls[1][1]?.body as string)).toEqual({workflow:'welcome',resource:'email',expected:{title:'title',content:'old',active:true},desired:{title:'title',content:'new',active:true},reason:'ticket'});
    await app.loadProvider();app.providerChoice='resend';app.providerReason='ticket';app.providerConfirmed=true;await app.saveProvider();expect(app.providerStatus()).toBe('saved');
    expect(JSON.parse(fetchMock.mock.calls[3][1]?.body as string)).toEqual({workflow:'welcome',resource:'email-provider',expected:{provider:'sendgrid'},desired:{provider:'resend'},reason:'ticket'});
  });

  it('requires a reason for JWT workflow changes', async () => {
    const fetchMock=vi.spyOn(globalThis,'fetch').mockResolvedValueOnce(new Response(JSON.stringify({data:{workflowIdentifier:'welcome',channel:'sms',title:null,content:'old',active:true}})));
    const app=TestBed.createComponent(App).componentInstance;app.workflowAuthMode='jwt';app.workflowEnvironment='env-a';app.workflowIdentifier='welcome';app.workflowChannel='sms';app.workflowKey='aaa.bbb.ccc';await app.loadWorkflow();app.workflowConfirmed=true;await app.saveWorkflow();expect(fetchMock).toHaveBeenCalledTimes(1);expect(app.workflowMessage()).toContain('사유');
  });

  it('uses the correct JWT payload for SMS, push and Slack steps', async () => {
    const fetchMock=vi.spyOn(globalThis,'fetch');const app=TestBed.createComponent(App).componentInstance;app.workflowAuthMode='jwt';app.workflowEnvironment='env-a';app.workflowIdentifier='welcome';app.workflowKey='aaa.bbb.ccc';
    for(const channel of ['sms','push','slack'] as const){
      app.workflowChannel=channel;const current=channel==='slack'?{content:'old',active:true}:{title:channel==='sms'?null:'title',content:'old',active:true};
      const data={workflowIdentifier:'welcome',...(channel==='slack'?{}:{channel}),...current};fetchMock.mockResolvedValueOnce(new Response(JSON.stringify({data}))).mockResolvedValueOnce(new Response(JSON.stringify({data:{...data,content:'new'}})));
      await app.loadWorkflow();app.workflowContent='new';app.workflowReason='ticket';app.workflowConfirmed=true;await app.saveWorkflow();expect(app.workflowStatus()).toBe('saved');
      const payload=JSON.parse(fetchMock.mock.calls.at(-1)![1]?.body as string);expect(payload.resource).toBe(channel);expect(payload.expected).toEqual(current);expect(payload.desired).toEqual({...current,content:'new'});
    }
  });

  it('clears an expired OIDC workflow read and ignores its delayed response', async () => {
    vi.useFakeTimers();vi.setSystemTime(new Date('2026-01-01T00:00:00Z'));
    vi.spyOn(OperatorOidc.prototype,'prepare').mockImplementation(async settings=>({settings,metadata:{issuer:settings.issuer}}));vi.spyOn(OperatorOidc.prototype,'signIn').mockResolvedValue({token:'aaa.bbb.ccc',subject:'alice',environmentId:'env-a',expiresAt:Math.floor(Date.now()/1000)+2});
    let finish!:(response:Response)=>void;vi.spyOn(globalThis,'fetch').mockReturnValue(new Promise(done=>{finish=done;}));
    const fixture=TestBed.createComponent(App);const app=fixture.componentInstance;app.oidcIssuer='https://issuer.test';app.oidcClientId='console';app.oidcEnvironment='env-a';await app.prepareOidc();await app.signInOidc();app.useOidc('workflows');app.workflowIdentifier='welcome';
    const pending=app.loadWorkflow();vi.setSystemTime(new Date('2026-01-01T00:00:03Z'));finish(new Response(JSON.stringify({data:{workflowIdentifier:'welcome',channel:'email',title:'private',content:'secret',active:true}})));await pending;
    expect(app.workflowKey).toBe('');expect(app.workflowStatus()).toBe('idle');expect(app.workflowContent).toBe('');fixture.destroy();
  });

  it('preserves a sent workflow write result when the OIDC token expires', async () => {
    vi.useFakeTimers();vi.setSystemTime(new Date('2026-01-01T00:00:00Z'));
    vi.spyOn(OperatorOidc.prototype,'prepare').mockImplementation(async settings=>({settings,metadata:{issuer:settings.issuer}}));vi.spyOn(OperatorOidc.prototype,'signIn').mockResolvedValue({token:'aaa.bbb.ccc',subject:'alice',environmentId:'env-a',expiresAt:Math.floor(Date.now()/1000)+2});
    const data={workflowIdentifier:'welcome',channel:'email',title:'title',content:'old',active:true};let finish!:(response:Response)=>void;
    vi.spyOn(globalThis,'fetch').mockResolvedValueOnce(new Response(JSON.stringify({data}))).mockReturnValueOnce(new Promise(done=>{finish=done;}));
    const fixture=TestBed.createComponent(App);const app=fixture.componentInstance;app.oidcIssuer='https://issuer.test';app.oidcClientId='console';app.oidcEnvironment='env-a';await app.prepareOidc();await app.signInOidc();app.useOidc('workflows');app.workflowIdentifier='welcome';await app.loadWorkflow();app.workflowReason='ticket';app.workflowConfirmed=true;
    const pending=app.saveWorkflow();vi.advanceTimersByTime(2000);expect(app.workflowStatus()).toBe('saving');expect(app.workflowKey).toBe('');finish(new Response(JSON.stringify({data})));await pending;
    expect(app.workflowStatus()).toBe('saved');expect(app.workflowContent).toBe('');fixture.destroy();
  });

  it('reads and conditionally changes the workflow email provider', async () => {
    const fetchMock=vi.spyOn(globalThis,'fetch').mockResolvedValueOnce(new Response(JSON.stringify({data:{workflowIdentifier:'welcome',provider:'sendgrid'}}))).mockResolvedValueOnce(new Response(JSON.stringify({data:{workflowIdentifier:'welcome',provider:'resend'}})));
    const app=TestBed.createComponent(App).componentInstance;app.setSection('workflows');app.workflowIdentifier='welcome';app.workflowKey='key';await app.loadProvider();expect(app.providerChoice).toBe('sendgrid');app.providerChoice='resend';app.providerConfirmed=true;await app.saveProvider();
    expect(app.providerStatus()).toBe('saved');expect(JSON.parse(fetchMock.mock.calls[1][1]?.body as string)).toEqual({provider:'resend',expected:'sendgrid'});expect(new URL(fetchMock.mock.calls[0][0] as string).pathname).toBe('/v1/experimental/workflows/welcome/email-provider');
    app.setSection('overview');expect(app.workflowKey).toBe('');expect(app.providerStatus()).toBe('idle');
  });

  it('requires rereading after provider conflict and reports unknown results', async () => {
    const data=JSON.stringify({data:{workflowIdentifier:'welcome',provider:'sendgrid'}});
    vi.spyOn(globalThis,'fetch').mockResolvedValueOnce(new Response(data)).mockResolvedValueOnce(new Response('',{status:409})).mockResolvedValueOnce(new Response(data)).mockRejectedValueOnce(new TypeError('network'));
    const app=TestBed.createComponent(App).componentInstance;app.workflowIdentifier='welcome';app.workflowKey='key';await app.loadProvider();app.providerChoice='resend';app.providerConfirmed=true;await app.saveProvider();expect(app.providerStatus()).toBe('error');expect(app.providerMessage()).toContain('다시 조회');
    await app.loadProvider();app.providerChoice='resend';app.providerConfirmed=true;await app.saveProvider();expect(app.providerStatus()).toBe('unknown');expect(app.providerMessage()).toContain('감사');
  });

  it('loads and updates each supported workflow channel with the correct API contract', async () => {
    const fetchMock=vi.spyOn(globalThis,'fetch');
    const app=TestBed.createComponent(App).componentInstance;app.setSection('workflows');app.workflowIdentifier='welcome';app.workflowKey='test-key';
    for(const channel of ['email','sms','push','slack'] as const){
      app.workflowChannel=channel;
      const data={workflowIdentifier:'welcome',...(channel==='slack'?{}:{channel,title:channel==='sms'?null:'title'}),content:'old',active:true};
      fetchMock.mockResolvedValueOnce(new Response(JSON.stringify({data}),{status:200})).mockResolvedValueOnce(new Response(JSON.stringify({data:{...data,content:'new'}}),{status:200}));
      await app.loadWorkflow();expect(app.workflowStatus()).toBe('loaded');app.workflowContent='new';app.workflowConfirmed=true;await app.saveWorkflow();expect(app.workflowStatus()).toBe('saved');
      const calls=fetchMock.mock.calls.slice(-2);expect((calls[0][0] as string)).toContain(channel==='slack'?'/slack-step':`/channel-steps/${channel}`);
      expect(calls[1][1]?.headers).toEqual({Authorization:'ApiKey test-key','Content-Type':'application/json'});
      expect(JSON.parse(calls[1][1]?.body as string)).toEqual(channel==='slack'?{content:'new',active:true,expected:{content:'old',active:true}}:{title:channel==='sms'?null:'title',content:'new',active:true,expected:{title:channel==='sms'?null:'title',content:'old',active:true}});
    }
    app.setSection('overview');expect(app.workflowKey).toBe('');expect(app.workflowContent).toBe('');
  });

  it('requires confirmation and treats an interrupted workflow update as unknown', async () => {
    let finish!:(response:Response)=>void;
    const fetchMock=vi.spyOn(globalThis,'fetch').mockResolvedValueOnce(new Response(JSON.stringify({data:{workflowIdentifier:'welcome',channel:'email',title:'title',content:'old',active:true}}))).mockReturnValueOnce(new Promise(done=>{finish=done;}));
    const app=TestBed.createComponent(App).componentInstance;app.setSection('workflows');app.workflowIdentifier='welcome';app.workflowKey='key';await app.loadWorkflow();await app.saveWorkflow();expect(fetchMock).toHaveBeenCalledTimes(1);
    app.workflowConfirmed=true;const pending=app.saveWorkflow();app.setSection('overview');expect(app.section()).toBe('workflows');finish(new Response('',{status:500}));await pending;
    expect(app.workflowStatus()).toBe('unknown');expect(app.workflowMessage()).toContain('감사');expect(app.workflowConfirmed).toBe(false);
  });

  it('rejects a stale workflow edit and requires a fresh read', async () => {
    const fetchMock=vi.spyOn(globalThis,'fetch').mockResolvedValueOnce(new Response(JSON.stringify({data:{workflowIdentifier:'welcome',channel:'email',title:'title',content:'old',active:true}}))).mockResolvedValueOnce(new Response('',{status:409}));
    const app=TestBed.createComponent(App).componentInstance;app.workflowIdentifier='welcome';app.workflowKey='key';await app.loadWorkflow();app.workflowConfirmed=true;await app.saveWorkflow();
    expect(app.workflowStatus()).toBe('error');expect(app.workflowMessage()).toContain('다시 조회');await app.saveWorkflow();expect(fetchMock).toHaveBeenCalledTimes(2);
  });

  it('does not apply an outdated workflow read after credentials change', async () => {
    let finish!:(response:Response)=>void;vi.spyOn(globalThis,'fetch').mockReturnValueOnce(new Promise(done=>{finish=done;}));
    const app=TestBed.createComponent(App).componentInstance;app.setSection('workflows');app.workflowIdentifier='welcome';app.workflowKey='key';const pending=app.loadWorkflow();app.workflowKey='other';app.clearWorkflow();
    finish(new Response(JSON.stringify({data:{workflowIdentifier:'welcome',channel:'email',title:'secret',content:'private',active:true}})));await pending;
    expect(app.workflowStatus()).toBe('idle');expect(app.workflowContent).toBe('');
  });

  it('pages Resend jobs and selects a job using a fresh result request', async () => {
    const row=(n:number)=>({jobId:`00000000-0000-0000-0000-${n.toString().padStart(12,'0')}`,jobStatus:'completed',result:'unknown',attemptState:null,providerIdKnown:false,providerIdAmbiguous:false,autoCorrelated:false,autoCorrelationConflict:false,autoCorrelatedAt:null,lastCompletedScanAt:null,secret:'hidden'});
    const page1=Array.from({length:20},(_,i)=>row(i+1)); const page2=[row(21)];
    const fetchMock=vi.spyOn(globalThis,'fetch').mockResolvedValueOnce(new Response(JSON.stringify({data:page1,hasMore:true,next:page1[19].jobId}))).mockResolvedValueOnce(new Response(JSON.stringify({data:page2,hasMore:false,next:null}))).mockResolvedValueOnce(new Response(JSON.stringify({data:page1,hasMore:true,next:page1[19].jobId}))).mockResolvedValueOnce(new Response(JSON.stringify(page1[0])));
    const app=TestBed.createComponent(App).componentInstance;app.setSection('alerts');app.alertAuthMode='jwt';app.alertEnvironment='env-a';app.alertKey='aaa.bbb.ccc';
    await app.loadResendJobs();expect(app.resendJobs().length).toBe(20);expect(JSON.stringify(app.resendJobs())).not.toContain('hidden');
    await app.loadResendJobs('next');expect(app.resendJobsPage()).toBe(2);expect(new URL(fetchMock.mock.calls[1][0] as string).searchParams.get('after')).toBe(page1[19].jobId);
    await app.loadResendJobs('previous');expect(app.resendJobsPage()).toBe(1);expect(new URL(fetchMock.mock.calls[2][0] as string).searchParams.has('after')).toBe(false);
    app.selectResendJob(app.resendJobs()[0]);await vi.waitFor(()=>expect(app.resendStatus()).toBe('loaded'));expect(app.resendResult()?.jobId).toBe(page1[0].jobId);
    expect(new URL(fetchMock.mock.calls[3][0] as string).pathname).toBe('/v1/experimental/operator/resend-result');
    app.setSection('overview');expect(app.resendJobs()).toEqual([]);expect(app.resendJobsStatus()).toBe('idle');
  });

  it('rejects unauthorized and malformed lists and ignores a late response after logout', async () => {
    let finish!:(response:Response)=>void;
    vi.spyOn(globalThis,'fetch').mockResolvedValueOnce(new Response('',{status:401})).mockResolvedValueOnce(new Response(JSON.stringify({data:[],hasMore:true,next:'bad'}))).mockReturnValueOnce(new Promise(done=>{finish=done;}));
    const app=TestBed.createComponent(App).componentInstance;app.setSection('alerts');app.alertAuthMode='jwt';app.alertEnvironment='env-a';app.alertKey='aaa.bbb.ccc';
    await app.loadResendJobs();expect(app.resendJobsError()).toContain('권한');
    await app.loadResendJobs();expect(app.resendJobsError()).toContain('응답');
    const pending=app.loadResendJobs();app.forgetAlertKey();finish(new Response(JSON.stringify({data:[],hasMore:false,next:null})));await pending;
    expect(app.resendJobsStatus()).toBe('idle');expect(app.resendJobs()).toEqual([]);
  });

  it('loads the scoped Resend result using the operator JWT and keeps only display fields', async () => {
    const job='1889f88e-c0a3-4ab5-b861-2fdc1f26f254';
    const body={jobId:job,jobStatus:'completed',result:'unknown',attemptState:'accepted',providerIdKnown:true,providerIdAmbiguous:false,autoCorrelated:true,autoCorrelationConflict:true,autoCorrelatedAt:'2026-01-01',lastCompletedScanAt:null,unexpectedSecret:'private'};
    const fetchMock=vi.spyOn(globalThis,'fetch').mockResolvedValue(new Response(JSON.stringify(body),{status:200}));
    const app=TestBed.createComponent(App).componentInstance;app.setSection('alerts');app.alertAuthMode='jwt';app.alertEnvironment='env-a';app.alertKey='aaa.bbb.ccc';app.resendJobId=job;
    await app.loadResend();
    expect(app.resendStatus()).toBe('loaded');expect(app.resendResult()?.autoCorrelationConflict).toBe(true);
    expect(JSON.stringify(app.resendResult())).not.toContain('private');
    const url=new URL(fetchMock.mock.calls[0][0] as string);expect(url.pathname).toBe('/v1/experimental/operator/resend-result');expect(url.searchParams.get('environmentId')).toBe('env-a');expect(url.searchParams.get('jobId')).toBe(job);
    expect(fetchMock.mock.calls[0][1]).toEqual(expect.objectContaining({headers:{Authorization:'Bearer aaa.bbb.ccc'},credentials:'omit',cache:'no-store',redirect:'error'}));
    app.setSection('overview');expect(app.resendResult()).toBeNull();expect(app.alertKey).toBe('');expect(app.resendJobId).toBe('');
  });

  it('rejects malformed Resend requests and results without retaining stale data', async () => {
    const job='1889f88e-c0a3-4ab5-b861-2fdc1f26f254';
    const fetchMock=vi.spyOn(globalThis,'fetch').mockResolvedValueOnce(new Response('',{status:401})).mockResolvedValueOnce(new Response(JSON.stringify({jobId:job,result:'delivered'}),{status:200})).mockResolvedValueOnce(new Response(JSON.stringify({jobId:job,jobStatus:'completed',result:'delivered',attemptState:'accepted',providerIdKnown:true,providerIdAmbiguous:false,autoCorrelated:true,autoCorrelationConflict:true,autoCorrelatedAt:'today',lastCompletedScanAt:null}),{status:200}));
    const app=TestBed.createComponent(App).componentInstance;app.setSection('alerts');app.alertAuthMode='jwt';app.alertEnvironment='env-a';app.alertKey='aaa.bbb.ccc';
    app.resendJobId='not-a-uuid';await app.loadResend();expect(fetchMock).not.toHaveBeenCalled();expect(app.resendStatus()).toBe('error');
    app.resendJobId=job;await app.loadResend();expect(app.resendStatus()).toBe('error');expect(app.resendError()).toContain('권한');
    await app.loadResend();expect(app.resendStatus()).toBe('error');expect(app.resendResult()).toBeNull();
    await app.loadResend();expect(app.resendStatus()).toBe('error');expect(app.resendError()).toContain('응답');expect(app.resendResult()).toBeNull();
  });

  it('discards a late Resend response after credentials or section change', async () => {
    let finish!:(response:Response)=>void;vi.spyOn(globalThis,'fetch').mockReturnValueOnce(new Promise(done=>{finish=done;}));
    const app=TestBed.createComponent(App).componentInstance;app.setSection('alerts');app.alertAuthMode='jwt';app.alertEnvironment='env-a';app.alertKey='aaa.bbb.ccc';app.resendJobId='1889f88e-c0a3-4ab5-b861-2fdc1f26f254';
    const pending=app.loadResend();app.setSection('overview');finish(new Response(JSON.stringify({jobId:'1889f88e-c0a3-4ab5-b861-2fdc1f26f254',jobStatus:'completed',result:'delivered',attemptState:'accepted',providerIdKnown:true,providerIdAmbiguous:false,autoCorrelated:true,autoCorrelationConflict:false,autoCorrelatedAt:'today',lastCompletedScanAt:null}),{status:200}));await pending;
    expect(app.resendStatus()).toBe('idle');expect(app.resendResult()).toBeNull();expect(app.alertKey).toBe('');
  });

  it('looks up a saved permission request using the current token after a reload', async () => {
    const id='1889f88e-c0a3-4ab5-b861-2fdc1f26f254';const desired={enabled:true,subscriberRead:true,alertRead:false,alertRetry:false,alertAccepted:false};
    const fetchMock=vi.spyOn(globalThis,'fetch').mockResolvedValueOnce(new Response(JSON.stringify({found:true,requestId:id,targetIssuer:'https://issuer.test',targetSubject:'alice',targetClientId:'console',expected:null,desired,reason:'ticket',changed:true,createdAt:'2026-01-01'}),{status:200})).mockResolvedValueOnce(new Response(JSON.stringify({found:false}),{status:200})).mockResolvedValueOnce(new Response('',{status:401}));
    const app=TestBed.createComponent(App).componentInstance;app.setSection('settings');app.permissionEnvironment='env-a';app.permissionKey='new.token.value';app.permissionRequestId=id;
    await app.lookupPermissionRequest();expect(app.permissionRequestStatus()).toBe('found');expect(app.permissionRequestRecord()?.desired.subscriberRead).toBe(true);
    const url=new URL(fetchMock.mock.calls[0][0] as string);expect(url.pathname).toBe('/v1/experimental/operator/permissions/request');expect(url.searchParams.get('requestId')).toBe(id);expect(fetchMock.mock.calls[0][1]?.headers).toEqual({Authorization:'Bearer new.token.value'});
    await app.lookupPermissionRequest();expect(app.permissionRequestStatus()).toBe('missing');expect(app.permissionRequestRecord()).toBeNull();
    await app.lookupPermissionRequest();expect(app.permissionRequestStatus()).toBe('error');app.setSection('overview');expect(app.permissionRequestId).toBe('');expect(app.permissionKey).toBe('');
  });

  it('ignores a saved request response after the credential changes', async () => {
    let finish!:(response:Response)=>void;vi.spyOn(globalThis,'fetch').mockReturnValueOnce(new Promise(done=>{finish=done;}));
    const app=TestBed.createComponent(App).componentInstance;app.permissionEnvironment='env-a';app.permissionKey='aaa.bbb.ccc';app.permissionRequestId='1889f88e-c0a3-4ab5-b861-2fdc1f26f254';
    const pending=app.lookupPermissionRequest();app.permissionKey='different.token.value';app.clearPermissions();finish(new Response(JSON.stringify({found:false}),{status:200}));await pending;
    expect(app.permissionRequestStatus()).toBe('idle');expect(app.permissionRequestRecord()).toBeNull();
  });

  it('changes a loaded identity using its observed state and keeps the request ID', async () => {
    const old={issuer:'https://issuer.test',subject:'alice',client_id:'console',environment_id:'env-a',enabled:true,subscriber_read:true,alert_read:false,alert_retry:false,alert_accepted:false};
    const fetchMock=vi.spyOn(globalThis,'fetch').mockResolvedValueOnce(new Response(JSON.stringify({found:true,identity:old}),{status:200})).mockResolvedValueOnce(new Response(JSON.stringify({changed:true,replayed:false}),{status:200}));
    const app=TestBed.createComponent(App).componentInstance;app.setSection('settings');app.permissionEnvironment='env-a';app.permissionKey='aaa.bbb.ccc';app.permissionIssuer=old.issuer;app.permissionSubject=old.subject;app.permissionClient=old.client_id;
    await app.loadPermissions('identity');app.permissionDesired.alertRead=true;app.permissionReason='ticket';app.permissionConfirmed=true;await app.changePermissions();
    expect(app.permissionChangeStatus()).toBe('recorded');expect(app.permissionChangeChanged()).toBe(true);
    const sent=JSON.parse(fetchMock.mock.calls[1][1]?.body as string);expect(sent.expected.alertRead).toBe(false);expect(sent.desired.alertRead).toBe(true);expect(sent.requestId).toBe(app.permissionChange()?.body.requestId);
  });

  it('keeps an uncertain creation and only retries its exact request with the original credential', async () => {
    const fetchMock=vi.spyOn(globalThis,'fetch').mockResolvedValueOnce(new Response(JSON.stringify({found:false}),{status:200})).mockRejectedValueOnce(new TypeError('network')).mockResolvedValueOnce(new Response(JSON.stringify({changed:true,replayed:true}),{status:200}));
    const app=TestBed.createComponent(App).componentInstance;app.setSection('settings');app.permissionEnvironment='env-a';app.permissionKey='aaa.bbb.ccc';app.permissionIssuer='https://issuer.test';app.permissionSubject='alice';app.permissionClient='console';
    await app.loadPermissions('identity');app.permissionDesired.enabled=true;app.permissionReason='ticket';app.permissionConfirmed=true;await app.changePermissions();
    expect(app.permissionChangeStatus()).toBe('unknown');expect(app.permissionChange()?.body.expected).toBeNull();
    app.permissionKey='different.token.value';app.permissionRecoveryConfirmed=true;await app.recoverPermissionChange('retry');expect(app.permissionChangeStatus()).toBe('unknown');expect(fetchMock).toHaveBeenCalledTimes(2);
    app.permissionKey='aaa.bbb.ccc';app.permissionRecoveryConfirmed=true;await app.recoverPermissionChange('retry');expect(app.permissionChangeStatus()).toBe('recorded');expect(fetchMock.mock.calls[2][1]?.body).toBe(fetchMock.mock.calls[1][1]?.body);
  });

  it('does not treat a lookup for different permission contents as the original request', async () => {
    const fetchMock=vi.spyOn(globalThis,'fetch').mockResolvedValueOnce(new Response(JSON.stringify({found:false}),{status:200})).mockRejectedValueOnce(new TypeError('network'));
    const app=TestBed.createComponent(App).componentInstance;app.permissionEnvironment='env-a';app.permissionKey='aaa.bbb.ccc';app.permissionIssuer='https://issuer.test';app.permissionSubject='alice';app.permissionClient='console';
    await app.loadPermissions('identity');app.permissionDesired.enabled=true;app.permissionReason='ticket';app.permissionConfirmed=true;await app.changePermissions();
    const snapshot=app.permissionChange()!;fetchMock.mockResolvedValueOnce(new Response(JSON.stringify({found:true,requestId:snapshot.body.requestId,targetIssuer:snapshot.body.targetIssuer,targetSubject:snapshot.body.targetSubject,targetClientId:snapshot.body.targetClientId,reason:snapshot.body.reason,expected:null,desired:{...snapshot.body.desired,alertRead:true},changed:true}),{status:200}));
    await app.recoverPermissionChange('lookup');expect(app.permissionChangeStatus()).toBe('unknown');expect(app.permissionChangeMessage()).toContain('불명확');
  });

  it('keeps the request while a permission change is in flight', async () => {
    let finish!:(response:Response)=>void;
    vi.spyOn(globalThis,'fetch').mockResolvedValueOnce(new Response(JSON.stringify({found:false}),{status:200})).mockImplementationOnce(()=>new Promise(done=>{finish=done;}));
    const app=TestBed.createComponent(App).componentInstance;app.setSection('settings');app.permissionEnvironment='env-a';app.permissionKey='aaa.bbb.ccc';app.permissionIssuer='https://issuer.test';app.permissionSubject='alice';app.permissionClient='console';
    await app.loadPermissions('identity');app.permissionDesired.enabled=true;app.permissionReason='ticket';app.permissionConfirmed=true;
    const pending=app.changePermissions();await vi.waitFor(()=>expect(finish).toBeDefined());app.setSection('overview');app.clearPermissionChange();expect(app.section()).toBe('settings');expect(app.permissionChange()?.body.requestId).toBeTruthy();
    finish(new Response(JSON.stringify({changed:true,replayed:false}),{status:200}));await pending;expect(app.permissionChangeStatus()).toBe('recorded');
  });

  it('reviews a specific operator identity and clears credentials and results on leaving settings', async () => {
    const fetchMock=vi.spyOn(globalThis,'fetch').mockResolvedValue(new Response(JSON.stringify({found:true,identity:{issuer:'https://issuer.test',subject:'alice',client_id:'console',environment_id:'env-a',enabled:true,subscriber_read:true,alert_read:false,alert_retry:false,alert_accepted:false}}),{status:200}));
    const app=TestBed.createComponent(App).componentInstance;app.setSection('settings');app.permissionEnvironment='env-a';app.permissionKey='aaa.bbb.ccc';app.permissionIssuer='https://issuer.test';app.permissionSubject='alice';app.permissionClient='console';
    await app.loadPermissions('identity');expect(app.permissionStatus()).toBe('identity');expect(app.permissionIdentity()?.subscriberRead).toBe(true);
    const url=new URL(fetchMock.mock.calls[0][0] as string);expect(url.pathname).toBe('/v1/experimental/operator/permissions');expect(url.searchParams.get('issuer')).toBe('https://issuer.test');
    expect(fetchMock.mock.calls[0][1]).toEqual(expect.objectContaining({headers:{Authorization:'Bearer aaa.bbb.ccc'},credentials:'omit',redirect:'error'}));
    app.setSection('overview');expect(app.permissionKey).toBe('');expect(app.permissionIdentity()).toBeNull();
  });

  it('handles audit pagination and independently rejected audit access', async () => {
    const row={id:'9007199254740993',recordedAt:'2026-01-01',issuer:'https://issuer.test',subject:'alice',clientId:'console',actor:'api-db',jwtIssuer:'https://issuer.test',jwtSubject:'admin',jwtClientId:'console',requestId:'request-id',reason:'ticket',previous:null,current:{enabled:true}};
    const fetchMock=vi.spyOn(globalThis,'fetch').mockResolvedValueOnce(new Response(JSON.stringify({data:[row],next:'9007199254740993'}),{status:200})).mockResolvedValueOnce(new Response(JSON.stringify({data:[],next:null}),{status:200})).mockResolvedValueOnce(new Response('',{status:401}));
    const app=TestBed.createComponent(App).componentInstance;app.permissionEnvironment='env-a';app.permissionKey='aaa.bbb.ccc';
    await app.loadPermissions('audit');expect(app.permissionAudit()[0].id).toBe(row.id);expect(app.permissionAudit()[0].jwtSubject).toBe('admin');expect(app.permissionNext()).toBe(row.id);
    await app.loadPermissions('audit',true);expect(new URL(fetchMock.mock.calls[1][0] as string).searchParams.get('before')).toBe(row.id);expect(app.permissionAudit()).toEqual([]);
    await app.loadPermissions('audit');expect(app.permissionStatus()).toBe('error');expect(app.permissionAudit()).toEqual([]);
  });

  it('discards an in-flight permission response when the environment changes', async () => {
    let finish!:(response:Response)=>void;vi.spyOn(globalThis,'fetch').mockReturnValueOnce(new Promise(done=>{finish=done;}));
    const app=TestBed.createComponent(App).componentInstance;app.permissionEnvironment='env-a';app.permissionKey='aaa.bbb.ccc';
    const pending=app.loadPermissions('audit');app.permissionEnvironment='env-b';app.clearPermissions();finish(new Response(JSON.stringify({data:[],next:null}),{status:200}));await pending;
    expect(app.permissionStatus()).toBe('idle');expect(app.permissionAudit()).toEqual([]);
  });

  it('clears the login token from permission review when that session expires', async () => {
    vi.useFakeTimers();vi.setSystemTime(new Date('2026-01-01T00:00:00Z'));
    vi.spyOn(OperatorOidc.prototype,'prepare').mockImplementation(async settings=>({settings,metadata:{issuer:settings.issuer}}));
    vi.spyOn(OperatorOidc.prototype,'signIn').mockResolvedValue({token:'aaa.bbb.ccc',subject:'alice',environmentId:'env-a',expiresAt:Math.floor(Date.now()/1000)+2});
    const fixture=TestBed.createComponent(App);const app=fixture.componentInstance;app.oidcIssuer='https://issuer.test';app.oidcClientId='console';app.oidcEnvironment='env-a';
    await app.prepareOidc();await app.signInOidc();app.setSection('settings');app.useOidcForPermissions();expect(app.permissionKey).toBe('aaa.bbb.ccc');expect(app.permissionEnvironment).toBe('env-a');
    vi.advanceTimersByTime(2000);expect(app.permissionKey).toBe('');expect(app.permissionStatus()).toBe('idle');expect(app.oidcStatus()).toBe('expired');fixture.destroy();
  });

  it('uses the selected login for authorized alert decisions and request lookup without copying the token', async () => {
    vi.spyOn(OperatorOidc.prototype,'prepare').mockImplementation(async settings=>({settings,metadata:{issuer:settings.issuer}}));
    vi.spyOn(OperatorOidc.prototype,'signIn').mockResolvedValue({token:'aaa.bbb.ccc',subject:'alice',environmentId:'env-a',expiresAt:Math.floor(Date.now()/1000)+600});
    const attempt='1889f88e-c0a3-4ab5-b861-2fdc1f26f254';
    const fetchMock=vi.spyOn(globalThis,'fetch').mockResolvedValueOnce(new Response(JSON.stringify({data:[{channel:'slack',targetId:'ops',state:'unknown',lastStartedAt:null,attemptId:attempt}],hasMore:false,next:null}),{status:200})).mockResolvedValueOnce(new Response(JSON.stringify({recorded:true,replayed:false}),{status:200})).mockResolvedValueOnce(new Response(JSON.stringify({found:false}),{status:200}));
    const fixture=TestBed.createComponent(App);const app=fixture.componentInstance;
    app.oidcIssuer='https://issuer.test';app.oidcClientId='console';app.oidcEnvironment='env-a';await app.prepareOidc();await app.signInOidc();app.useOidc('alerts');
    await app.loadAlerts();app.selectAlert(app.alertSlots()[0]);app.decisionReason='checked';app.decisionConfirmed=true;app.useOidcForDecision('decision');
    await app.resolveAlert();expect(app.decisionStatus()).toBe('recorded');expect(fetchMock.mock.calls[1][1]?.headers).toEqual({Authorization:'Bearer aaa.bbb.ccc','Content-Type':'application/json'});
    app.useOidcForDecision('lookup');await app.lookupDecisionRequest();expect(app.lookupStatus()).toBe('not-found');expect(fetchMock.mock.calls[2][1]?.headers).toEqual({Authorization:'Bearer aaa.bbb.ccc'});
    app.alertEnvironment='other';app.lookupKey='';app.useOidcForDecision('lookup');expect(app.lookupKey).toBe('');
    app.alertEnvironment='env-a';app.alertAuthMode='restricted';app.useOidcForDecision('decision');expect(app.decisionKey).toBe('');fixture.destroy();
  });

  it('reuses the login for an uncertain decision only with the original token', async () => {
    vi.spyOn(OperatorOidc.prototype,'prepare').mockImplementation(async settings=>({settings,metadata:{issuer:settings.issuer}}));
    vi.spyOn(OperatorOidc.prototype,'signIn').mockResolvedValue({token:'aaa.bbb.ccc',subject:'alice',environmentId:'env-a',expiresAt:Math.floor(Date.now()/1000)+600});
    const attempt='1889f88e-c0a3-4ab5-b861-2fdc1f26f254';
    const fetchMock=vi.spyOn(globalThis,'fetch').mockResolvedValueOnce(new Response(JSON.stringify({data:[{channel:'slack',targetId:'ops',state:'unknown',lastStartedAt:null,attemptId:attempt}],hasMore:false,next:null}),{status:200})).mockRejectedValueOnce(new TypeError('network')).mockResolvedValueOnce(new Response(JSON.stringify({recorded:true,replayed:true}),{status:200}));
    const fixture=TestBed.createComponent(App);const app=fixture.componentInstance;
    app.oidcIssuer='https://issuer.test';app.oidcClientId='console';app.oidcEnvironment='env-a';await app.prepareOidc();await app.signInOidc();app.useOidc('alerts');await app.loadAlerts();app.selectAlert(app.alertSlots()[0]);
    app.decisionReason='checked';app.decisionConfirmed=true;app.useOidcForDecision('decision');await app.resolveAlert();expect(app.decisionStatus()).toBe('unknown');
    app.useOidcForDecision('retry');app.retryConfirmed=true;await app.retryDecision();expect(app.decisionStatus()).toBe('recorded');
    expect(fetchMock.mock.calls[2][1]?.body).toBe(fetchMock.mock.calls[1][1]?.body);fixture.destroy();
  });

  it('expires the popup token without losing an uncertain decision or a different credential', async () => {
    vi.useFakeTimers(); vi.setSystemTime(new Date('2026-01-01T00:00:00Z'));
    vi.spyOn(OperatorOidc.prototype,'prepare').mockImplementation(async settings=>({settings,metadata:{issuer:settings.issuer}}));
    vi.spyOn(OperatorOidc.prototype,'signIn').mockResolvedValue({token:'aaa.bbb.ccc',subject:'alice',environmentId:'env-a',expiresAt:Math.floor(Date.now()/1000)+2});
    const fixture=TestBed.createComponent(App); const app=fixture.componentInstance;
    app.oidcIssuer='https://issuer.test';app.oidcClientId='console';app.oidcEnvironment='env-a';
    await app.prepareOidc();await app.signInOidc();app.useOidc('alerts');
    app.useOidcForDecision('decision');app.useOidcForDecision('lookup');app.useOidcForDecision('retry');
    app.apiKey='different.credential.value';app.decisionStatus.set('unknown');app.decisionRequestId.set('retained-request');
    vi.advanceTimersByTime(2000);
    expect(app.oidcStatus()).toBe('expired');expect(app.oidcSubject()).toBe('');expect(app.alertKey).toBe('');
    expect(app.decisionKey).toBe('');expect(app.lookupKey).toBe('');expect(app.retryKey).toBe('');
    expect(app.apiKey).toBe('different.credential.value');expect(app.decisionStatus()).toBe('unknown');expect(app.decisionRequestId()).toBe('retained-request');
    app.useOidc('alerts');expect(app.alertKey).toBe('');
    fixture.destroy();
  });

  it('discards pending subscriber, alert and decision lookup responses after delayed OIDC expiry', async () => {
    vi.useFakeTimers();vi.setSystemTime(new Date('2026-01-01T00:00:00Z'));
    vi.spyOn(OperatorOidc.prototype,'prepare').mockImplementation(async settings=>({settings,metadata:{issuer:settings.issuer}}));
    vi.spyOn(OperatorOidc.prototype,'signIn').mockImplementation(async()=>({token:'aaa.bbb.ccc',subject:'alice',environmentId:'env-a',expiresAt:Math.floor(Date.now()/1000)+2}));
    let finish!:(response:Response)=>void;
    const fetchMock=vi.spyOn(globalThis,'fetch');
    for (const kind of ['subscribers','alerts','lookup'] as const) {
      vi.setSystemTime(new Date('2026-01-01T00:00:00Z'));
      const fixture=TestBed.createComponent(App);const app=fixture.componentInstance;
      app.oidcIssuer='https://issuer.test';app.oidcClientId='console';app.oidcEnvironment='env-a';await app.prepareOidc();await app.signInOidc();
      app.useOidc(kind==='subscribers'?'subscribers':'alerts');
      fetchMock.mockReturnValueOnce(new Promise(done=>{finish=done;}));
      if (kind==='lookup') {app.useOidcForDecision('lookup');app.lookupId='1889f88e-c0a3-4ab5-b861-2fdc1f26f254';}
      const pending=kind==='subscribers'?app.loadSubscribers():kind==='alerts'?app.loadAlerts():app.lookupDecisionRequest();
      app.decisionStatus.set('unknown');app.decisionRequestId.set('preserved');
      vi.setSystemTime(new Date('2026-01-01T00:00:03Z'));
      const body=kind==='subscribers'?{data:[{subscriberId:'person',email:'private@example.test'}],next:null,previous:null}:kind==='alerts'?{data:[{channel:'slack',targetId:'ops',state:'unknown',lastStartedAt:null}],hasMore:false,next:null}:{found:false};
      finish(new Response(JSON.stringify(body),{status:200}));await pending;
      expect(app.oidcStatus()).toBe('expired');expect(app.subscribers()).toEqual([]);expect(app.alertSlots()).toEqual([]);expect(app.lookupStatus()).toBe('idle');
      expect(app.decisionStatus()).toBe('unknown');expect(app.decisionRequestId()).toBe('preserved');fixture.destroy();
    }
  });

  it('removes already displayed subscriber and alert data when their OIDC session expires', async () => {
    vi.useFakeTimers();vi.setSystemTime(new Date('2026-01-01T00:00:00Z'));
    vi.spyOn(OperatorOidc.prototype,'prepare').mockImplementation(async settings=>({settings,metadata:{issuer:settings.issuer}}));
    vi.spyOn(OperatorOidc.prototype,'signIn').mockResolvedValue({token:'aaa.bbb.ccc',subject:'alice',environmentId:'env-a',expiresAt:Math.floor(Date.now()/1000)+2});
    const fixture=TestBed.createComponent(App);const app=fixture.componentInstance;
    app.oidcIssuer='https://issuer.test';app.oidcClientId='console';app.oidcEnvironment='env-a';await app.prepareOidc();await app.signInOidc();
    app.apiKey='aaa.bbb.ccc';app.alertKey='aaa.bbb.ccc';app.lookupKey='aaa.bbb.ccc';app.lookupId='preserved-lookup';
    app.subscribers.set([{id:'person',firstName:'',lastName:'',email:'private@example.test'}]);app.subscriberStatus.set('loaded');
    app.alertSlots.set([{channel:'slack',targetId:'ops',state:'unknown',lastStartedAt:null}]);app.alertStatus.set('loaded');
    app.decisionStatus.set('unknown');app.decisionRequestId.set('preserved-request');
    vi.advanceTimersByTime(2000);
    expect(app.subscribers()).toEqual([]);expect(app.subscriberStatus()).toBe('idle');expect(app.alertSlots()).toEqual([]);expect(app.alertStatus()).toBe('idle');
    expect(app.lookupId).toBe('preserved-lookup');expect(app.decisionStatus()).toBe('unknown');expect(app.decisionRequestId()).toBe('preserved-request');fixture.destroy();
  });

  it('discards permission reads after delayed OIDC expiry while preserving the stored request ID', async () => {
    vi.useFakeTimers();vi.setSystemTime(new Date('2026-01-01T00:00:00Z'));
    vi.spyOn(OperatorOidc.prototype,'prepare').mockImplementation(async settings=>({settings,metadata:{issuer:settings.issuer}}));
    vi.spyOn(OperatorOidc.prototype,'signIn').mockImplementation(async()=>({token:'aaa.bbb.ccc',subject:'alice',environmentId:'env-a',expiresAt:Math.floor(Date.now()/1000)+2}));
    let finish!:(response:Response)=>void;const fetchMock=vi.spyOn(globalThis,'fetch');
    for(const mode of ['identity','audit','request'] as const){
      vi.setSystemTime(new Date('2026-01-01T00:00:00Z'));const fixture=TestBed.createComponent(App);const app=fixture.componentInstance;
      app.oidcIssuer='https://issuer.test';app.oidcClientId='console';app.oidcEnvironment='env-a';await app.prepareOidc();await app.signInOidc();app.setSection('settings');app.useOidcForPermissions();
      app.permissionIssuer='https://issuer.test';app.permissionSubject='alice';app.permissionClient='console';app.permissionRequestId='1889f88e-c0a3-4ab5-b861-2fdc1f26f254';
      fetchMock.mockReturnValueOnce(new Promise(done=>{finish=done;}));
      const pending=mode==='request'?app.lookupPermissionRequest():app.loadPermissions(mode);
      vi.setSystemTime(new Date('2026-01-01T00:00:03Z'));
      finish(new Response(JSON.stringify(mode==='identity'?{found:false}:mode==='audit'?{data:[],next:null}:{found:false})));await pending;
      expect(app.oidcStatus()).toBe('expired');expect(app.permissionKey).toBe('');expect(app.permissionStatus()).toBe('idle');expect(app.permissionRequestStatus()).toBe('idle');expect(app.permissionRequestId).toBe('1889f88e-c0a3-4ab5-b861-2fdc1f26f254');fixture.destroy();
    }
  });

  it('does not submit a permission change if the OIDC token expires during credential hashing', async () => {
    vi.useFakeTimers();vi.setSystemTime(new Date('2026-01-01T00:00:00Z'));
    vi.spyOn(OperatorOidc.prototype,'prepare').mockImplementation(async settings=>({settings,metadata:{issuer:settings.issuer}}));
    vi.spyOn(OperatorOidc.prototype,'signIn').mockResolvedValue({token:'aaa.bbb.ccc',subject:'alice',environmentId:'env-a',expiresAt:Math.floor(Date.now()/1000)+2});
    const fetchMock=vi.spyOn(globalThis,'fetch');const fixture=TestBed.createComponent(App);const app=fixture.componentInstance;
    app.oidcIssuer='https://issuer.test';app.oidcClientId='console';app.oidcEnvironment='env-a';await app.prepareOidc();await app.signInOidc();app.setSection('settings');app.useOidcForPermissions();
    app.permissionIssuer='https://issuer.test';app.permissionSubject='alice';app.permissionClient='console';app.permissionStatus.set('missing');app.permissionReason='ticket';app.permissionConfirmed=true;
    let finish!:(hash:string)=>void;const internal=app as unknown as {credentialFingerprint:(key:string)=>Promise<string>};vi.spyOn(internal,'credentialFingerprint').mockReturnValue(new Promise(done=>{finish=done;}));
    const pending=app.changePermissions();vi.setSystemTime(new Date('2026-01-01T00:00:03Z'));finish('hash');await pending;
    expect(fetchMock).not.toHaveBeenCalled();expect(app.permissionKey).toBe('');expect(app.permissionChangeStatus()).toBe('unavailable');expect(app.permissionChange()).toBeNull();fixture.destroy();
  });

  it('does not send an alert decision when OIDC expires during credential hashing', async () => {
    vi.useFakeTimers();vi.setSystemTime(new Date('2026-01-01T00:00:00Z'));
    vi.spyOn(OperatorOidc.prototype,'prepare').mockImplementation(async settings=>({settings,metadata:{issuer:settings.issuer}}));
    vi.spyOn(OperatorOidc.prototype,'signIn').mockResolvedValue({token:'aaa.bbb.ccc',subject:'alice',environmentId:'env-a',expiresAt:Math.floor(Date.now()/1000)+2});
    const fetchMock=vi.spyOn(globalThis,'fetch');const fixture=TestBed.createComponent(App);const app=fixture.componentInstance;
    app.oidcIssuer='https://issuer.test';app.oidcClientId='console';app.oidcEnvironment='env-a';await app.prepareOidc();await app.signInOidc();app.useOidc('alerts');
    app.selectedAlert.set({channel:'slack',targetId:'ops',state:'unknown',lastStartedAt:null,attemptId:'1889f88e-c0a3-4ab5-b861-2fdc1f26f254'});app.decisionReason='ticket';app.decisionConfirmed=true;app.useOidcForDecision('decision');
    let finish!:(hash:string)=>void;const internal=app as unknown as {credentialFingerprint:(key:string)=>Promise<string>};vi.spyOn(internal,'credentialFingerprint').mockReturnValue(new Promise(done=>{finish=done;}));
    const pending=app.resolveAlert();vi.setSystemTime(new Date('2026-01-01T00:00:03Z'));finish('hash');await pending;
    expect(fetchMock).not.toHaveBeenCalled();expect(app.decisionStatus()).toBe('unavailable');expect(app.decisionRequestId()).toBeNull();fixture.destroy();
  });

  it('retains the result and request ID of a permission change already sent before expiry', async () => {
    vi.useFakeTimers();vi.setSystemTime(new Date('2026-01-01T00:00:00Z'));
    vi.spyOn(OperatorOidc.prototype,'prepare').mockImplementation(async settings=>({settings,metadata:{issuer:settings.issuer}}));
    vi.spyOn(OperatorOidc.prototype,'signIn').mockResolvedValue({token:'aaa.bbb.ccc',subject:'alice',environmentId:'env-a',expiresAt:Math.floor(Date.now()/1000)+2});
    let finish!:(response:Response)=>void;vi.spyOn(globalThis,'fetch').mockReturnValue(new Promise(done=>{finish=done;}));
    const fixture=TestBed.createComponent(App);const app=fixture.componentInstance;app.oidcIssuer='https://issuer.test';app.oidcClientId='console';app.oidcEnvironment='env-a';await app.prepareOidc();await app.signInOidc();app.setSection('settings');app.useOidcForPermissions();
    app.permissionIssuer='https://issuer.test';app.permissionSubject='alice';app.permissionClient='console';app.permissionStatus.set('missing');app.permissionReason='ticket';app.permissionConfirmed=true;
    const internal=app as unknown as {credentialFingerprint:(key:string)=>Promise<string>};vi.spyOn(internal,'credentialFingerprint').mockResolvedValue('hash');
    const pending=app.changePermissions();await Promise.resolve();expect(finish).toBeDefined();const requestId=app.permissionChange()?.body.requestId;expect(requestId).toBeTruthy();
    vi.advanceTimersByTime(2000);expect(app.permissionKey).toBe('');finish(new Response(JSON.stringify({changed:true,replayed:false})));await pending;
    expect(app.permissionChangeStatus()).toBe('recorded');expect(app.permissionChange()?.body.requestId).toBe(requestId);fixture.destroy();
  });

  it('retains an uncertain alert decision without retrying after OIDC expires during hashing', async () => {
    vi.useFakeTimers();vi.setSystemTime(new Date('2026-01-01T00:00:00Z'));
    vi.spyOn(OperatorOidc.prototype,'prepare').mockImplementation(async settings=>({settings,metadata:{issuer:settings.issuer}}));
    vi.spyOn(OperatorOidc.prototype,'signIn').mockResolvedValue({token:'aaa.bbb.ccc',subject:'alice',environmentId:'env-a',expiresAt:Math.floor(Date.now()/1000)+2});
    const fetchMock=vi.spyOn(globalThis,'fetch');const fixture=TestBed.createComponent(App);const app=fixture.componentInstance;app.oidcIssuer='https://issuer.test';app.oidcClientId='console';app.oidcEnvironment='env-a';await app.prepareOidc();await app.signInOidc();app.useOidc('alerts');
    const id='1889f88e-c0a3-4ab5-b861-2fdc1f26f254';app.decisionStatus.set('unknown');app.decisionRequestId.set(id);app.retrySnapshot.set({origin:'http://127.0.0.1:3000',endpoint:'/retry',scheme:'Bearer',requestId:id,channel:'slack',targetId:'ops',attemptId:id,decision:'retry',reason:'ticket',keyHash:'hash'});app.useOidcForDecision('retry');app.retryConfirmed=true;
    let finish!:(hash:string)=>void;const internal=app as unknown as {credentialFingerprint:(key:string)=>Promise<string>};vi.spyOn(internal,'credentialFingerprint').mockReturnValue(new Promise(done=>{finish=done;}));
    const pending=app.retryDecision();vi.setSystemTime(new Date('2026-01-01T00:00:03Z'));finish('hash');await pending;
    expect(fetchMock).not.toHaveBeenCalled();expect(app.decisionStatus()).toBe('unknown');expect(app.decisionRequestId()).toBe(id);expect(app.retrySnapshot()?.requestId).toBe(id);fixture.destroy();
  });

  it('checks expiry before applying a session even if the browser delayed its timer', async () => {
    vi.useFakeTimers();vi.setSystemTime(new Date('2026-01-01T00:00:00Z'));
    vi.spyOn(OperatorOidc.prototype,'prepare').mockImplementation(async settings=>({settings,metadata:{issuer:settings.issuer}}));
    vi.spyOn(OperatorOidc.prototype,'signIn').mockResolvedValue({token:'aaa.bbb.ccc',subject:'alice',environmentId:'env-a',expiresAt:Math.floor(Date.now()/1000)+2});
    const fixture=TestBed.createComponent(App);const app=fixture.componentInstance;
    app.oidcIssuer='https://issuer.test';app.oidcClientId='console';app.oidcEnvironment='env-a';
    await app.prepareOidc();await app.signInOidc();vi.setSystemTime(new Date('2026-01-01T00:00:03Z'));
    app.useOidc('subscribers');expect(app.oidcStatus()).toBe('expired');expect(app.apiKey).toBe('');
    fixture.destroy();
  });

  it('uses an operator session only after login and removes it on local logout', async () => {
    vi.spyOn(OperatorOidc.prototype,'prepare').mockImplementation(async settings=>({settings,metadata:{issuer:settings.issuer}}));
    vi.spyOn(OperatorOidc.prototype,'signIn').mockResolvedValue({token:'aaa.bbb.ccc',subject:'alice',environmentId:'env-a',expiresAt:Math.floor(Date.now()/1000)+600});
    const app=TestBed.createComponent(App).componentInstance;
    app.oidcIssuer='https://issuer.test'; app.oidcClientId='console'; app.oidcEnvironment='env-a';
    await app.prepareOidc(); expect(app.oidcStatus()).toBe('ready');
    await app.signInOidc(); expect(app.oidcStatus()).toBe('signed-in'); expect(app.oidcSubject()).toBe('alice');
    app.useOidc('subscribers'); expect(app.credentialMode).toBe('jwt'); expect(app.apiKey).toBe('aaa.bbb.ccc');
    app.useOidc('alerts'); expect(app.alertAuthMode).toBe('jwt'); expect(app.alertKey).toBe('aaa.bbb.ccc');
    app.resetOidc(); expect(app.alertKey).toBe(''); expect(app.oidcStatus()).toBe('idle');
    app.useOidc('alerts'); expect(app.alertKey).toBe('');
  });

  it('ignores a login result after settings or API address change', async () => {
    vi.spyOn(OperatorOidc.prototype,'prepare').mockImplementation(async settings=>({settings,metadata:{issuer:settings.issuer}}));
    let finish!: (session:{token:string;subject:string;environmentId:string;expiresAt:number})=>void;
    vi.spyOn(OperatorOidc.prototype,'signIn').mockReturnValue(new Promise(done=>{finish=done;}));
    const app=TestBed.createComponent(App).componentInstance;
    app.oidcIssuer='https://issuer.test';app.oidcClientId='console';app.oidcEnvironment='env-a';
    await app.prepareOidc();const pending=app.signInOidc();app.connectionChanged();
    finish({token:'aaa.bbb.ccc',subject:'alice',environmentId:'env-a',expiresAt:Math.floor(Date.now()/1000)+600});await pending;
    expect(app.oidcStatus()).toBe('idle');expect(app.oidcSubject()).toBe('');app.useOidc('alerts');expect(app.alertKey).toBe('');
  });

  it('loads subscriber pages with an operator JWT and an explicit environment', async () => {
    const fetchMock=vi.spyOn(globalThis,'fetch').mockResolvedValue(new Response(JSON.stringify({data:[{subscriberId:'alice',firstName:'Alice',email:'alice@example.test'}],next:'alice',previous:null}),{status:200}));
    const app=TestBed.createComponent(App).componentInstance;
    app.setSection('subscribers'); app.credentialMode='jwt'; app.apiKey='aaa.bbb.ccc'; app.subscriberEnvironment='env / one';
    await app.loadSubscribers();
    expect(app.subscribers()[0].id).toBe('alice');
    const url=new URL(fetchMock.mock.calls[0][0] as string);
    expect(url.pathname).toBe('/v1/experimental/operator/subscribers'); expect(url.searchParams.get('environmentId')).toBe('env / one');
    expect(fetchMock.mock.calls[0][1]).toEqual(expect.objectContaining({headers:{Authorization:'Bearer aaa.bbb.ccc'},credentials:'omit',redirect:'error'}));
    await app.loadSubscribers('after');
    expect(new URL(fetchMock.mock.calls[1][0] as string).searchParams.get('after')).toBe('alice');
    app.setSection('overview'); expect(app.apiKey).toBe(''); expect(app.subscribers()).toEqual([]);
  });

  it('uses JWT alert permissions for decisions and preserves the exact uncertain request', async () => {
    const attempt='1889f88e-c0a3-4ab5-b861-2fdc1f26f254';
    const fetchMock=vi.spyOn(globalThis,'fetch').mockResolvedValueOnce(new Response(JSON.stringify({data:[{channel:'slack',targetId:'ops',state:'unknown',lastStartedAt:null,attemptId:attempt}],hasMore:false,next:null}),{status:200})).mockRejectedValueOnce(new TypeError('network')).mockResolvedValueOnce(new Response(JSON.stringify({recorded:true,replayed:true}),{status:200}));
    const app=TestBed.createComponent(App).componentInstance;
    app.setSection('alerts'); app.alertAuthMode='jwt'; app.alertEnvironment='env-a'; app.alertKey='aaa.bbb.ccc';
    await app.loadAlerts();
    expect(fetchMock.mock.calls[0][0]).toBe('http://127.0.0.1:3000/v1/experimental/operator/alerts?environmentId=env-a');
    app.selectAlert(app.alertSlots()[0]); app.decisionProtocol='legacy'; app.decisionKey='aaa.bbb.ccc'; app.decisionReason='verified'; app.decisionConfirmed=true;
    await app.resolveAlert();
    expect(app.decisionStatus()).toBe('unknown');
    expect(fetchMock.mock.calls[1][0]).toBe('http://127.0.0.1:3000/v1/experimental/operator/alerts/resolve?environmentId=env-a');
    expect(JSON.parse(fetchMock.mock.calls[1][1]?.body as string).requestId).toBe(app.decisionRequestId());
    expect(fetchMock.mock.calls[1][1]?.headers).toEqual({Authorization:'Bearer aaa.bbb.ccc','Content-Type':'application/json'});
    app.retryKey='aaa.bbb.ccc'; app.retryConfirmed=true; await app.retryDecision();
    expect(fetchMock.mock.calls[2][0]).toBe(fetchMock.mock.calls[1][0]); expect(fetchMock.mock.calls[2][1]?.body).toBe(fetchMock.mock.calls[1][1]?.body);
    expect(app.decisionStatus()).toBe('recorded'); expect(app.retryKey).toBe('');
    fetchMock.mockResolvedValueOnce(new Response(JSON.stringify({found:false}),{status:200}));
    app.lookupKey='ddd.eee.fff'; app.lookupId=app.decisionRequestId()!; await app.lookupDecisionRequest();
    const url=new URL(fetchMock.mock.calls[3][0] as string); expect(url.pathname).toBe('/v1/experimental/operator/alerts/request'); expect(url.searchParams.get('environmentId')).toBe('env-a');
    expect(fetchMock.mock.calls[3][1]?.headers).toEqual({Authorization:'Bearer ddd.eee.fff'});
    app.setSection('overview'); expect(app.alertKey).toBe(''); expect(app.lookupKey).toBe('');
  });

  it('does not send a JWT without an environment or with malformed token bytes', async () => {
    const fetchMock=vi.spyOn(globalThis,'fetch'); const app=TestBed.createComponent(App).componentInstance;
    app.credentialMode='jwt'; app.apiKey='aaa.bbb.ccc'; app.subscriberEnvironment=''; await app.loadSubscribers();
    app.alertAuthMode='jwt'; app.alertEnvironment='env-a'; app.alertKey='bad token'; await app.loadAlerts();
    expect(fetchMock).not.toHaveBeenCalled(); expect(app.subscriberStatus()).toBe('error'); expect(app.alertStatus()).toBe('error');
  });

  it('shows unimplemented management without invented operational data', async () => {
    const fixture = TestBed.createComponent(App);
    await fixture.whenStable();
    expect(fixture.nativeElement.textContent).toContain('대부분의 관리 기능은 개발 중');
    expect(fixture.nativeElement.textContent).toContain('연결 확인 필요');
  });

  it('only reports ready for the API ready response', async () => {
    const fetchMock = vi.spyOn(globalThis, 'fetch').mockResolvedValueOnce(new Response(JSON.stringify({ status: 'ready' }), { status: 200, headers: { 'content-type': 'application/json' } }));
    const app = TestBed.createComponent(App).componentInstance;
    await app.checkConnection();
    expect(app.status()).toBe('ready');
    expect(fetchMock).toHaveBeenCalledWith('http://127.0.0.1:3000/health/ready', expect.objectContaining({ credentials: 'omit' }));
    fetchMock.mockResolvedValueOnce(new Response(JSON.stringify({ status: 'not_ready' }), { status: 503 }));
    await app.checkConnection();
    expect(app.status()).toBe('unavailable');
  });

  it('ignores a result after the API address has changed', async () => {
    let resolve!: (value: Response) => void;
    vi.spyOn(globalThis, 'fetch').mockReturnValueOnce(new Promise<Response>((done) => { resolve = done; }));
    const app = TestBed.createComponent(App).componentInstance;
    const pending = app.checkConnection();
    app.apiOrigin = 'https://another.example';
    app.connectionChanged();
    resolve(new Response(JSON.stringify({ status: 'ready' }), { status: 200 }));
    await pending;
    expect(app.status()).toBe('unchecked');
  });

  it('uses the separate read-only credential by default', async () => {
    const fetchMock = vi.spyOn(globalThis, 'fetch').mockResolvedValueOnce(new Response(JSON.stringify({data:[],next:null,previous:null}),{status:200}));
    const app = TestBed.createComponent(App).componentInstance;
    app.apiKey = 'a'.repeat(64);
    await app.loadSubscribers();
    expect(app.subscriberStatus()).toBe('loaded');
    expect(fetchMock).toHaveBeenCalledWith('http://127.0.0.1:3000/v1/experimental/console/subscribers?limit=20',expect.objectContaining({headers:{Authorization:`SwallowConsole ${'a'.repeat(64)}`}}));
  });

  it('loads only display fields and sends the API key without cookies or redirect following', async () => {
    const fetchMock = vi.spyOn(globalThis, 'fetch').mockResolvedValueOnce(new Response(JSON.stringify({data:[{subscriberId:'alice',firstName:'Alice',email:'a@example.test',channels:[{credentials:{deviceTokens:['secret']}}]}],next:'next',previous:null}),{status:200}));
    const app = TestBed.createComponent(App).componentInstance;
    app.credentialMode = 'api-key';
    app.apiKey = 'fixture-key';
    await app.loadSubscribers();
    expect(app.subscriberStatus()).toBe('loaded');
    expect(app.subscribers()).toEqual([{id:'alice',firstName:'Alice',lastName:'',email:'a@example.test'}]);
    expect(fetchMock).toHaveBeenCalledWith('http://127.0.0.1:3000/v2/subscribers?limit=20',expect.objectContaining({headers:{Authorization:'ApiKey fixture-key'},credentials:'omit',redirect:'error'}));
    app.forgetKey();
    expect(app.apiKey).toBe('');
    expect(app.subscribers()).toEqual([]);
  });

  it('requests the API cursors for next and previous pages', async () => {
    const fetchMock = vi.spyOn(globalThis, 'fetch')
      .mockResolvedValueOnce(new Response(JSON.stringify({data:[{subscriberId:'first'}],next:'cursor-next',previous:null}),{status:200}))
      .mockResolvedValueOnce(new Response(JSON.stringify({data:[{subscriberId:'second'}],next:null,previous:'cursor-prev'}),{status:200}))
      .mockResolvedValueOnce(new Response(JSON.stringify({data:[{subscriberId:'first'}],next:'cursor-next',previous:null}),{status:200}));
    const app = TestBed.createComponent(App).componentInstance;
    app.credentialMode = 'api-key';
    app.apiKey = 'fixture-key';
    await app.loadSubscribers();
    await app.loadSubscribers('after');
    expect(fetchMock.mock.calls[1][0]).toContain('after=cursor-next');
    expect(app.subscribers()[0].id).toBe('second');
    await app.loadSubscribers('before');
    expect(fetchMock.mock.calls[2][0]).toContain('before=cursor-prev');
    expect(app.subscribers()[0].id).toBe('first');
  });

  it('clears rows and reports authorization failures', async () => {
    vi.spyOn(globalThis, 'fetch').mockResolvedValueOnce(new Response('',{status:401}));
    const app = TestBed.createComponent(App).componentInstance;
    app.credentialMode = 'api-key';
    app.apiKey = 'fixture-key';
    await app.loadSubscribers();
    expect(app.subscriberStatus()).toBe('error');
    expect(app.subscriberError()).toContain('인증');
    expect(app.subscribers()).toEqual([]);
  });

  it('discards a subscriber response after the API key changes', async () => {
    let resolve!: (response: Response) => void;
    vi.spyOn(globalThis, 'fetch').mockReturnValueOnce(new Promise<Response>((done) => { resolve = done; }));
    const app = TestBed.createComponent(App).componentInstance;
    app.credentialMode = 'api-key';
    app.apiKey = 'first-key';
    const pending = app.loadSubscribers();
    app.apiKey = 'second-key';
    app.clearSubscribers();
    resolve(new Response(JSON.stringify({data:[{subscriberId:'old'}],next:null,previous:null}),{status:200}));
    await pending;
    expect(app.subscriberStatus()).toBe('idle');
    expect(app.subscribers()).toEqual([]);
  });

  it('loads alert slots only with a restricted credential and clears them on leaving', async () => {
    const fetchMock = vi.spyOn(globalThis, 'fetch').mockResolvedValueOnce(new Response(JSON.stringify({data:[{channel:'slack',targetId:'ops',state:'unknown',lastStartedAt:null}],hasMore:true,next:{channel:'slack',targetId:'ops'}}),{status:200}));
    const app = TestBed.createComponent(App).componentInstance;
    app.setSection('alerts');
    app.alertKey = 'a'.repeat(64);
    await app.loadAlerts();
    expect(app.alertSlots()).toEqual([{channel:'slack',targetId:'ops',state:'unknown',lastStartedAt:null}]);
    expect(app.alertsHaveMore()).toBe(true);
    expect(fetchMock).toHaveBeenCalledWith('http://127.0.0.1:3000/v1/experimental/console/alerts',expect.objectContaining({headers:{Authorization:`SwallowConsole ${'a'.repeat(64)}`},credentials:'omit',redirect:'error'}));
    app.setSection('overview');
    expect(app.alertKey).toBe('');
    expect(app.alertSlots()).toEqual([]);
  });

  it('shows attempt identifiers and rejects malformed ones', async () => {
    const id = '1889f88e-c0a3-4ab5-b861-2fdc1f26f254';
    const body = (attemptId: unknown) => ({data:[{channel:'slack',targetId:'ops',state:'unknown',lastStartedAt:null,attemptId}],hasMore:false,next:null});
    vi.spyOn(globalThis, 'fetch').mockResolvedValueOnce(new Response(JSON.stringify(body(id)),{status:200})).mockResolvedValueOnce(new Response(JSON.stringify(body('bad-id')),{status:200}));
    const fixture = TestBed.createComponent(App);
    const app = fixture.componentInstance;
    app.setSection('alerts');
    app.alertKey = 'a'.repeat(64);
    await app.loadAlerts();
    fixture.detectChanges();
    expect(app.alertSlots()[0].attemptId).toBe(id);
    expect(fixture.nativeElement.textContent).toContain(id);
    await app.loadAlerts();
    expect(app.alertStatus()).toBe('error');
    expect(app.alertSlots()).toEqual([]);
  });

  it('distinguishes absent attempts from an older API response', async () => {
    vi.spyOn(globalThis, 'fetch').mockResolvedValueOnce(new Response(JSON.stringify({data:[{channel:'email',targetId:'one',state:'idle',lastStartedAt:null,attemptId:null},{channel:'slack',targetId:'two',state:'unknown',lastStartedAt:null}],hasMore:false,next:null}),{status:200}));
    const fixture = TestBed.createComponent(App);
    const app = fixture.componentInstance;
    app.setSection('alerts');
    app.alertKey = 'a'.repeat(64);
    await app.loadAlerts();
    fixture.detectChanges();
    expect(app.alertSlots()[0].attemptId).toBeNull();
    expect(app.alertSlots()[1].attemptId).toBeUndefined();
    expect(fixture.nativeElement.textContent).toContain('API 업데이트 필요');
  });

  it('submits a separately authorized decision once and clears sensitive inputs', async () => {
    const id = '1889f88e-c0a3-4ab5-b861-2fdc1f26f254';
    const fetchMock = vi.spyOn(globalThis, 'fetch').mockResolvedValueOnce(new Response(JSON.stringify({data:[{channel:'slack',targetId:'ops',state:'unknown',lastStartedAt:null,attemptId:id}],hasMore:false,next:null}),{status:200}));
    let resolve!: (response: Response) => void;
    fetchMock.mockReturnValueOnce(new Promise<Response>(done => { resolve = done; }));
    const app = TestBed.createComponent(App).componentInstance;
    app.setSection('alerts'); app.alertKey = 'a'.repeat(64);
    await app.loadAlerts();
    app.selectAlert(app.alertSlots()[0]);
    app.decisionProtocol = 'legacy'; app.decisionKey = 'b'.repeat(64); app.decision = 'accepted'; app.decisionReason = 'ticket'; app.decisionConfirmed = true;
    const pending = app.resolveAlert();
    await app.resolveAlert();
    app.setSection('overview');
    expect(app.section()).toBe('alerts');
    expect(fetchMock).toHaveBeenCalledTimes(2);
    expect(fetchMock.mock.calls[1][0]).toBe('http://127.0.0.1:3000/v1/experimental/console/alerts/resolve');
    expect(fetchMock.mock.calls[1][1]).toEqual(expect.objectContaining({method:'POST',headers:{Authorization:`SwallowConsole ${'b'.repeat(64)}`,'Content-Type':'application/json'},credentials:'omit',redirect:'error',body:JSON.stringify({channel:'slack',targetId:'ops',attemptId:id,decision:'accepted',reason:'ticket'})}));
    resolve(new Response(JSON.stringify({recorded:true}),{status:200}));
    await pending;
    expect(app.decisionStatus()).toBe('recorded');
    expect(app.decisionKey).toBe(''); expect(app.decisionReason).toBe('');
    expect(app.alertSlots()).toEqual([]); expect(app.alertStatus()).toBe('idle');
  });

  it('requires evidence confirmation and treats an interrupted request as unknown', async () => {
    const id = '1889f88e-c0a3-4ab5-b861-2fdc1f26f254';
    const fetchMock = vi.spyOn(globalThis, 'fetch').mockResolvedValueOnce(new Response(JSON.stringify({data:[{channel:'email',targetId:'ops',state:'rejected',lastStartedAt:null,attemptId:id}],hasMore:false,next:null}),{status:200})).mockRejectedValueOnce(new TypeError('network'));
    const app = TestBed.createComponent(App).componentInstance;
    app.setSection('alerts'); app.alertKey = 'a'.repeat(64); await app.loadAlerts();
    app.selectAlert(app.alertSlots()[0]); app.decisionProtocol = 'legacy'; app.decisionKey = 'b'.repeat(64); app.decisionReason = 'ticket';
    await app.resolveAlert(); expect(fetchMock).toHaveBeenCalledTimes(1);
    app.decisionConfirmed = true; app.decision = 'accepted';
    await app.resolveAlert(); expect(fetchMock).toHaveBeenCalledTimes(1);
    app.decision = 'retry'; await app.resolveAlert();
    expect(fetchMock).toHaveBeenCalledTimes(2); expect(app.decisionStatus()).toBe('unknown');
    expect(app.decisionKey).toBe(''); expect(app.alertSlots()).toEqual([]);
    await app.resolveAlert(); expect(fetchMock).toHaveBeenCalledTimes(2);
  });

  it('uses a new request ID and can look up its committed result', async () => {
    const attempt = '1889f88e-c0a3-4ab5-b861-2fdc1f26f254';
    const fetchMock = vi.spyOn(globalThis, 'fetch').mockResolvedValueOnce(new Response(JSON.stringify({data:[{channel:'slack',targetId:'ops',state:'unknown',lastStartedAt:null,attemptId:attempt}],hasMore:false,next:null}),{status:200})).mockResolvedValueOnce(new Response(JSON.stringify({recorded:true,replayed:false}),{status:200}));
    const fixture = TestBed.createComponent(App);
    const app = fixture.componentInstance;
    app.setSection('alerts'); app.alertKey = 'a'.repeat(64); await app.loadAlerts();
    app.selectAlert(app.alertSlots()[0]); app.decisionKey = 'b'.repeat(64); app.decisionReason = 'verified'; app.decisionConfirmed = true;
    await app.resolveAlert();
    const id = app.decisionRequestId();
    expect(id).toMatch(/^[0-9a-f-]{36}$/);
    expect(fetchMock.mock.calls[1][0]).toBe('http://127.0.0.1:3000/v1/experimental/console/alerts/resolve-v2');
    expect(JSON.parse(fetchMock.mock.calls[1][1]?.body as string)).toEqual({channel:'slack',targetId:'ops',attemptId:attempt,decision:'retry',reason:'verified',requestId:id});
    expect(app.lookupId).toBe(id); expect(app.decisionKey).toBe('');
    fetchMock.mockResolvedValueOnce(new Response(JSON.stringify({found:true,requestId:id,channel:'slack',targetId:'ops',attemptId:attempt,decision:'retry',reason:'verified',createdAt:'2026-01-01T00:00:00Z'}),{status:200}));
    app.lookupKey = 'b'.repeat(64);
    await app.lookupDecisionRequest(); fixture.detectChanges();
    expect(app.lookupStatus()).toBe('found');
    expect(fixture.nativeElement.textContent).toContain('verified');
    expect(fetchMock.mock.calls[2][0]).toContain(`requestId=${id}`);
    app.setSection('overview'); expect(app.lookupKey).toBe(''); expect(app.lookupId).toBe(''); expect(app.decisionRequestId()).toBeNull();
  });

  it('preserves an ambiguous request ID without retrying and treats absence as inconclusive', async () => {
    const attempt='1889f88e-c0a3-4ab5-b861-2fdc1f26f254';
    const fetchMock=vi.spyOn(globalThis,'fetch').mockResolvedValueOnce(new Response(JSON.stringify({data:[{channel:'email',targetId:'ops',state:'unknown',lastStartedAt:null,attemptId:attempt}],hasMore:false,next:null}),{status:200})).mockRejectedValueOnce(new TypeError('network')).mockResolvedValueOnce(new Response(JSON.stringify({found:false}),{status:200}));
    const app=TestBed.createComponent(App).componentInstance;
    app.setSection('alerts'); app.alertKey='a'.repeat(64); await app.loadAlerts();
    app.selectAlert(app.alertSlots()[0]); app.decisionKey='b'.repeat(64); app.decisionReason='investigation'; app.decisionConfirmed=true;
    await app.resolveAlert();
    expect(app.decisionStatus()).toBe('unknown'); expect(app.lookupId).toBe(app.decisionRequestId());
    app.lookupKey='b'.repeat(64); await app.lookupDecisionRequest();
    expect(app.lookupStatus()).toBe('not-found'); expect(fetchMock).toHaveBeenCalledTimes(3);
  });

  it('replays only the identical ambiguous request with the original credential', async () => {
    const attempt='1889f88e-c0a3-4ab5-b861-2fdc1f26f254';
    const fetchMock=vi.spyOn(globalThis,'fetch').mockResolvedValueOnce(new Response(JSON.stringify({data:[{channel:'email',targetId:'ops',state:'unknown',lastStartedAt:null,attemptId:attempt}],hasMore:false,next:null}),{status:200})).mockRejectedValueOnce(new TypeError('network')).mockResolvedValueOnce(new Response(JSON.stringify({recorded:true,replayed:true}),{status:200}));
    const app=TestBed.createComponent(App).componentInstance;
    app.setSection('alerts'); app.alertKey='a'.repeat(64); await app.loadAlerts();
    app.selectAlert(app.alertSlots()[0]); app.decisionKey='b'.repeat(64); app.decisionReason='investigation'; app.decisionConfirmed=true;
    await app.resolveAlert();
    expect(app.decisionStatus()).toBe('unknown');
    app.retryKey='c'.repeat(64); app.retryConfirmed=true; await app.retryDecision();
    expect(fetchMock).toHaveBeenCalledTimes(2); expect(app.decisionStatus()).toBe('unknown'); expect(app.retryKey).toBe('');
    app.retryKey='b'.repeat(64); app.retryConfirmed=true; await app.retryDecision();
    expect(fetchMock).toHaveBeenCalledTimes(3);
    expect(fetchMock.mock.calls[2][1]?.body).toBe(fetchMock.mock.calls[1][1]?.body);
    expect(app.decisionStatus()).toBe('recorded'); expect(app.retrySnapshot()).toBeNull(); expect(app.retryKey).toBe('');
  });

  it('keeps an ambiguous result after a failed replay and clears it when leaving', async () => {
    const attempt='1889f88e-c0a3-4ab5-b861-2fdc1f26f254';
    const fetchMock=vi.spyOn(globalThis,'fetch').mockResolvedValueOnce(new Response(JSON.stringify({data:[{channel:'email',targetId:'ops',state:'unknown',lastStartedAt:null,attemptId:attempt}],hasMore:false,next:null}),{status:200})).mockRejectedValueOnce(new TypeError('network')).mockResolvedValueOnce(new Response('',{status:403}));
    const app=TestBed.createComponent(App).componentInstance;
    app.setSection('alerts'); app.alertKey='a'.repeat(64); await app.loadAlerts();
    app.selectAlert(app.alertSlots()[0]); app.decisionKey='b'.repeat(64); app.decisionReason='investigation'; app.decisionConfirmed=true;
    await app.resolveAlert(); app.retryKey='b'.repeat(64); app.retryConfirmed=true; await app.retryDecision();
    expect(fetchMock).toHaveBeenCalledTimes(3); expect(app.decisionStatus()).toBe('unknown'); expect(app.retrySnapshot()).not.toBeNull();
    app.setSection('overview'); expect(app.retrySnapshot()).toBeNull(); expect(app.retryKey).toBe('');
  });

  it('fails closed when credential fingerprinting is unavailable', async () => {
    const attempt='1889f88e-c0a3-4ab5-b861-2fdc1f26f254';
    const fetchMock=vi.spyOn(globalThis,'fetch').mockResolvedValueOnce(new Response(JSON.stringify({data:[{channel:'email',targetId:'ops',state:'unknown',lastStartedAt:null,attemptId:attempt}],hasMore:false,next:null}),{status:200}));
    vi.spyOn(globalThis.crypto.subtle,'digest').mockRejectedValueOnce(new Error('unavailable'));
    const app=TestBed.createComponent(App).componentInstance;
    app.setSection('alerts'); app.alertKey='a'.repeat(64); await app.loadAlerts();
    app.selectAlert(app.alertSlots()[0]); app.decisionKey='b'.repeat(64); app.decisionReason='investigation'; app.decisionConfirmed=true;
    await app.resolveAlert();
    expect(fetchMock).toHaveBeenCalledTimes(1); expect(app.decisionStatus()).toBe('unavailable');
    expect(app.decisionRequestId()).toBeNull(); expect(app.retrySnapshot()).toBeNull(); expect(app.decisionKey).toBe('');
  });

  it('does not issue concurrent retries or allow leaving during a retry', async () => {
    const attempt='1889f88e-c0a3-4ab5-b861-2fdc1f26f254';
    const fetchMock=vi.spyOn(globalThis,'fetch').mockResolvedValueOnce(new Response(JSON.stringify({data:[{channel:'email',targetId:'ops',state:'unknown',lastStartedAt:null,attemptId:attempt}],hasMore:false,next:null}),{status:200})).mockRejectedValueOnce(new TypeError('network'));
    let finish!: (response: Response) => void;
    fetchMock.mockReturnValueOnce(new Promise<Response>(done => { finish=done; }));
    const app=TestBed.createComponent(App).componentInstance;
    app.setSection('alerts'); app.alertKey='a'.repeat(64); await app.loadAlerts();
    app.selectAlert(app.alertSlots()[0]); app.decisionKey='b'.repeat(64); app.decisionReason='investigation'; app.decisionConfirmed=true;
    await app.resolveAlert(); app.retryKey='b'.repeat(64); app.retryConfirmed=true;
    const pending=app.retryDecision(); await app.retryDecision(); app.setSection('overview');
    expect(app.section()).toBe('alerts');
    await vi.waitFor(() => expect(fetchMock).toHaveBeenCalledTimes(3));
    finish(new Response(JSON.stringify({recorded:true,replayed:true}),{status:200})); await pending;
    expect(app.decisionStatus()).toBe('recorded');
  });

  it('does not downgrade or send when secure request IDs cannot be created', async () => {
    const attempt='1889f88e-c0a3-4ab5-b861-2fdc1f26f254';
    const fetchMock=vi.spyOn(globalThis,'fetch').mockResolvedValueOnce(new Response(JSON.stringify({data:[{channel:'email',targetId:'ops',state:'unknown',lastStartedAt:null,attemptId:attempt}],hasMore:false,next:null}),{status:200}));
    vi.spyOn(globalThis.crypto,'randomUUID').mockImplementation(() => { throw new Error('unavailable'); });
    const app=TestBed.createComponent(App).componentInstance;
    app.setSection('alerts'); app.alertKey='a'.repeat(64); await app.loadAlerts();
    app.selectAlert(app.alertSlots()[0]); app.decisionKey='b'.repeat(64); app.decisionReason='ticket'; app.decisionConfirmed=true;
    await app.resolveAlert();
    expect(app.decisionStatus()).toBe('unavailable'); expect(fetchMock).toHaveBeenCalledTimes(1); expect(app.decisionRequestId()).toBeNull();
  });

  it('ignores stale request lookups and clears their credentials', async () => {
    let resolve!: (response: Response) => void;
    vi.spyOn(globalThis,'fetch').mockReturnValueOnce(new Promise<Response>(done => { resolve=done; }));
    const app=TestBed.createComponent(App).componentInstance;
    app.setSection('alerts'); app.lookupKey='b'.repeat(64); app.lookupId='1889f88e-c0a3-4ab5-b861-2fdc1f26f254';
    const pending=app.lookupDecisionRequest();
    app.setSection('overview');
    resolve(new Response(JSON.stringify({found:false}),{status:200}));
    await pending;
    expect(app.lookupStatus()).toBe('idle'); expect(app.lookupKey).toBe(''); expect(app.lookupId).toBe('');
  });

  it('retrieves only a specified decision history and clears it on leaving', async () => {
    const attempt = '1889f88e-c0a3-4ab5-b861-2fdc1f26f254';
    const fetchMock = vi.spyOn(globalThis, 'fetch').mockResolvedValueOnce(new Response(JSON.stringify({data:[{id:'1',decision:'retry',reason:'reviewed',createdAt:'2026-01-01T00:00:00Z'}]}),{status:200}));
    const fixture = TestBed.createComponent(App);
    const app = fixture.componentInstance;
    app.setSection('alerts'); app.historyKey = 'b'.repeat(64); app.historyChannel = 'slack'; app.historyTarget = 'ops'; app.historyAttempt = attempt;
    await app.loadDecisionHistory(); fixture.detectChanges();
    expect(app.historyStatus()).toBe('loaded');
    expect(fixture.nativeElement.textContent).toContain('reviewed');
    expect(fetchMock.mock.calls[0][0]).toContain(`channel=slack&targetId=ops&attemptId=${attempt}`);
    expect(fetchMock.mock.calls[0][1]).toEqual(expect.objectContaining({headers:{Authorization:`SwallowConsole ${'b'.repeat(64)}`},credentials:'omit',redirect:'error'}));
    app.setSection('overview');
    expect(app.historyKey).toBe(''); expect(app.historyRows()).toEqual([]);
  });

  it('discards an obsolete history response', async () => {
    let resolve!: (response: Response) => void;
    vi.spyOn(globalThis, 'fetch').mockReturnValueOnce(new Promise<Response>(done => { resolve = done; }));
    const app = TestBed.createComponent(App).componentInstance;
    app.historyKey = 'b'.repeat(64); app.historyTarget = 'ops'; app.historyAttempt = '1889f88e-c0a3-4ab5-b861-2fdc1f26f254';
    const pending = app.loadDecisionHistory();
    app.clearHistory(true);
    resolve(new Response(JSON.stringify({data:[{id:'1',decision:'retry',reason:'old',createdAt:'2026-01-01T00:00:00Z'}]}),{status:200}));
    await pending;
    expect(app.historyStatus()).toBe('idle'); expect(app.historyRows()).toEqual([]); expect(app.historyKey).toBe('');
  });

  it('requests the next alert page and can return to the first', async () => {
    const first = {data:[{channel:'email',targetId:'ops',state:'accepted',lastStartedAt:null}],hasMore:true,next:{channel:'email',targetId:'ops'}};
    const second = {data:[{channel:'slack',targetId:'ops',state:'unknown',lastStartedAt:null}],hasMore:false,next:null};
    const fetchMock = vi.spyOn(globalThis, 'fetch').mockResolvedValueOnce(new Response(JSON.stringify(first),{status:200})).mockResolvedValueOnce(new Response(JSON.stringify(second),{status:200})).mockResolvedValueOnce(new Response(JSON.stringify(first),{status:200}));
    const app = TestBed.createComponent(App).componentInstance;
    app.alertKey = 'a'.repeat(64);
    await app.loadAlerts();
    await app.loadAlerts('next');
    expect(fetchMock.mock.calls[1][0]).toContain('afterChannel=email&afterTarget=ops');
    expect(app.alertPage()).toBe(2);
    expect(app.alertSlots()[0].channel).toBe('slack');
    await app.loadAlerts();
    expect(app.alertPage()).toBe(1);
    expect(fetchMock.mock.calls[2][0]).not.toContain('?');
  });

  it('returns to earlier alert pages using prior cursors', async () => {
    const pages = [
      {data:[{channel:'email',targetId:'one',state:'accepted',lastStartedAt:null}],hasMore:true,next:{channel:'email',targetId:'one'}},
      {data:[{channel:'email',targetId:'two',state:'accepted',lastStartedAt:null}],hasMore:true,next:{channel:'email',targetId:'two'}},
      {data:[{channel:'slack',targetId:'three',state:'unknown',lastStartedAt:null}],hasMore:false,next:null},
    ];
    const fetchMock = vi.spyOn(globalThis, 'fetch');
    for (const n of [0,1,2,1,0]) fetchMock.mockResolvedValueOnce(new Response(JSON.stringify(pages[n]),{status:200}));
    const app = TestBed.createComponent(App).componentInstance;
    app.alertKey = 'a'.repeat(64);
    await app.loadAlerts();
    await app.loadAlerts('next');
    await app.loadAlerts('next');
    await app.loadAlerts('previous');
    expect(app.alertPage()).toBe(2);
    expect(fetchMock.mock.calls[3][0]).toContain('afterChannel=email&afterTarget=one');
    await app.loadAlerts('previous');
    expect(app.alertPage()).toBe(1);
    expect(fetchMock.mock.calls[4][0]).not.toContain('?');
  });

  it('does not show an obsolete or unauthorized alert response', async () => {
    let resolve!: (response: Response) => void;
    const fetchMock = vi.spyOn(globalThis, 'fetch').mockReturnValueOnce(new Promise<Response>((done) => { resolve = done; }));
    const app = TestBed.createComponent(App).componentInstance;
    app.alertKey = 'a'.repeat(64);
    const pending = app.loadAlerts();
    app.forgetAlertKey();
    resolve(new Response(JSON.stringify({data:[{channel:'email',targetId:'old',state:'unknown',lastStartedAt:null}],hasMore:false,next:null}),{status:200}));
    await pending;
    expect(app.alertStatus()).toBe('idle');
    expect(app.alertSlots()).toEqual([]);
    fetchMock.mockResolvedValueOnce(new Response('',{status:401}));
    app.alertKey = 'b'.repeat(64);
    await app.loadAlerts();
    expect(app.alertStatus()).toBe('error');
    expect(app.alertError()).toContain('권한');
  });

  it('rejects insecure remote API addresses without sending a request', async () => {
    const fetchMock = vi.spyOn(globalThis, 'fetch');
    const app = TestBed.createComponent(App).componentInstance;
    app.apiOrigin = 'http://example.test';
    await app.checkConnection();
    expect(app.status()).toBe('unavailable');
    expect(fetchMock).not.toHaveBeenCalled();
  });
});
