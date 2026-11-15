import { TestBed } from '@angular/core/testing';
import { afterEach, describe, expect, it, vi } from 'vitest';
import { SlackIntegrations } from './slack-integrations';
describe('Slack integration management',()=>{
  afterEach(()=>vi.restoreAllMocks());
  it('lists integrations and paginates connection identifiers',async()=>{
    const mock=vi.spyOn(globalThis,'fetch').mockResolvedValueOnce(new Response(JSON.stringify({data:['team'],hasMore:false,next:null}))).mockResolvedValueOnce(new Response(JSON.stringify({data:['first'],hasMore:true,next:'first'}))).mockResolvedValueOnce(new Response(JSON.stringify({data:['second'],hasMore:false,next:null})));
    const fixture=TestBed.createComponent(SlackIntegrations);const component=fixture.componentInstance;component.apiOrigin='https://api.example.test';component.key='fixture';component.integration='team';
    await component.list('integrations');expect(component.items()).toEqual(['team']);await component.list('connections');await component.list('connections',true);
    expect(mock.mock.calls[2][0]).toBe('https://api.example.test/v1/experimental/slack-integrations/team/connections?after=first');expect(mock.mock.calls[0][1]).toMatchObject({headers:{Authorization:'ApiKey fixture'},credentials:'omit',redirect:'error',cache:'no-store'});fixture.destroy();
  });
  it('requires confirmation and sends connection creation and deletion contracts',async()=>{
    const mock=vi.spyOn(globalThis,'fetch').mockImplementation(async()=>new Response(JSON.stringify({data:{integrationIdentifier:'team',connectionIdentifier:'main'}}),{status:200}));
    const fixture=TestBed.createComponent(SlackIntegrations);const component=fixture.componentInstance;component.apiOrigin='https://api.example.test';component.key='fixture';component.integration='team';component.connection='main';component.operation='create-connection';
    await component.save();expect(mock).not.toHaveBeenCalled();component.confirmed=true;await component.save();expect(component.writeStatus()).toBe('saved');expect(mock.mock.calls[0][1]).toMatchObject({method:'POST',body:'{"identifier":"main"}'});
    component.operation='delete-connection';component.confirmed=true;await component.save();expect(mock.mock.calls[1][0]).toBe('https://api.example.test/v1/experimental/slack-integrations/team/connections/main');expect(mock.mock.calls[1][1]).toMatchObject({method:'DELETE'});expect(component.writeStatus()).toBe('saved');fixture.destroy();
  });
  it('paginates audit and clears records on denied access',async()=>{
    const row={id:'10000000-0000-4000-8000-000000000001',integrationIdentifier:'team',connectionIdentifier:'main',action:'create',userId:'user',recordedAt:'2026-01-01'};
    const mock=vi.spyOn(globalThis,'fetch').mockResolvedValueOnce(new Response(JSON.stringify({data:[row],nextCursor:row.id}))).mockResolvedValueOnce(new Response(JSON.stringify({data:[],nextCursor:null}))).mockResolvedValueOnce(new Response('',{status:403}));
    const fixture=TestBed.createComponent(SlackIntegrations);const component=fixture.componentInstance;component.apiOrigin='https://api.example.test';component.key='fixture';await component.audit();expect(component.audits()).toEqual([row]);await component.audit(true);expect(mock.mock.calls[1][0]).toContain(`?after=${row.id}`);await component.audit();expect(component.auditStatus()).toBe('error');expect(component.audits()).toEqual([]);fixture.destroy();
  });
  it('ignores an audit response after credentials are cleared',async()=>{
    let resolve!:(response:Response)=>void;vi.spyOn(globalThis,'fetch').mockImplementation(()=>new Promise(r=>resolve=r));const fixture=TestBed.createComponent(SlackIntegrations);const component=fixture.componentInstance;component.apiOrigin='https://api.example.test';component.key='fixture';const pending=component.audit();component.clear(true);resolve(new Response(JSON.stringify({data:[],nextCursor:null})));await pending;expect(component.auditStatus()).toBe('idle');expect(component.key).toBe('');fixture.destroy();
  });
  it('blocks resubmission after a lost response and rejects unsafe origins',async()=>{
    const mock=vi.spyOn(globalThis,'fetch').mockRejectedValue(new Error('network'));const fixture=TestBed.createComponent(SlackIntegrations);const component=fixture.componentInstance;component.apiOrigin='http://remote.example.test';component.key='fixture';component.integration='team';component.confirmed=true;await component.save();expect(mock).not.toHaveBeenCalled();
    component.apiOrigin='https://api.example.test';component.confirmed=true;await component.save();expect(component.writeStatus()).toBe('unknown');component.confirmed=true;await component.save();expect(mock).toHaveBeenCalledOnce();fixture.destroy();expect(component.key).toBe('');
  });
  it('ignores late results after credentials change',async()=>{
    let resolve!:(response:Response)=>void;vi.spyOn(globalThis,'fetch').mockImplementation(()=>new Promise(r=>resolve=r));const fixture=TestBed.createComponent(SlackIntegrations);const component=fixture.componentInstance;component.apiOrigin='https://api.example.test';component.key='old';const pending=component.list('integrations');component.key='new';component.clear();resolve(new Response(JSON.stringify({data:['old'],hasMore:false,next:null})));await pending;expect(component.items()).toEqual([]);expect(component.listStatus()).toBe('idle');fixture.destroy();
  });
});
