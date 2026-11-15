import { TestBed } from '@angular/core/testing';
import { afterEach, describe, expect, it, vi } from 'vitest';
import { FcmIntegrations } from './fcm-integrations';
const row={internalId:'fixture-id',identifier:'push-main',active:false,createdAt:'2026-01-01T00:00:00Z'};
describe('FCM integration management',()=>{
  afterEach(()=>vi.restoreAllMocks());
  it('lists cursor pages without sending credentials to an unsafe origin',async()=>{
    const mock=vi.spyOn(globalThis,'fetch').mockResolvedValueOnce(new Response(JSON.stringify({data:[row],hasMore:true,next:'fixture-id'}))).mockResolvedValueOnce(new Response(JSON.stringify({data:[],hasMore:false,next:null})));
    const fixture=TestBed.createComponent(FcmIntegrations);const component=fixture.componentInstance;component.apiOrigin='https://api.example.test';component.key='fixture';
    await component.list();expect(component.rows()).toEqual([row]);await component.list(true);
    expect(mock.mock.calls[1][0]).toContain('?after=fixture-id');expect(mock.mock.calls[0][1]).toMatchObject({headers:{Authorization:'ApiKey fixture'},credentials:'omit',redirect:'error',cache:'no-store'});
    component.apiOrigin='http://remote.example.test';await component.list();expect(component.listStatus()).toBe('error');expect(mock).toHaveBeenCalledTimes(2);fixture.destroy();
  });
  it('creates and updates only after confirmation',async()=>{
    const mock=vi.spyOn(globalThis,'fetch').mockImplementation(async()=>new Response(JSON.stringify({data:row}),{status:201}));
    const fixture=TestBed.createComponent(FcmIntegrations);const component=fixture.componentInstance;component.apiOrigin='https://api.example.test';component.key='fixture';component.identifier='push-main';
    await component.save('create');expect(mock).not.toHaveBeenCalled();component.confirmed=true;await component.save('create');expect(component.writeStatus()).toBe('saved');expect(mock.mock.calls[0][1]).toMatchObject({method:'POST',body:JSON.stringify({identifier:'push-main',active:false})});
    component.confirmed=true;await component.save('update');expect(mock.mock.calls[1][0]).toBe('https://api.example.test/v1/experimental/fcm-integrations/push-main');expect(mock.mock.calls[1][1]).toMatchObject({method:'PATCH',body:'{"active":false}'});expect(component.writeStatus()).toBe('saved');fixture.destroy();
  });
  it('marks lost responses unknown, blocks resubmission, and clears secrets on destruction',async()=>{
    const mock=vi.spyOn(globalThis,'fetch').mockRejectedValue(new Error('network'));const fixture=TestBed.createComponent(FcmIntegrations);const component=fixture.componentInstance;
    component.apiOrigin='https://api.example.test';component.key='fixture';component.identifier='push-main';component.confirmed=true;await component.save('create');
    expect(component.writeStatus()).toBe('unknown');component.confirmed=true;await component.save('create');expect(mock).toHaveBeenCalledOnce();fixture.destroy();expect(component.key).toBe('');
  });
  it('discards a late list response after the key changes',async()=>{
    let resolve!:(response:Response)=>void;vi.spyOn(globalThis,'fetch').mockImplementation(()=>new Promise(r=>resolve=r));
    const fixture=TestBed.createComponent(FcmIntegrations);const component=fixture.componentInstance;component.apiOrigin='https://api.example.test';component.key='old';const pending=component.list();component.key='new';component.clear();resolve(new Response(JSON.stringify({data:[row],hasMore:false,next:null})));await pending;expect(component.rows()).toEqual([]);expect(component.listStatus()).toBe('idle');fixture.destroy();
  });
});
