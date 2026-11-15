// Capture the exact HTTP body produced by the pinned original provider.
const path=require('path');
async function loadProvider() {
  const root=process.env.NOVU_REFERENCE_SOURCE, deps=process.env.SENDGRID_REFERENCE_DEPS;
  if (!deps) return require(path.join(root,'packages/providers/dist/cjs/lib/email/sendgrid/sendgrid.provider.js')).SendgridEmailProvider;
  const esbuild=require(path.join(root,'apps/dashboard/swallow/node_modules/esbuild'));
  const output=path.join(deps,'novu-sendgrid-alternatives-reference.cjs');
  await esbuild.build({entryPoints:[path.join(root,'packages/providers/src/lib/email/sendgrid/sendgrid.provider.ts')],bundle:true,platform:'node',format:'cjs',outfile:output,external:['@sendgrid/*'],plugins:[{name:'test-enums',setup(build){
    build.onResolve({filter:/^@novu\/(shared|stateless)$/},args=>({path:args.path,namespace:'test-enums'}));
    build.onLoad({filter:/.*/,namespace:'test-enums'},args=>({loader:'js',contents:args.path.endsWith('shared') ? 'export const EmailProviderIdEnum={SendGrid:"sendgrid"}' : 'export const ChannelTypeEnum={EMAIL:"email"}; export const CheckIntegrationResponseEnum={}; export const EmailEventStatusEnum={};'}));
  }}]});
  return require(output).SendgridEmailProvider;
}
const config={apiKey:'SG.fixture',from:'sender@example.test',senderName:'Sender'};
const cases=[
  {to:['a@example.test'],subject:'Hi',html:'<p>Hi</p>'},
  {to:['a@example.test'],subject:'Hi',html:'<p>Hi</p>',id:'',notificationDetails:{transactionId:'',workflowIdentifier:'',subscriberId:''}},
  {to:['a@example.test'],subject:'Hi',html:'<p>Hi</p>',id:'message123',notificationDetails:{transactionId:'txn1',workflowIdentifier:'greeting',subscriberId:'sub1'}},
  {to:['a@example.test','b@example.test'],cc:['cc@example.test'],bcc:['bcc@example.test'],subject:'Hi',html:'<p>Hi</p>',from:'alt@example.test',senderName:'Alias',replyTo:'reply@example.test',headers:{'X-Test':'yes'},customData:{source:'example'},id:'m1',notificationDetails:{transactionId:'txn1',workflowIdentifier:'greeting',subscriberId:'sub1'}},
  {to:['a@example.test'],subject:'Hi',html:'<p>Hi</p>',customData:{templateId:'d-123',dynamicTemplateData:{name:'Lee'},source:'example'}},
  {to:['a@example.test'],subject:'Hi',html:'<p>Hi</p>',text:'Plain',alternatives:[{contentType:'text/calendar',content:'Meeting'}]},
  {to:['a@example.test'],subject:'Hi',html:'<p>Hi</p>',alternatives:[{contentType:'text/calendar; method=REQUEST',content:'BEGIN:VCALENDAR'}, {contentType:'text/x-amp-html',content:'<html amp4email></html>'}]},
  {to:['a@example.test'],subject:'Hi',html:'<p>Hi</p>',text:'',alternatives:[{contentType:'text/calendar',content:''}]},
  {to:['a@example.test'],subject:'Hi',html:'<p>Hi</p>',attachments:[{file:Buffer.from('hello'),name:'a.txt',mime:'text/plain'},{file:Buffer.from([0,1,255]),name:'img.png',mime:'image/png',cid:'img'},{file:Buffer.from('hi'),name:'x.txt',mime:'text/plain',cid:'x',disposition:'attachment'}]},
];
const trackingCases=[
  {trackingSettings:{clickTracking:{enable:true,enableText:false},openTracking:{enable:true,substitutionTag:'%open%'}}},
  {trackingSettings:{subscriptionTracking:{enable:true,text:'unsubscribe',html:'<p>unsubscribe</p>',substitutionTag:'%unsub%'},ganalytics:{enable:true,utmSource:'source',utmMedium:'email',utmTerm:'term',utmContent:'content',utmCampaign:'campaign'}}},
  {trackingSettings:{clickTracking:{enable:true,enableText:true},openTracking:{enable:true}},_passthrough:{body:{trackingSettings:{clickTracking:{enable:false},subscriptionTracking:{enable:true,text:'bye'}}}}},
];
(async()=>{const SendgridEmailProvider=await loadProvider();let bodies=[];for(const options of cases){const provider=new SendgridEmailProvider(config);provider.client.request=async request=>{bodies.push(request.body);return [{headers:{}}]};await provider.sendMessage(options)}for(const tracking of trackingCases){const provider=new SendgridEmailProvider(config);provider.client.request=async request=>{bodies.push(request.body);return [{headers:{}}]};await provider.sendMessage({to:['a@example.test'],subject:'Hi',html:'<p>Hi</p>'},tracking)}process.stdout.write(JSON.stringify(bodies))})().catch(error=>{console.error(error);process.exit(1)});
