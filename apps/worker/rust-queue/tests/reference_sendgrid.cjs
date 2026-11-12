// Capture the exact HTTP body produced by the pinned original provider.
const path=require('path');
const {SendgridEmailProvider}=require(path.join(process.env.NOVU_REFERENCE_SOURCE,'packages/providers/dist/cjs/lib/email/sendgrid/sendgrid.provider.js'));
const config={apiKey:'SG.fixture',from:'sender@example.test',senderName:'Sender'};
const cases=[
  {to:['a@example.test'],subject:'Hi',html:'<p>Hi</p>'},
  {to:['a@example.test'],subject:'Hi',html:'<p>Hi</p>',id:'',notificationDetails:{transactionId:'',workflowIdentifier:'',subscriberId:''}},
  {to:['a@example.test'],subject:'Hi',html:'<p>Hi</p>',id:'message123',notificationDetails:{transactionId:'txn1',workflowIdentifier:'greeting',subscriberId:'sub1'}},
  {to:['a@example.test','b@example.test'],cc:['cc@example.test'],bcc:['bcc@example.test'],subject:'Hi',html:'<p>Hi</p>',from:'alt@example.test',senderName:'Alias',replyTo:'reply@example.test',headers:{'X-Test':'yes'},customData:{source:'example'},id:'m1',notificationDetails:{transactionId:'txn1',workflowIdentifier:'greeting',subscriberId:'sub1'}},
  {to:['a@example.test'],subject:'Hi',html:'<p>Hi</p>',customData:{templateId:'d-123',dynamicTemplateData:{name:'Lee'},source:'example'}},
  {to:['a@example.test'],subject:'Hi',html:'<p>Hi</p>',text:'Plain',alternatives:[{contentType:'text/calendar',content:'Meeting'}]},
  {to:['a@example.test'],subject:'Hi',html:'<p>Hi</p>',attachments:[{file:Buffer.from('hello'),name:'a.txt',mime:'text/plain'},{file:Buffer.from([0,1,255]),name:'img.png',mime:'image/png',cid:'img'},{file:Buffer.from('hi'),name:'x.txt',mime:'text/plain',cid:'x',disposition:'attachment'}]},
];
(async()=>{let bodies=[];for(const options of cases){const provider=new SendgridEmailProvider(config);provider.client.request=async request=>{bodies.push(request.body);return [{headers:{}}]};await provider.sendMessage(options)}process.stdout.write(JSON.stringify(bodies))})().catch(error=>{console.error(error);process.exit(1)});
