// Execute the built pinned Novu provider and installed Resend SDK with fetch mocked.
const {ResendEmailProvider} = require('/ref/packages/providers/dist/cjs/lib/email/resend/resend.provider.js');
const config={apiKey:'re_fixture',from:'sender@example.test',senderName:'Sender'};
const cases=[
  {to:['a@example.test'],subject:'Hello',html:'<p>Hello</p>'},
  {to:['a@example.test','b@example.test'],cc:['cc@example.test'],bcc:['bcc@example.test'],from:'alt@example.test',senderName:'Alias',subject:'Hi',html:'<b>Hi</b>',text:'Hi',replyTo:'reply@example.test',headers:{'X-Trace':'value'}},
  {to:['a@example.test'],subject:'Attachment',html:'<img src="cid:logo">',replyTo:'reply@example.test',attachments:[{file:Buffer.from([0,1,255]),name:'image.png',mime:'image/png',cid:'logo'}]},
];
(async()=>{
  const captured=[];
  global.fetch=async (url,options)=>{if(url!=='https://api.resend.com/emails') throw Error('unexpected URL');captured.push(JSON.parse(options.body));return new Response(JSON.stringify({id:'test-id'}),{status:200,headers:{'content-type':'application/json'}})};
  for(const options of cases) await new ResendEmailProvider(config).sendMessage(options);
  process.stdout.write(JSON.stringify(captured));
})().catch(error=>{process.stderr.write(String(error));process.exit(1)});
