// Execute the pinned original sendMessage method with a mocked SDK and empty
// bridge data. This checks selected option mapping, not the real SDK wire body.
const fs = require('node:fs');
const path = require('node:path');
const {stripTypeScriptTypes} = require('node:module');
const root = path.resolve(__dirname, '../../../..');
const source = fs.readFileSync(path.join(root, 'packages/providers/src/lib/email/resend/resend.provider.ts'), 'utf8');
const begin = source.indexOf('  async sendMessage(');
const end = source.indexOf('  async checkIntegration(', begin);
if (begin < 0 || end < 0) throw Error('reference method not found');
const classSource = `class Original { constructor(config) { this.config=config; this.resendClient={emails:{send:async (body)=>{this.captured=body;return {data:{id:'mock-id'}}}}}; } transform(bridge, body) { if (Object.keys(bridge).length) throw Error('bridge is outside this test'); return {body}; } ${source.slice(begin,end)} }`;
const js = stripTypeScriptTypes(classSource, {mode:'transform'});
const Original = new Function('isResendError',`${js}; return Original;`)(()=>false);
const config = {apiKey:'fixture',from:'sender@example.test',senderName:'Sender'};
const cases = [
  {to:['a@example.test'],subject:'Hello',html:'<p>Hello</p>',replyTo:'reply@example.test'},
  {to:['a@example.test','b@example.test'],cc:['cc@example.test'],bcc:['bcc@example.test'],from:'alt@example.test',senderName:'Alias',subject:'Hi',html:'<b>Hi</b>',text:'Hi',replyTo:'reply@example.test'},
  {to:['a@example.test'],subject:'Plain',html:'Hi',replyTo:'reply@example.test',senderName:'No comma'},
];
(async()=>{
  const results=[];
  for(const options of cases){
    const provider=new Original(config); await provider.sendMessage(options);
    const o=provider.captured;
    const wire={from:o.from,to:o.to,subject:o.subject,html:o.html,reply_to:o.replyTo};
    for(const field of ['cc','bcc','text']) if(o[field] !== undefined) wire[field]=o[field];
    results.push(wire);
  }
  process.stdout.write(JSON.stringify(results));
})().catch(error=>{process.stderr.write(String(error));process.exit(1)});
