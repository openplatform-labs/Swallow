// Bundle the checked-out original provider and its real BaseProvider transform.
const path=require('node:path'),os=require('node:os'),fs=require('node:fs');
const root=path.resolve(__dirname,'../../../..');
const esbuild=require(path.join(root,'apps/dashboard/swallow/node_modules/esbuild'));
(async()=>{
 const dir=fs.mkdtempSync(path.join(os.tmpdir(),'swallow-resend-reference-'));
 try {
  const outfile=path.join(dir,'provider.cjs');
  await esbuild.build({entryPoints:[path.join(root,'packages/providers/src/lib/email/resend/resend.provider.ts')],bundle:true,platform:'node',format:'cjs',outfile,plugins:[{name:'fixture',setup(build){
   build.onResolve({filter:/^(@novu\/(shared|stateless)|resend|svix)$/},args=>({path:args.path,namespace:'fixture'}));
   build.onLoad({filter:/.*/,namespace:'fixture'},args=>({loader:'js',contents:args.path==='resend'?'export class Resend { constructor(){this.emails={send:async body=>{globalThis.fixtureBody=body;return {data:{id:"fixture"}}}}} }':args.path==='svix'?'export class Webhook {}':args.path==='@novu/shared'?'export const EmailProviderIdEnum={Resend:"resend"}':'export const ChannelTypeEnum={EMAIL:"email"}; export const CheckIntegrationResponseEnum={}; export const EmailEventStatusEnum={};'}));
  }}]});
  const {ResendEmailProvider}=require(outfile);const provider=new ResendEmailProvider({apiKey:'fixture',from:'sender@example.test',senderName:'Sender'});
  await provider.sendMessage({to:['recipient@example.test','known@example.test'],cc:['known-cc@example.test'],subject:'template',html:'<p>template</p>',headers:{'X-Base':'known-step'}},{subject:'known step',from:'step@example.test',to:['known@example.test'],cc:['known-cc@example.test'],headers:{'X-Global':'known-global','X-Base':'known-step'},_passthrough:{body:{subject:'pass global',html:'',text:'literal {{name}}',from:'Pass Sender <pass@example.test>',replyTo:'reply@example.test',to:['extra@example.test'],bcc:['hidden@example.test'],tags:[{name:'campaign',value:'summer-1'}],headers:{'X-Pass':'global','X-Override':'step','X-Step':'step'}}}});
  process.stdout.write(JSON.stringify({subject:globalThis.fixtureBody.subject,html:globalThis.fixtureBody.html,text:globalThis.fixtureBody.text,from:globalThis.fixtureBody.from,replyTo:globalThis.fixtureBody.replyTo,to:globalThis.fixtureBody.to,cc:globalThis.fixtureBody.cc,bcc:globalThis.fixtureBody.bcc,headers:globalThis.fixtureBody.headers,tags:globalThis.fixtureBody.tags}));
 } finally {fs.rmSync(dir,{recursive:true,force:true});}
})().catch(error=>{process.stderr.write(String(error));process.exit(1)});
