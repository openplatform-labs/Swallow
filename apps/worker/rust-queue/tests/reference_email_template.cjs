// Compare the actual original email compiler in custom HTML mode without a layout.
const path = require('path');
const root = path.join(process.env.NOVU_REFERENCE_SOURCE, 'libs/application-generic/build/main/usecases');
const { CompileEmailTemplate } = require(path.join(root, 'compile-email-template/compile-email-template.usecase.js'));
const { CompileTemplate } = require(path.join(root, 'compile-template/compile-template.usecase.js'));
const compiler = new CompileEmailTemplate(new CompileTemplate(), { findById: async () => ({branding:{}}) }, null, null, null);
const cases = [
  {subject:' Hello {{subscriber.firstName}} ',content:'<p>{{name}}</p>',payload:{name:'<Alice>',subscriber:{subscriberId:'s',firstName:'Jane'}}},
  {subject:'{{actor.firstName}}: {{subscriber.subscriberId}}',content:' {{context.tenant.data.plan}} & {{name}} ',payload:{name:'A&B',subscriber:{subscriberId:'s'},actor:{firstName:'Sam'},context:{tenant:{data:{plan:'gold'}}}}},
  {subject:'{{missing}}',content:'{{subscriber.email}} {{branding.color}}',payload:{subscriber:{email:'a@example.test'}}},
];
(async () => { const result=[]; for (const c of cases) {const v=await compiler.execute({...c,organizationId:'fixture',contentType:'customHtml'}); result.push({subject:v.subject,html:v.html});} process.stdout.write(JSON.stringify(result)); })().catch(e=>{console.error(e);process.exit(1)});
