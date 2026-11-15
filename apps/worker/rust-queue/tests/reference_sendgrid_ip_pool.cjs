// Bundle the checked-out original provider with only its runtime enums stubbed.
// SendGrid packages are installed separately at their pnpm-lock versions.
const path = require('node:path');
const root = process.env.NOVU_REFERENCE_SOURCE;
const deps = process.env.SENDGRID_REFERENCE_DEPS;
const esbuild = require(path.join(root, 'apps/dashboard/swallow/node_modules/esbuild'));
(async () => {
  const output = path.join(deps, 'novu-sendgrid-reference.cjs');
  await esbuild.build({
    entryPoints: [path.join(root, 'packages/providers/src/lib/email/sendgrid/sendgrid.provider.ts')],
    bundle: true, platform: 'node', format: 'cjs', outfile: output,
    external: ['@sendgrid/*'],
    plugins: [{name: 'test-enums', setup(build) {
      build.onResolve({filter: /^@novu\/(shared|stateless)$/}, args => ({path: args.path, namespace: 'test-enums'}));
      build.onLoad({filter: /.*/, namespace: 'test-enums'}, args => ({loader: 'js', contents:
        args.path.endsWith('shared') ? 'export const EmailProviderIdEnum={SendGrid:"sendgrid"}' :
        'export const ChannelTypeEnum={EMAIL:"email"}; export const CheckIntegrationResponseEnum={}; export const EmailEventStatusEnum={};'}));
    }}],
  });
  const {SendgridEmailProvider} = require(output);
  const cases = [
    [undefined, undefined, undefined], ['default-pool', undefined, undefined],
    ['default-pool', 'override-pool', undefined], ['default-pool', '', undefined],
    ['default-pool', 'channel-pool', 'provider-pool'], ['default-pool', 'channel-pool', ''],
  ];
  const results = [];
  for (const [defaultPool, optionPool, providerPool] of cases) {
    const provider = new SendgridEmailProvider({apiKey: 'SG.fixture', from: 'sender@example.test', senderName: 'Sender', ipPoolName: defaultPool});
    provider.client.request = async request => {results.push(request.body.ip_pool_name ?? null); return [{headers: {}}]};
    const options = {to: ['a@example.test'], subject: 'Hi', html: '<p>Hi</p>'};
    if (optionPool !== undefined) options.ipPoolName = optionPool;
    await provider.sendMessage(options, providerPool === undefined ? {} : {ipPoolName: providerPool});
  }
  process.stdout.write(JSON.stringify(results));
})().catch(error => {console.error(error); process.exit(1)});
