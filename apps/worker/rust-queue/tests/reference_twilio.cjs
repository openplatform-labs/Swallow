const path = require('path');
const { TwilioSmsProvider } = require(path.join(process.env.NOVU_REFERENCE_SOURCE, 'packages/providers/dist/cjs/lib/sms/twilio/twilio.provider.js'));
const accountSid = 'AC00000000000000000000000000000001';
const cases = [
  [{accountSid, authToken:'fixture', from:'+15550000001'}, {to:'+15550000002', content:'Hello & 한글'}],
  [{accountSid, authToken:'fixture', from:'+15550000001'}, {to:'+15550000002', content:'line 1\nline 2', from:'+15550000003'}],
  [{accountSid, apiKeySid:'SK00000000000000000000000000000002', apiKeySecret:'fixture', from:'+15550000001', region:'eu'}, {to:'+15550000002', content:'Hello'}],
];
(async () => {
  const results = [];
  for (const [config, message] of cases) {
    const provider = new TwilioSmsProvider(config);
    provider.twilioClient.httpClient.request = async request => {
      results.push({uri:request.uri, data:request.data, username:request.username, password:request.password});
      return {statusCode:201, body:{sid:'SM123', date_created:'Wed, 28 Oct 2026 10:00:00 +0000'}};
    };
    const receipt = await provider.sendMessage(message);
    results[results.length - 1].receipt = receipt;
  }
  process.stdout.write(JSON.stringify(results));
})().catch(error => { console.error(error); process.exit(1); });
