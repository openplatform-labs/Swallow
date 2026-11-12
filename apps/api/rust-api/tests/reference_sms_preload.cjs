// Test-only interception before the Twilio SDK sends an external request.
const fs = require('fs');
const {createRequire} = require('module');
const providerRequire = createRequire('/app/packages/providers/dist/cjs/lib/sms/twilio/twilio.provider.js');
const RequestClient = providerRequire('twilio/lib/base/RequestClient');
RequestClient.prototype.request = async function(request) {
  if (!request.uri.includes('/Messages.json') || request.method !== 'post') throw new Error('unexpected Twilio request');
  const form = Object.fromEntries(Object.entries(request.data).map(([key,value]) => [key,Array.isArray(value) ? value.map(String) : String(value)]));
  fs.appendFileSync('/tmp/novu-sms-capture.jsonl', JSON.stringify(form)+'\n');
  if (request.data.To === '+15550000999') return {statusCode:429,body:{code:20429,message:'Too many requests',more_info:'https://example.test',status:429}};
  return {statusCode:201,body:{sid:'SM123',date_created:'Wed, 28 Oct 2026 10:00:00 +0000'}};
};
