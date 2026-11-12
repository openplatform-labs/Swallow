// Disposable test only: intercept the original provider before any network call.
const fs = require('fs');
const {createRequire} = require('module');
const providerRequire = createRequire('/app/packages/providers/dist/cjs/lib/email/sendgrid/sendgrid.provider.js');
const {Client} = providerRequire('@sendgrid/client');
Client.prototype.request = async function(request) {
  if (request.url !== '/v3/mail/send' || request.method !== 'POST') throw new Error('unexpected SendGrid request');
  fs.appendFileSync('/tmp/novu-email-capture.jsonl', JSON.stringify(request.body)+'\n');
  if (request.body.personalizations[0].to[0].email === 'reject@example.test') {
    const clientRequire = createRequire(providerRequire.resolve('@sendgrid/client'));
    const {ResponseError} = clientRequire('@sendgrid/helpers').classes;
    throw new ResponseError({status:429,statusText:'Too Many Requests',headers:{},data:{errors:[{message:'rate limit'}]}});
  }
  return [{statusCode:202,headers:{'x-message-id':'mock-id'}},{}];
};
