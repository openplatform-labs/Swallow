// Test-only interception of the original SDK; no external mail is sent.
const fs=require('node:fs');
const original=global.fetch;
global.fetch=async (url,options)=>{
  if(String(url)==='https://api.resend.com/emails') {
    fs.appendFileSync('/tmp/novu-resend-capture.jsonl',options.body+'\n');
    return new Response(JSON.stringify({id:'mock-resend-id'}),{status:200,headers:{'content-type':'application/json'}});
  }
  return original(url,options);
};
