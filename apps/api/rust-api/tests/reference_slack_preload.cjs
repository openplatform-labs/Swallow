// Disposable reference worker: intercept Slack SDK HTTP without external delivery.
const fs=require('node:fs');
const {SlackProvider}=require('/app/packages/providers/dist/cjs/lib/chat/slack/slack.provider.js');
const original=SlackProvider.prototype.sendMessage;
SlackProvider.prototype.sendMessage=async function(data,...rest){
  this.axiosInstance.post=async (url,body,config)=>{
    if(url!=='https://slack.com/api/chat.postMessage' || config.headers.Authorization!=='Bearer fixture-token') throw new Error('unexpected fixture Slack request');
    fs.appendFileSync('/tmp/novu-slack-capture.jsonl',JSON.stringify(body)+'\n');
    return {data:{ok:true,channel:body.channel,ts:'1.2'}};
  };
  return original.call(this,data,...rest);
};
