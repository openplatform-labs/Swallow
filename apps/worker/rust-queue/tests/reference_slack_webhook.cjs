const path = require('path');
const root = process.env.NOVU_REFERENCE_SOURCE;
const safe = require(path.join(root, 'packages/providers/dist/cjs/utils/safe-chat-webhook-request.js'));
let captured;
safe.safeChatWebhookJsonRequest = async opts => { captured=opts; return {body:'ok',headers:{'x-slack-req-id':'fixture-id'}}; };
const {SlackProvider}=require(path.join(root,'packages/providers/dist/cjs/lib/chat/slack/slack.provider.js'));
(async()=>{const provider=new SlackProvider();const receipt=await provider.sendMessage({channelData:{type:'webhook',identifier:'fixture',endpoint:{url:'https://hooks.slack.com/services/A/B/C'}},content:'hello',nativePayload:{blocks:[{type:'section',text:{type:'mrkdwn',text:'hello'}}]},customData:{thread_ts:'1.2',unfurl_links:false}});process.stdout.write(JSON.stringify({body:captured.body,id:receipt.id}));})().catch(e=>{console.error(e);process.exit(1)});
