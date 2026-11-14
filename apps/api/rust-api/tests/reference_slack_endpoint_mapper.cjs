const path = require('path');
const mapper = require(path.join(process.env.NOVU_REFERENCE_SOURCE,'apps/api/dist/app/channel-endpoints/dtos/dto.mapper.js'));
const result=mapper.mapChannelEndpointEntityToDto({identifier:'endpoint',channel:'chat',providerId:'slack',integrationIdentifier:'main',connectionIdentifier:'connection',subscriberId:'alice',contextKeys:[],type:'slack_channel',endpoint:{channelId:'C123'},createdAt:'fixture',updatedAt:'fixture'});
delete result.createdAt;delete result.updatedAt;
process.stdout.write(JSON.stringify(result));
