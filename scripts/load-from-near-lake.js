const { startStream } = require("near-lake-framework");

const lakeConfig = {
    s3BucketName: "near-lake-data-mainnet",
    s3RegionName: "eu-central-1",
    startBlockHeight: 63804051,
};

async function handleStreamerMessage(streamerMessage) {
    console.log(`Block #${streamerMessage.block.header.height} Shards: ${streamerMessage.shards.length}`);
    console.log('streamerMessage', streamerMessage, streamerMessage.shards[0]);
}

(async () => {
    await startStream(lakeConfig, handleStreamerMessage);
})();