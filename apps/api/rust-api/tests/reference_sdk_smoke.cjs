/* Use the reference-built @novu/js against a disposable Rust API.
 * Requires NOVU_REFERENCE_JS_PATH (absolute CJS bundle path), NOVU_RUST_URL,
 * NOVU_TEST_APP_IDENTIFIER and NOVU_TEST_API_KEY. Creates one subscriber.
 * Does not exercise real-time delivery, preferences or channel integrations. */
const { createHmac, randomUUID } = require('crypto');
const { Novu } = require(process.env.NOVU_REFERENCE_JS_PATH);

(async () => {
  const subscriberId = `compat-sdk-${randomUUID()}`;
  const subscriberHash = createHmac('sha256', process.env.NOVU_TEST_API_KEY)
    .update(subscriberId).digest('hex');
  const novu = new Novu({
    applicationIdentifier: process.env.NOVU_TEST_APP_IDENTIFIER,
    subscriberId,
    subscriberHash,
    apiUrl: process.env.NOVU_RUST_URL,
    useCache: false,
  });
  const list = await novu.notifications.list();
  if (list.error) throw list.error;
  if (!list.data || list.data.notifications.length !== 0) throw new Error('Unexpected notification list');
  const count = await novu.notifications.count({ filters: [{ read: false, snoozed: false }] });
  if (count.error) throw count.error;
  if (!count.data) throw new Error('Missing notification count');
  console.log('@novu/js session, list and count succeeded');
})().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
