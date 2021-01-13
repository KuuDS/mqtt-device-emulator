package me.kuuds.emu;

import io.micrometer.core.instrument.MeterRegistry;
import io.netty.handler.codec.mqtt.MqttQoS;
import io.vertx.core.*;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.micrometer.backends.BackendRegistries;
import io.vertx.mqtt.MqttClient;
import io.vertx.mqtt.MqttClientOptions;
import io.vertx.mqtt.messages.MqttConnAckMessage;

import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

import static me.kuuds.emu.MqttManagerVerticle.EVENT_ADDRESS_REDEPLOY;

public class MqttVerticle extends AbstractVerticle {

    private final Logger log = LoggerFactory.getLogger(this.getClass());
    private static final int MAX_RETRY_TIMES = Integer.MAX_VALUE;
    private static final int LINGER = 10;
    private final Object lock = new Object();
    private static final String TOPIC_BASIC = "v1/devices/me";
    private static final String TOPIC_FOR_STRING = TOPIC_BASIC + "/telemetry";
    private static final String TOPIC_FOR_BYTE = TOPIC_BASIC + "/raw";
    private static final AtomicInteger onlineCounter = new AtomicInteger(0);

    private final MeterRegistry registry;
    private final MqttClientConfiguration configuration;
    private MqttClient client;

    public MqttVerticle(MqttClientConfiguration configuration) {
        this.configuration = configuration;
        registry = BackendRegistries.getDefaultNow();
        registry.gauge("mqtt.client.online", onlineCounter);
    }

    @Override
    public void init(Vertx vertx, Context context) {
        super.init(vertx, context);
    }

    @Override
    public void start(Promise<Void> startPromise) throws Exception {
        Future.<MqttClient>future(promise -> {
            initClient(0, promise);
        }).onSuccess(client -> startPromise.complete())
                .onFailure(e -> {
                    redeploy();
                    startPromise.fail(e);
                });
    }

    private void redeploy() {
        vertx.eventBus().publish(EVENT_ADDRESS_REDEPLOY, configuration);
    }

    private void initClient(int retry, Promise<MqttClient> initPromise) {
        log.info("[MQTT|{}] mqtt client try to connect. {}", configuration);
        if (retry >= MAX_RETRY_TIMES) {
            initPromise.fail("[MQTT|" + clientId() + "] fail to connect over" + MAX_RETRY_TIMES + "times.");
            return;
        }
        final var options = new MqttClientOptions();
        options.setUsername(configuration.getUsername());
        options.setPassword(configuration.getPassword());
        options.setAutoKeepAlive(true);
        options.setKeepAliveInterval(300);
        final var port = configuration.getPort();
        final var host = configuration.getHost();
        client = MqttClient.create(getVertx(), options);
        client.closeHandler(_void -> doClose(client));
        Future.<MqttConnAckMessage>future(promise -> client.connect(port, host, promise))
                .onSuccess(msg -> {
                    onlineCounter.incrementAndGet();
                    doPublish(client);
                    initPromise.complete(client);
                }).onFailure(e -> vertx.setTimer(timeWithLinger(5000), id -> initClient(retry + 1, initPromise)));
    }

    private void doClose(MqttClient client) {
        log.warn("[MQTT|{}] client closed.", client.clientId());
        onlineCounter.decrementAndGet();
        Future.<Void>future(client::disconnect)
                .onSuccess(v -> log.info("[MQTT|{}] mqtt client disconnect.", clientId()))
                .onFailure(e -> log.error("[MQTT|{}] mqtt client disconnect failed.", clientId(), e));
    }

    public void doPublish(MqttClient client) {
        if (client == null || !client.isConnected()) {
            return;
        }
        final var buffer = buildPayloadBuffer();
        final var topic = buildTopic();
        registry.counter("mqtt.send.message.total").increment();
        Future.<Integer>future(promise -> client.publish(topic,
                buffer,
                MqttQoS.AT_MOST_ONCE,
                false,
                false,
                promise))
                .onSuccess(responseCode -> vertx.setTimer(timeWithLinger(2_000), id -> doPublish(client)))
                .onFailure(e -> {
                    registry.counter("mqtt.send.message.failed").increment();
                    log.error("fail to send msg.", e);
                });
    }

    private String buildTopic() {
        final var type = configuration.getType();
        if (type.startsWith("json")) {
            return TOPIC_FOR_STRING;
        } else if (type.startsWith("byte")) {
            return TOPIC_FOR_BYTE;
        } else {
            throw new RuntimeException("illegal type.");
        }
    }

    private Buffer buildPayloadBuffer() {
        final var type = configuration.getType();
        Buffer b = null;
        final var payload = configuration.getPayload();
        if (type.startsWith("json") && type.endsWith("string")) {
            b = Buffer.buffer(payload);
        } else if (type.startsWith("json") && type.endsWith("json")) {
            b = new JsonObject().put(payload, new Random().nextDouble() + new Random().nextInt(10)).toBuffer();
        } else if ("byte".equals(type)) {
            b = fromHexStringToBuffer(payload);
        } else {
            log.error("none payload for [{}].", client.clientId());
            throw new RuntimeException("none payload for [{}]");
        }
        return b;
    }

    private Buffer fromHexStringToBuffer(String payload) {
        final var buffer = Buffer.buffer();
        payload = payload.toLowerCase();
        for (int i = 0; i < payload.length(); i = i + 2) {
            if (i + 1 >= payload.length()) {
                break;
            }
            byte b = (byte) Integer.parseInt(payload, i, i + 1, 16);
            buffer.appendByte(b);
        }
        return buffer;
    }

    @Override
    public void stop() throws Exception {
        if (client != null) {
            MqttClient _client;
            synchronized (lock) {
                _client = client;
                client = null;
            }
            if (_client != null) {
                _client.disconnect(ar -> log.info("[MQTT|{}] disconnect.", clientId()));
            }
        }
    }

    private int timeWithLinger(int baseTime) {
        return baseTime + new Random().nextInt(LINGER + LINGER) - LINGER;
    }

    private String clientId() {
        return client.clientId();
    }
}
