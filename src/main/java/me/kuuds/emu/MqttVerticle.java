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
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class MqttVerticle extends AbstractVerticle {

    private final Logger log = LoggerFactory.getLogger(this.getClass());
    private static final int MAX_RETRY_TIMES = Integer.MAX_VALUE;
    private static final int PING_INTERVAL_MS = 30_000;
    private static final int RECONNECT_INTERVAL_MS = 10_000;
    private static final String TOPIC_BASIC = "v1/devices/me";
    private static final String TOPIC_FOR_STRING = TOPIC_BASIC + "/telemetry";
    private static final String TOPIC_FOR_BYTE = TOPIC_BASIC + "/raw";
    private static final AtomicInteger onlineCounter = new AtomicInteger(0);

    private final AtomicBoolean closed = new AtomicBoolean(false);
    private final MeterRegistry registry;
    private final MqttClientConfiguration configuration;
    private MqttClient client;
    private final String id;
    private final Random random = new Random(System.nanoTime());

    public MqttVerticle(MqttClientConfiguration configuration) {
        this.configuration = configuration;
        this.id = configuration.getUsername();
        registry = BackendRegistries.getDefaultNow();
        registry.gauge("mqtt.client.online", onlineCounter);
    }

    @Override
    public void start(Promise<Void> startPromise) throws Exception {
        initClient();
        Future.<Void>future(promise -> doConnect(0, promise)).onSuccess(v -> {
            doPublish();
            doPing();
            startPromise.complete();
        }).onFailure(startPromise::fail);
    }

    private void initClient() {
        final var options = new MqttClientOptions();
        options.setUsername(configuration.getUsername());
        options.setPassword(configuration.getPassword());
        options.setAutoKeepAlive(false);
        options.setReusePort(true);
        options.setReconnectAttempts(5);
        options.setReconnectInterval(10_000);
        options.setClientId(configuration.getUsername());
        client = MqttClient.create(getVertx(), options);
        client.closeHandler(v -> doClose(client));
    }

    private void doConnect(int retry, Promise<Void> connectPromise) {
        log.info("[MQTT|{}] connect to broker. {}", id, configuration);
        if (retry >= MAX_RETRY_TIMES) {
            connectPromise.fail("[MQTT|" + id + "] fail to connect over" + MAX_RETRY_TIMES + "times.");
            return;
        }
        final var port = configuration.getPort();
        final var host = configuration.getHost();
        Future.<MqttConnAckMessage>future(promise -> client.connect(port, host, promise)).onSuccess(msg -> {
            log.info("MQTT|{}] connection succeed", id);
            onlineCounter.incrementAndGet();
            connectPromise.complete();
        }).onFailure(e -> {
            log.error("MQTT|{}] connect failed.", id, e);
            vertx.setTimer(RECONNECT_INTERVAL_MS, tid -> doConnect(retry + 1, connectPromise));
        });
    }

    private void doClose(MqttClient client) {
        log.warn("[MQTT|{}] connection closed.", client.clientId());
        onlineCounter.decrementAndGet();
        vertx.setTimer(RECONNECT_INTERVAL_MS,
                tid -> Future.<Void>future(promise -> doConnect(0, promise)).onFailure(e -> vertx.close()));
    }

    private void doPing() {
        if (client != null && client.isConnected()) {
            client.ping();
        }
        vertx.setTimer(PING_INTERVAL_MS, tid -> doPing());
    }

    private void doPublish() {
        if (client == null || !client.isConnected()) {
            log.info("[MQTT|{}] client disconnect. skip.", id);
            vertx.setTimer(configuration.getPublishInterval(), tid -> doPublish());
            return;
        }
        final var buffer = buildPayloadBuffer();
        final var topic = buildTopic();
        registry.counter("mqtt.send.message.total").increment();
        Future.<Integer>future(promise -> client.publish(topic, buffer, MqttQoS.AT_MOST_ONCE, false, false, promise))
                .onComplete(ar -> {
                    if (ar.failed()) {
                        registry.counter("mqtt.send.message.failed").increment();
                        log.error("MQTT|{}] fail to send msg.", id, ar.cause());
                    }
                    vertx.setTimer(configuration.getPublishInterval(), tid -> doPublish());
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
            b = new JsonObject().put(payload, random.nextDouble() + random.nextInt(10)).toBuffer();
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
        if (closed.compareAndSet(false, true) && client != null) {
            if (client.isConnected()) {
                client.disconnect(ar -> log.info("[MQTT|{}] disconnect.", id));
            }
            client = null;
        }
    }
}
