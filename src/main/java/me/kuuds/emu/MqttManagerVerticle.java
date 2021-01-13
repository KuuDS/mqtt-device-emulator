package me.kuuds.emu;

import io.vertx.config.ConfigRetriever;
import io.vertx.config.ConfigRetrieverOptions;
import io.vertx.config.ConfigStoreOptions;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;

import java.util.LinkedList;

public class MqttManagerVerticle extends AbstractVerticle {
    final Logger log = LoggerFactory.getLogger(MqttManagerVerticle.class);
    public static final String EVENT_ADDRESS_REDEPLOY = "redeploy";

    @Override
    public void start(Promise<Void> startPromise) throws Exception {
        Future.<JsonObject>future(promise -> {
            final var fileOptions = new ConfigStoreOptions()
                    .setType("file")
                    .setFormat("properties")
                    .setOptional(true)
                    .setConfig(new JsonObject().put("path", "config.properties"));
            final var retrieverOptions = new ConfigRetrieverOptions()
                    .addStore(fileOptions);
            final var retriever = ConfigRetriever.create(vertx, retrieverOptions);
            retriever.getConfig(promise);
        }).compose(config -> {
            @SuppressWarnings("rawtypes") final var futureList = new LinkedList<Future>();
            final var clientSize = config.getInteger("dev.size");
            for (int i = 0; i < clientSize; ++i) {
                final var username = String.format("%s%04x", config.getString("dev.prefix"), i);
                log.info("build client for dev [{}].", username);
                final var mqttConfig = new MqttClientConfiguration();
                mqttConfig.setHost(config.getString("mqtt.host"));
                mqttConfig.setPort(config.getInteger("mqtt.port"));
                mqttConfig.setUsername(username);
                mqttConfig.setPassword(config.getValue("mqtt.password").toString());
                mqttConfig.setType(config.getString("mqtt.type"));
                mqttConfig.setPayload(config.getString("mqtt.payload"));
                mqttConfig.setPublishInterval(config.getLong("mqtt.publish.interval", 2000L));
                futureList.add(createMqttVerticle(mqttConfig));
            }
            return CompositeFuture.all(futureList);
        }).onSuccess(void0 -> {
            log.info("application has started.");
            startPromise.complete();
        }).onFailure(cause -> {
            log.error("application fail on starting.", cause);
            startPromise.fail(cause);
        });
    }

    public Future<String> createMqttVerticle(MqttClientConfiguration mqttConfig) {
        final var verticle = new MqttVerticle(mqttConfig);
        return Future.future(promise -> vertx.deployVerticle(verticle, promise));
    }

    @Override
    public void stop() throws Exception {
    }
}
