package me.kuuds.emu;

import io.netty.handler.codec.mqtt.MqttQoS;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.mqtt.MqttClient;
import io.vertx.mqtt.MqttClientOptions;
import io.vertx.mqtt.messages.MqttConnAckMessage;

public class ClientApp {

    public static void main(String[] args) {
        Vertx vertx = Vertx.vertx();
        MqttClientOptions options = new MqttClientOptions();
        options.setReconnectAttempts(5);
        options.setReconnectInterval(10_000);
        MqttClient mqttClient = MqttClient.create(vertx);
        mqttClient.publishHandler(msg -> System.out.println("msg: " + msg.payload().toString()));
        mqttClient.closeHandler(v -> {
            System.out.println("close");
            vertx.setTimer(5_000, tid -> Future.<Void>future(promise -> connect(vertx, mqttClient, promise)));
        });
        vertx.setPeriodic(30_000, tid -> mqttClient.ping());
        mqttClient.pingResponseHandler(msg -> System.out.println("receive pingResp"));
        Future.<Void>future(promise -> connect(vertx, mqttClient, promise));
    }

    private static void connect(Vertx vertx, MqttClient mqttClient, Promise<Void> connPromise)  {
        Future.<MqttConnAckMessage>future(promise -> mqttClient.connect(1883, "::1", promise))
                .onFailure(e -> {
                    System.out.println("connError");
                    vertx.setTimer(5_000, tid -> connect(vertx, mqttClient, connPromise));
                })
                .onSuccess(msg -> {
                    mqttClient.subscribe("v1/devices/me/telemetry", MqttQoS.AT_LEAST_ONCE.value());
                    connPromise.complete();
                });
    }




}
