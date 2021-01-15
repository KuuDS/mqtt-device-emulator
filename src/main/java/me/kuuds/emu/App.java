package me.kuuds.emu;

import io.vertx.core.DeploymentOptions;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.core.http.HttpServerOptions;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.core.logging.SLF4JLogDelegateFactory;
import io.vertx.micrometer.MicrometerMetricsOptions;
import io.vertx.micrometer.VertxPrometheusOptions;

public class App {

    public static void main(String[] args) {
        final var vertx = initVertx();
        final var options = new DeploymentOptions();
        Future.<String>future(promise -> vertx.deployVerticle(MqttManagerVerticle.class, options, promise))
                .onFailure(e -> vertx.close());
    }

    public static Vertx initVertx() {
        System.setProperty(LoggerFactory.LOGGER_DELEGATE_FACTORY_CLASS_NAME, SLF4JLogDelegateFactory.class.getName());
        return Vertx.vertx(new VertxOptions().setMetricsOptions(
                new MicrometerMetricsOptions().setPrometheusOptions(new VertxPrometheusOptions().setEnabled(true)
                        .setStartEmbeddedServer(true).setEmbeddedServerOptions(new HttpServerOptions().setPort(8080))
                        .setEmbeddedServerEndpoint("/metrics/vertx")).setEnabled(true)));
    }

}
