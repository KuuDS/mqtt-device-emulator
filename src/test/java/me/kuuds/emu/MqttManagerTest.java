package me.kuuds.emu;

import io.vertx.core.DeploymentOptions;
import io.vertx.core.Vertx;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.concurrent.TimeUnit;

@ExtendWith(VertxExtension.class)
public class MqttManagerTest {


    @Test
    void start(VertxTestContext testContext ) throws Throwable {
        Vertx vertx = App.initVertx();
        vertx.deployVerticle(MqttManagerVerticle.class,
                new DeploymentOptions(),
                testContext.completing());
        testContext.awaitCompletion(1, TimeUnit.MINUTES);
        Assertions.assertTrue(testContext.completed() && !testContext.failed());
    }

}
