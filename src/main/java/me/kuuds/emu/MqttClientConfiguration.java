package me.kuuds.emu;

import lombok.Data;
import lombok.ToString;

@ToString
@Data
public class MqttClientConfiguration {

    String host;
    int port;
    String username;
    String password;
    String type;
    String payload;
    long publishInterval;


}
