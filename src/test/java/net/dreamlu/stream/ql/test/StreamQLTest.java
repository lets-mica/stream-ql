package net.dreamlu.stream.ql.test;

import net.dreamlu.iot.mqtt.core.server.model.Message;
import net.dreamlu.stream.ql.StreamQL;

import java.util.ArrayList;
import java.util.List;

public class StreamQLTest {

    public static void main(String[] args) {
        StreamQL.sql("select * from '/test/1', '/test/2', '/test/3'")
                .build()
                .execute((table) ->
                        getMessage().stream()
                        .filter(data -> data.getTopic().equals(table)))
                .forEach(System.out::println);
    }

    private static List<Message> getMessage() {
        List<Message> data = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            Message message = new Message();
            message.setId(i);
            message.setTopic("/test/" + i);
            message.setClientId("clientId" + i);
            message.setTimestamp(System.currentTimeMillis());
            data.add(message);
        }
        return data;
    }
}
