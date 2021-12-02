# StreamQL

![JAVA 8](https://img.shields.io/badge/JDK-1.8+-brightgreen.svg)

Stream + Druid Sql Parser = StreamQL

## 使用

```java
public static void main(String[] args) {
    StreamQL.sql("select * from '/test/1'")
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
```

## 参考vs借鉴

重度参考 vs copy [reactor-ql](https://github.com/jetlinks/reactor-ql)，reactor-ql 非常强大。