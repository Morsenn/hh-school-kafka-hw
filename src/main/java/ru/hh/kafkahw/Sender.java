package ru.hh.kafkahw;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import ru.hh.kafkahw.internal.KafkaProducer;
import ru.hh.kafkahw.senders.SendingStrategy;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Component
public class Sender {
  private final Map<String, SendingStrategy> sendingStrategies;

  @Autowired
  public Sender(List<SendingStrategy> senders) {
    // Здесь мы собираем мапу вида Топик - Стратегия, которая к нему применяется
    this.sendingStrategies = senders.stream().flatMap(
        sender -> sender.getTopics()
            .stream()
            .map(topic -> Map.entry(topic, sender))
        ).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
  }

  public void doSomething(String topic, String message) {
    SendingStrategy strategy = sendingStrategies.get(topic);
    if (strategy == null) {
      throw new IllegalArgumentException("This topic don't have any sending strategy");
    }
    strategy.send(topic, message);
  }

}
