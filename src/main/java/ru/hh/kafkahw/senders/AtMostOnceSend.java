package ru.hh.kafkahw.senders;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import ru.hh.kafkahw.internal.KafkaProducer;

import java.util.List;

@Component
public class AtMostOnceSend implements SendingStrategy {
  private final List<String> topics = List.of("topic1");
  private final KafkaProducer producer;

  @Autowired
  public AtMostOnceSend(KafkaProducer producer) {
    this.producer = producer;
  }

  @Override
  public void send(String topic, String payload) {
    if (!topics.contains(topic)) {
      throw new IllegalArgumentException("Given topic is not processing by called sender");
    }
    try {
      producer.send(topic, payload);
    } catch (Exception e) {
    }
  }
  @Override
  public List<String> getTopics() {
    return topics;
  }
}
