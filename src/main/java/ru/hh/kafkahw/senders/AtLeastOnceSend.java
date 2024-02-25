package ru.hh.kafkahw.senders;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import ru.hh.kafkahw.internal.KafkaProducer;

import java.util.List;

@Component
public class AtLeastOnceSend implements SendingStrategy {
  // Не стал создавать отдельный бин как в ExactlyOnceSend, чтобы не раздувать код
  private final List<String> topics = List.of("topic2");
  private final KafkaProducer producer;

  @Autowired
  public AtLeastOnceSend(KafkaProducer producer) {
    this.producer = producer;
  }

  // Посылаем сообщение, пока не перестанем получать исключение
  @Override
  public void send(String topic, String payload) {
    if (!topics.contains(topic)) {
      throw new IllegalArgumentException("Given topic is not processing by called sender");
    }
    while (true) {
      try {
        producer.send(topic, payload);
        break;
      } catch (Exception e) {
      }
    }
  }
  @Override
  public List<String> getTopics() {
    return topics;
  }
}
