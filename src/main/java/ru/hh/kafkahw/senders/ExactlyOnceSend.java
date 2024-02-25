package ru.hh.kafkahw.senders;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Import;
import org.springframework.stereotype.Component;
import ru.hh.kafkahw.internal.KafkaProducer;

import java.util.List;

@Component
public class ExactlyOnceSend implements SendingStrategy {
  private final List<String> topics;
  private final KafkaProducer producer;
  private long counter = 0;
  private final static String PAYLOAD_FORMAT = "%d:%s";
  private final static Logger LOGGER = LoggerFactory.getLogger(ExactlyOnceSend.class);

  @Autowired
  public ExactlyOnceSend(KafkaProducer producer, List<String> topics) {
    this.producer = producer;
    this.topics = topics;
    LOGGER.info("ExactlyOnceSend created");
    topics.forEach(LOGGER::info);
  }

  // Отправляем сообщение вида УНИКАЛЬНЫЙ-КЛЮЧ:PAYLOAD, в интерсепторе разбиваем его на две части
  // Ключ нужен, чтобы гарантировать, что возможные дубликаты попадут в один Consumer. Уникальность ключа позволит
  // распознать дубликаты на стороне получателя
  @Override
  public void send(String topic, String payload) {
    if (!topics.contains(topic)) {
      throw new IllegalArgumentException("Given topic is not processing by called sender");
    }
    counter++;
    String newPayload = String.format(PAYLOAD_FORMAT, counter, payload);
    while (true) {
      try {
        producer.send(topic, newPayload);
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
