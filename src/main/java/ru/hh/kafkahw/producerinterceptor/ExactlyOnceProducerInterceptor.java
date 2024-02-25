package ru.hh.kafkahw.producerinterceptor;

import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.List;
import java.util.Map;


public class ExactlyOnceProducerInterceptor implements ProducerInterceptor<String, String> {
  private final List<String> processingTopics;

  public ExactlyOnceProducerInterceptor(List<String> processingTopics) {
    this.processingTopics = processingTopics;
  }
  // Если сообщение посылается в exactly once, то разбиваем его на ключ и значение
  @Override
  public ProducerRecord<String, String> onSend(ProducerRecord<String, String> producerRecord) {
    if (!processingTopics.contains(producerRecord.topic())) {
      return producerRecord;
    }
    String value = producerRecord.value();
    if (value == null) {
      throw new IllegalArgumentException("Producer record value must not be null");
    }
    String[] stringParts = value.split(":", 2);
    if (stringParts.length < 2) {
      throw new IllegalArgumentException("Producer record value must contain : pattern");
    }
    return new ProducerRecord<>(
        producerRecord.topic(),
        stringParts[0],
        stringParts[1]
        );
  }

  @Override
  public void onAcknowledgement(RecordMetadata metadata, Exception exception) {
  }

  @Override
  public void close() {
  }

  @Override
  public void configure(Map<String, ?> configs) {
  }
}
