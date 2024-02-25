package ru.hh.kafkahw.senders;

import ru.hh.kafkahw.internal.KafkaProducer;

import java.util.List;

// Стратегия отправки сообщения в кафку и список топиков, для которых она реализуется
public interface SendingStrategy {
  void send(String topic, String payload);
  List<String> getTopics();
}
