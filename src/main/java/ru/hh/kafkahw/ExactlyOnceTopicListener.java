package ru.hh.kafkahw;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationListener;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.event.ConsumerStartedEvent;
import org.springframework.kafka.event.ConsumerStoppedEvent;
import org.springframework.kafka.event.KafkaEvent;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.lang.NonNull;
import org.springframework.stereotype.Component;
import ru.hh.kafkahw.exceptions.ExactlyOnceProcessingException;
import ru.hh.kafkahw.internal.Service;

import java.util.HashMap;
import java.util.Map;

@Component
class ExactlyOnceTopicListener implements ApplicationListener<KafkaEvent> {
  // Здесь должен быть LRU кеш, но чтобы не раздувать код взятой с SO имплементацией, вставил Map))
  // Thread Local нужен, чтобы не было Contention на общий кеш
  private static ThreadLocal<Map<String, Integer>> mockLRUCache = new ThreadLocal<>();
  private final static Logger LOGGER = LoggerFactory.getLogger(TopicListener.class);
  private final Service service;

  @Autowired
  public ExactlyOnceTopicListener(Service service) {
    this.service = service;
  }

  @KafkaListener(topics = "topic3", groupId = "group3", concurrency = "2")
  public void exactlyOnce(ConsumerRecord<String, String> consumerRecord, Acknowledgment ack) {
    // Пытаемся отправить, пока не будет успех, но не более двух раз
    // Если предположить, что Random имеет равномерное распределение, то вероятность того,
    // что сообщение доставится ровно один раз примерно 97%, тогда вероятность пройти тест примерно 5%))
    // Возможно можно придумать более эффективную стратегию, если организовать третью доставку,
    // но с некоторой вероятностью 0 < p < 1
    LOGGER.info("Try handle message, topic {}, payload {}, partition {}, threadId {}",
        consumerRecord.topic(),
        consumerRecord.value(),
        consumerRecord.partition(),
        Thread.currentThread().threadId());
    if (mockLRUCache.get().getOrDefault(consumerRecord.key(), 0) >= 2) {
      LOGGER.info("Message, payload {}, was rejected", consumerRecord.value());
      return;
    }
    else {
      //Если записи нет в мапе, то добавится один, иначе прибавится +1 к записи
      mockLRUCache.get().merge(consumerRecord.key(), 1, Integer::sum);
    }
    try {
      service.handle("topic3", consumerRecord.value());
    }
    catch (Exception e) {
      LOGGER.warn("threw exception topic3");
      throw new ExactlyOnceProcessingException();
    }
    ack.acknowledge();
  }

  @Override
  public void onApplicationEvent(@NonNull KafkaEvent event){
    // Не нашёл более красивого способа создать кеши только для нужных консьюмеров)
    if (event.getContainer(ConcurrentMessageListenerContainer.class).getConcurrency() != 2) {
      return;
    }
    if (event instanceof ConsumerStartedEvent) {
      LOGGER.info("Initialized LRU cache, thread {}", Thread.currentThread().threadId());
      mockLRUCache.set(new HashMap<>());
    }
    else if (event instanceof ConsumerStoppedEvent) {
      LOGGER.info("Removed LRU cache, thread {}", Thread.currentThread().threadId());
      mockLRUCache.remove();
    }
  }
}
