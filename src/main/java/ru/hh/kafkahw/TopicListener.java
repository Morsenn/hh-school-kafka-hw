package ru.hh.kafkahw;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;
import ru.hh.kafkahw.internal.Service;

@Component
public class TopicListener {
  private final static Logger LOGGER = LoggerFactory.getLogger(TopicListener.class);
  private final Service service;

  @Autowired
  public TopicListener(Service service) {
    this.service = service;
  }

  @KafkaListener(topics = "topic1", groupId = "group1")
  public void atMostOnce(ConsumerRecord<?, String> consumerRecord, Acknowledgment ack) {
    LOGGER.info("Try handle message, topic {}, payload {}", consumerRecord.topic(), consumerRecord.value());
    ack.acknowledge();
    try {
      service.handle("topic1", consumerRecord.value());
    } catch (Exception e) {

    }
  }

  @KafkaListener(topics = "topic2", groupId = "group2")
  public void atLeastOnce(ConsumerRecord<?, String> consumerRecord, Acknowledgment ack) {
    LOGGER.info("Try handle message, topic {}, payload {}", consumerRecord.topic(), consumerRecord.value());
    while (true) {
      try {
        service.handle("topic2", consumerRecord.value());
      } catch (Exception e) {
        continue;
      }
      break;
    }
    ack.acknowledge();
  }

  @KafkaListener(topics = "topic3", groupId = "group3")
  public void exactlyOnce(ConsumerRecord<?, String> consumerRecord, Acknowledgment ack) {
    LOGGER.info("Try handle message, topic {}, payload {}", consumerRecord.topic(), consumerRecord.value());
    try {
      service.handle("topic3", consumerRecord.value());
    } catch (Exception e1) {
      try {
        service.handle("topic3", consumerRecord.value());
      }
        catch (Exception e2) {
        try {
          service.handle("topic3", consumerRecord.value());
        } catch (Exception ignore) {
          LOGGER.info("Thrice failed service handling");
        }
        }
      }
    ack.acknowledge();
  }
}
