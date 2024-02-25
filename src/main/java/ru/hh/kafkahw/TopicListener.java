package ru.hh.kafkahw;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationEvent;
import org.springframework.context.ApplicationListener;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.RetryableTopic;
import org.springframework.kafka.event.ConsumerStartedEvent;
import org.springframework.kafka.event.ConsumerStoppedEvent;
import org.springframework.kafka.event.KafkaEvent;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.lang.NonNull;
import org.springframework.retry.annotation.Backoff;
import org.springframework.stereotype.Component;
import org.springframework.util.backoff.ExponentialBackOff;
import ru.hh.kafkahw.exceptions.AtLeastOnceProcessingException;
import ru.hh.kafkahw.exceptions.AtMostOnceProcessingException;
import ru.hh.kafkahw.exceptions.ExactlyOnceProcessingException;
import ru.hh.kafkahw.internal.Service;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

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
      LOGGER.warn("threw exception topic1");
      throw new AtMostOnceProcessingException();
    }
  }

  @KafkaListener(topics = "topic2", groupId = "group2")
  public void atLeastOnce(ConsumerRecord<?, String> consumerRecord, Acknowledgment ack) {
    LOGGER.info("Try handle message, topic {}, payload {}", consumerRecord.topic(), consumerRecord.value());
    try {
      service.handle("topic2", consumerRecord.value());
    } catch(Exception e) {
      LOGGER.warn("threw exception topic2");
      throw new AtLeastOnceProcessingException();
    }
    ack.acknowledge();
  }
}
