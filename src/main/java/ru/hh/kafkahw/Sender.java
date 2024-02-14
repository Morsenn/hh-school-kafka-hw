package ru.hh.kafkahw;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import ru.hh.kafkahw.internal.KafkaProducer;

@Component
public class Sender {
  private final KafkaProducer producer;

  @Autowired
  public Sender(KafkaProducer producer) {
    this.producer = producer;
  }

  public void doSomething(String topic, String message) {
    if ("topic1".equals(topic)) {
      atMostOnce(topic, message);
    }
    else if ("topic2".equals(topic)) {
      atLeastOnce(topic, message);
    }
    else if ("topic3".equals(topic)){
      exactlyOnce(topic, message);
    }
    else {
      throw new RuntimeException("Unknown topic");
    }
  }

  private void atMostOnce(String topic, String message) {
    tryToSendMessage(topic, message);
  }

  private void atLeastOnce(String topic, String message) {
    while (!tryToSendMessage(topic, message));
  }

  private void exactlyOnce(String topic, String message) {
    boolean a = !tryToSendMessage(topic, message) &&
        !tryToSendMessage(topic, message); //&&
        //!tryToSendMessage(topic, message);
  }

  private boolean tryToSendMessage(String topic, String message){
    try {
      producer.send(topic, message);
    } catch (Exception ignore) {
      return false;
    }
    return true;
  }
}
