package ru.hh.kafkahw.errorhandlers;

import org.springframework.kafka.listener.CommonErrorHandler;

// Классы, реализующие этот интерфейс,
// связывают типы исключений со стратегией их обработки в KafkaListener
public interface ErrorHandlingStrategy {
  Class<? extends Throwable> getException();
  CommonErrorHandler getErrorHandler();
}
