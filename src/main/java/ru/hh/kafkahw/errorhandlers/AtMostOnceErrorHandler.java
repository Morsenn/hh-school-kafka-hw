package ru.hh.kafkahw.errorhandlers;

import org.springframework.kafka.listener.CommonErrorHandler;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.stereotype.Component;
import org.springframework.util.backoff.FixedBackOff;
import ru.hh.kafkahw.exceptions.AtMostOnceProcessingException;
import ru.hh.kafkahw.exceptions.ExactlyOnceProcessingException;

@Component
public class AtMostOnceErrorHandler implements ErrorHandlingStrategy{
  // Отправляем только один раз
  private final Class<? extends Throwable> exception = AtMostOnceProcessingException.class;
  private final CommonErrorHandler errorHandler = new DefaultErrorHandler(new FixedBackOff(0, 0));

  @Override
  public Class<? extends Throwable> getException() {
    return exception;
  }

  @Override
  public CommonErrorHandler getErrorHandler() {
    return errorHandler;
  }
}
