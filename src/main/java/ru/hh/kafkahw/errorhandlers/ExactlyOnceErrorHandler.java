package ru.hh.kafkahw.errorhandlers;

import org.springframework.kafka.listener.CommonErrorHandler;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.stereotype.Component;
import org.springframework.util.backoff.FixedBackOff;
import ru.hh.kafkahw.exceptions.ExactlyOnceProcessingException;

@Component
public class ExactlyOnceErrorHandler implements ErrorHandlingStrategy{
  private final CommonErrorHandler errorHandler = new DefaultErrorHandler(new FixedBackOff(0, 1));

  @Override
  public Class<? extends Throwable> getException() { return ExactlyOnceProcessingException.class; }

  @Override
  public CommonErrorHandler getErrorHandler() { return errorHandler; }
}
