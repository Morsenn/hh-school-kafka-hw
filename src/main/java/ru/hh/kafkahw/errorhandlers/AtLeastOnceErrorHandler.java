package ru.hh.kafkahw.errorhandlers;

import org.springframework.kafka.listener.CommonErrorHandler;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.stereotype.Component;
import org.springframework.util.backoff.ExponentialBackOff;
import org.springframework.util.backoff.FixedBackOff;
import ru.hh.kafkahw.exceptions.AtLeastOnceProcessingException;
import ru.hh.kafkahw.exceptions.ExactlyOnceProcessingException;

@Component
public class AtLeastOnceErrorHandler implements ErrorHandlingStrategy{
  // Бесконечно отправляем, пока не перестанем получать исключения
  private final Class<? extends Throwable> exception = AtLeastOnceProcessingException.class;
  private final CommonErrorHandler errorHandler = new DefaultErrorHandler(
      new ExponentialBackOff(0, 1.0)
  );

  @Override
  public Class<? extends Throwable> getException() { return exception; }

  @Override
  public CommonErrorHandler getErrorHandler() { return errorHandler; }
}
