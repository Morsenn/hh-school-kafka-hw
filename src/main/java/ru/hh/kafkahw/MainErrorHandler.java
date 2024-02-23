package ru.hh.kafkahw;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.listener.CommonDelegatingErrorHandler;
import org.springframework.kafka.listener.CommonErrorHandler;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.stereotype.Component;
import ru.hh.kafkahw.errorhandlers.ErrorHandlingStrategy;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

// Создаём ErrorHandler, который на основе типа брошенного исключения применит нужную стратегию
@Component
public class MainErrorHandler {

  private final CommonDelegatingErrorHandler errorHandler;

  @Autowired
  public MainErrorHandler(List<ErrorHandlingStrategy> errorHandlers) {
    this.errorHandler = new CommonDelegatingErrorHandler(new DefaultErrorHandler());
    Map<Class<? extends Throwable>, CommonErrorHandler> delegates = errorHandlers.stream().collect(Collectors.toMap(
        ErrorHandlingStrategy::getException,
        ErrorHandlingStrategy::getErrorHandler)
    );
    this.errorHandler.setErrorHandlers(delegates);
  }

  public CommonDelegatingErrorHandler getErrorHandler() {
    return this.errorHandler;
  }
}
