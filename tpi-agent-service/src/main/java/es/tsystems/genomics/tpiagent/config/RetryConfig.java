package es.tsystems.genomics.tpiagent.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Configuration;
import org.springframework.retry.RetryCallback;
import org.springframework.retry.RetryContext;
import org.springframework.retry.RetryListener;
import org.springframework.retry.annotation.EnableRetry;
import org.springframework.context.annotation.Bean;

@Configuration
@EnableRetry
public class RetryConfig {

    private static final Logger log = LoggerFactory.getLogger(RetryConfig.class);

    @Bean
    public RetryListener retryListener() {
        return new RetryListener() {
            @Override
            public <T, E extends Throwable> void onError(RetryContext context, RetryCallback<T, E> callback, Throwable throwable) {
                if (context.getRetryCount() > 0) {
                    log.warn("Retry attempt {} failed for {}: {}",
                        context.getRetryCount(),
                        context.getAttribute("context.name"),
                        throwable.getMessage());
                }
            }

            @Override
            public <T, E extends Throwable> void close(RetryContext context, RetryCallback<T, E> callback, Throwable throwable) {
                if (throwable != null && context.getRetryCount() > 0) {
                    log.error("All retry attempts exhausted for {}: {}",
                        context.getAttribute("context.name"),
                        throwable.getMessage());
                }
            }
        };
    }
}



