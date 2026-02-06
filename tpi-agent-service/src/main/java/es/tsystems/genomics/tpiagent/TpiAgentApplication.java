package es.tsystems.genomics.tpiagent;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
public class TpiAgentApplication {
    public static void main(String[] args) {
        SpringApplication.run(TpiAgentApplication.class, args);
    }
}

