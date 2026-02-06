package es.tsystems.genomics.tpiagent.upload.service;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.encoder.PatternLayoutEncoder;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.rolling.RollingFileAppender;
import ch.qos.logback.core.rolling.TimeBasedRollingPolicy;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Factory que crea loggers específicos para cada source.
 * Cada source tendrá su propio archivo de log en su carpeta de logs.
 */
@Component
public class SourceLoggerFactory {

    private static final org.slf4j.Logger log = LoggerFactory.getLogger(SourceLoggerFactory.class);
    private final Map<String, org.slf4j.Logger> sourceLoggers = new ConcurrentHashMap<>();

    /**
     * Obtiene o crea un logger específico para un source.
     * El logger escribirá en: {inboxDir}/{sourceName}/{agentId}/logs/source.log
     */
    public org.slf4j.Logger getLoggerForSource(String sourceName, String logsDirectory) {
        return sourceLoggers.computeIfAbsent(sourceName, name -> createSourceLogger(name, logsDirectory));
    }

    private org.slf4j.Logger createSourceLogger(String sourceName, String logsDirectory) {
        try {
            LoggerContext loggerContext = (LoggerContext) LoggerFactory.getILoggerFactory();

            // Crear el logger con un nombre único
            String loggerName = "source." + sourceName;
            Logger logger = loggerContext.getLogger(loggerName);

            // Evitar que el logger herede appenders del root logger
            logger.setAdditive(false);
            logger.setLevel(Level.INFO);

            // Crear el path del archivo de log
            Path logPath = Paths.get(logsDirectory);
            String logFile = logPath.resolve("source.log").toString();

            // Configurar el appender
            RollingFileAppender<ILoggingEvent> fileAppender = new RollingFileAppender<>();
            fileAppender.setContext(loggerContext);
            fileAppender.setName("FILE-" + sourceName);
            fileAppender.setFile(logFile);

            // Configurar el encoder (formato del log)
            PatternLayoutEncoder encoder = new PatternLayoutEncoder();
            encoder.setContext(loggerContext);
            encoder.setPattern("%d{yyyy-MM-dd HH:mm:ss.SSS} %-5level [%thread] %logger{36} - %msg%n");
            encoder.start();
            fileAppender.setEncoder(encoder);

            // Configurar la política de rolling (rotación diaria)
            TimeBasedRollingPolicy<ILoggingEvent> rollingPolicy = new TimeBasedRollingPolicy<>();
            rollingPolicy.setContext(loggerContext);
            rollingPolicy.setParent(fileAppender);
            rollingPolicy.setFileNamePattern(logPath.resolve("source.%d{yyyy-MM-dd}.log").toString());
            rollingPolicy.setMaxHistory(30);
            rollingPolicy.start();

            fileAppender.setRollingPolicy(rollingPolicy);
            fileAppender.start();

            // Agregar el appender al logger
            logger.addAppender(fileAppender);

            log.info("✓ Created source-specific logger for '{}' writing to: {}", sourceName, logFile);

            return logger;
        } catch (Exception e) {
            log.error("Failed to create source-specific logger for '{}', falling back to default logger", sourceName, e);
            return LoggerFactory.getLogger("source.fallback");
        }
    }
}
