package com.analytics.datagen.producer;

import com.analytics.datagen.EventSink;
import com.analytics.datagen.model.DomainEvent;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;

/**
 * File-based event sink for landing zone pattern.
 * Writes events to JSON files organized by date and topic.
 */
public class FileEventSink implements EventSink {

    private static final Logger LOG = LoggerFactory.getLogger(FileEventSink.class);
    private static final ObjectMapper MAPPER = new ObjectMapper().registerModule(new JavaTimeModule());
    private static final DateTimeFormatter DATE_FORMAT = DateTimeFormatter.ofPattern("yyyy-MM-dd");

    private final String basePath;

    public FileEventSink(String basePath) {
        this.basePath = basePath;
        new File(basePath).mkdirs();
        LOG.info("File event sink initialized: {}", basePath);
    }

    @Override
    public void send(String topicSuffix, DomainEvent event) {
        try {
            String date = LocalDate.now().format(DATE_FORMAT);
            String dirPath = basePath + "/" + topicSuffix + "/date=" + date;
            new File(dirPath).mkdirs();

            String filePath = dirPath + "/events.json";
            String json = MAPPER.writeValueAsString(event);

            try (BufferedWriter writer = new BufferedWriter(new FileWriter(filePath, true))) {
                writer.write(json);
                writer.newLine();
            }

        } catch (IOException e) {
            LOG.error("Failed to write event to file: {}", e.getMessage());
            throw new RuntimeException("File write failed", e);
        }
    }

    @Override
    public void flush() {
        // No-op for file sink
    }

    @Override
    public void close() {
        LOG.info("File event sink closed");
    }
}
