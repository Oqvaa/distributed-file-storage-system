package com.Distributedsystem.DFSS.entity;

import org.springframework.boot.CommandLineRunner;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

/**
 * Keeps the PostgreSQL sequence ahead of IDs already present in installations
 * created before FileEntity used generated identifiers.
 */
@Component
public class FileSequenceInitializer implements CommandLineRunner {

    private final JdbcTemplate jdbcTemplate;

    public FileSequenceInitializer(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    @Override
    public void run(String... args) {
        jdbcTemplate.execute(
                "CREATE SEQUENCE IF NOT EXISTS file_id_seq START WITH 1 INCREMENT BY 1");
        jdbcTemplate.queryForObject(
                "SELECT setval('file_id_seq', GREATEST(COALESCE((SELECT MAX(id) FROM files), 0) + 1, 1), false)",
                Long.class);
    }
}
