/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.shadowmanager;

import com.aws.greengrass.lifecyclemanager.Kernel;
import com.aws.greengrass.logging.api.Logger;
import com.aws.greengrass.logging.impl.LogManager;
import com.aws.greengrass.shadowmanager.exception.ShadowManagerDataException;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import lombok.Getter;
import lombok.Synchronized;
import org.flywaydb.core.Flyway;
import org.flywaydb.core.api.FlywayException;
import org.flywaydb.core.internal.exception.FlywaySqlException;
import org.sqlite.SQLiteException;
import org.sqlite.javax.SQLiteConnectionPoolDataSource;
import org.sqlite.javax.SQLitePooledConnection;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.util.stream.Stream;
import javax.inject.Inject;
import javax.inject.Singleton;

import static com.aws.greengrass.shadowmanager.ShadowManager.SERVICE_NAME;

/**
 * Connection manager for the local shadow documents.
 */
@Singleton
public class ShadowManagerDatabase implements Closeable {
    private static final String DATABASE_NAME = "shadow";
    // see https://www.h2database.com/javadoc/org/h2/engine/DbSettings.html
    // these setting optimize for minimal disk space over concurrent performance
    private static final String DATABASE_FORMAT = "jdbc:sqlite:%s/%s"
            ;
    private final SQLiteConnectionPoolDataSource dataSource;

    private static final Logger logger = LogManager.getLogger(ShadowManagerDatabase.class);
    private final Path databasePath;
    @Getter
    private boolean initialized = false;

    /**
     * Creates a database with a {@link javax.sql.DataSource} using the kernel config.
     *
     * @param kernel Kernel config for the database manager.
     */
    @Inject
    public ShadowManagerDatabase(final Kernel kernel) {
        this(kernel.getNucleusPaths().workPath().resolve(SERVICE_NAME));
    }

    /**
     * Create a new instance at the specified path.
     * @param path a path to store the db.
     */
    public ShadowManagerDatabase(Path path) {
        this.dataSource = new SQLiteConnectionPoolDataSource();
        this.dataSource.setUrl(String.format(DATABASE_FORMAT, path, DATABASE_NAME));
        this.databasePath = path;
    }

    /**
     * Performs the database installation. This includes any migrations that needs to be performed.
     *
     * @throws ShadowManagerDataException if flyway migration fails
     */
    @Synchronized
    public void install() throws ShadowManagerDataException {
        try {
            boolean isMigrationSuccessful = migrateAndGetResult();
            if (!isMigrationSuccessful) {
                logger.atWarn().log("Failed to migrate the existing shadow manager DB. "
                        + "Removing it and creating a new one.");
                deleteDB(databasePath);
                migrateDB();
            }
            initialized = true;
        } catch (FlywayException | IOException e) {
            throw new ShadowManagerDataException(e);
        }
    }

    private void migrateDB() {
        Flyway flyway = Flyway.configure(getClass().getClassLoader())
                .locations("db/migration")
                .dataSource(dataSource)
                .load();
        flyway.migrate();
    }

    @SuppressWarnings({"checkstyle:EmptyBlock", "checkstyle:WhitespaceAround", "PMD.AvoidCatchingGenericException"})
    private boolean migrateAndGetResult() {
        try {
            migrateDB();
        } catch (FlywaySqlException flywaySqlException) {
            if (flywaySqlException.getCause() instanceof SQLiteException
                    && flywaySqlException.getCause().getCause() instanceof IllegalStateException) {
                logger.atError().cause(flywaySqlException).log("Shadow manager DB is corrupted");
                return false;
            }
            throw flywaySqlException;
        }

        // Validate that after migration we're actually able to open and connect to the DB.
        // This may fail if closing the DB after migration failed for some reason.
        try {
            try (Connection p = getPool().getConnection()) {}
            return true;
        } catch (Exception e) {
            logger.atError().cause(e).log("Shadow manager DB could not be opened. Deleting and recreating it");
            close();
            return false;
        }
    }

    /**
     * Get a reference to the connection pool.
     * @return JDBC connection pool
     */
    public synchronized SQLiteConnectionPoolDataSource getPool() {
        return dataSource;
    }

    private void deleteDB(Path databasePath) throws IOException {
        try (Stream<Path> workPathFiles = Files.list(databasePath)) {
            workPathFiles.filter(path -> path.toString().endsWith("db"))
            .forEach(path -> {
                try {
                    logger.atDebug().kv("file", path).log("Deleting db file");
                    Files.deleteIfExists(path);
                } catch (IOException e) {
                    throw new ShadowManagerDataException(e);
                }
            });
        }
    }

    @Override
    @Synchronized
    @SuppressWarnings("PMD.NullAssignment")
    @SuppressFBWarnings(value = "UWF_FIELD_NOT_INITIALIZED_IN_CONSTRUCTOR", justification = "Field gated by flag")
    public void close() {
        // no need to close the pool
    }
}
