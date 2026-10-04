/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cassandra;

import static com.codahale.metrics.MetricRegistry.name;
import static io.debezium.connector.cassandra.CassandraConnectorTaskTemplate.METRIC_REGISTRY_INSTANCE;

import java.io.File;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.MetricRegistry;
import com.datastax.oss.driver.api.core.AllNodesFailedException;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.metadata.schema.SchemaChangeListener;
import com.datastax.oss.driver.api.core.metrics.Metrics;

/**
 * A wrapper around Cassandra driver that is used to query Cassandra table and table schema.
 */
public class CassandraClient implements AutoCloseable {

    private static final Logger LOGGER = LoggerFactory.getLogger(CassandraClient.class);

    private static final long MAX_CONNECT_BACKOFF_MS = 30_000;
    // How often the schema is refreshed to catch the tables the driver missed; 0 disables it
    private static final long SCHEMA_REFRESH_INTERVAL_SECONDS = Long.getLong("debezium.cassandra.schema.refresh.interval.seconds", 60);

    private final CqlSession session;
    private final ScheduledExecutorService schemaRefresher;

    public CassandraClient(String config, SchemaChangeListener schemaChangeListener) {
        session = connect(config, schemaChangeListener);

        registerClusterMetrics(session.getMetadata().getClusterName().orElse("unknown-cluster-name"));

        if (schemaChangeListener instanceof AbstractSchemaChangeListener && SCHEMA_REFRESH_INTERVAL_SECONDS > 0) {
            AbstractSchemaChangeListener listener = (AbstractSchemaChangeListener) schemaChangeListener;
            schemaRefresher = Executors.newSingleThreadScheduledExecutor(runnable -> {
                Thread thread = new Thread(runnable, "cassandra-schema-refresher");
                thread.setDaemon(true);
                return thread;
            });
            schemaRefresher.scheduleWithFixedDelay(() -> refreshSchema(listener),
                    SCHEMA_REFRESH_INTERVAL_SECONDS, SCHEMA_REFRESH_INTERVAL_SECONDS, TimeUnit.SECONDS);
        }
        else {
            schemaRefresher = null;
        }
    }

    /**
     * Retries until Cassandra accepts the connection, rather than failing the connector. Running next to
     * a Cassandra node that is still starting, or whose login role isn't created yet, is normal: on
     * Kubernetes the operator only creates the superuser once every container of the pod is ready.
     */
    private static CqlSession connect(String config, SchemaChangeListener schemaChangeListener) {
        long backoffMs = 1_000;
        for (int attempt = 1;; attempt++) {
            try {
                return CqlSession.builder()
                        .withConfigLoader(DriverConfigLoader.fromFile(new File(config)))
                        .withSchemaChangeListener(schemaChangeListener)
                        .build();
            }
            catch (AllNodesFailedException e) {
                LOGGER.warn("Connecting to Cassandra failed (attempt {}), retrying in {} ms: {}", attempt, backoffMs, e.getMessage());
                try {
                    Thread.sleep(backoffMs);
                }
                catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    throw e;
                }
                backoffMs = Math.min(backoffMs * 2, MAX_CONNECT_BACKOFF_MS);
            }
        }
    }

    private void refreshSchema(AbstractSchemaChangeListener listener) {
        try {
            // The driver notifies the listener of the differences it finds
            session.refreshSchema();
            listener.registerMissingCdcTables(session);
        }
        catch (Exception e) {
            LOGGER.warn("Refreshing the schema failed", e);
        }
    }

    public Set<Node> getHosts() {
        return new HashSet<>(session.getMetadata().getNodes().values());
    }

    public String getClusterName() {
        return session.getMetadata().getClusterName().orElse("unknown-cluster-name");
    }

    public boolean isQueryable() {
        return !session.isClosed();
    }

    public ResultSet execute(SimpleStatement statement) {
        return session.execute(statement);
    }

    public ResultSet execute(String query) {
        return session.execute(query);
    }

    public ResultSet execute(String query, Object... values) {
        return session.execute(query, values);
    }

    public ResultSet execute(String query, Map<String, Object> values) {
        return session.execute(query, values);
    }

    public void shutdown() {
        if (schemaRefresher != null) {
            schemaRefresher.shutdownNow();
        }
        if (!session.isClosed()) {
            session.close();
        }
    }

    private void registerClusterMetrics(String prefix) {
        Optional<Metrics> metrics = session.getMetrics();
        if (metrics.isPresent()) {
            MetricRegistry clusterRegistry = metrics.get().getRegistry();
            clusterRegistry.getMetrics().forEach((key, value) -> METRIC_REGISTRY_INSTANCE.register(name(prefix, key), value));
        }
    }

    @Override
    public void close() {
        shutdown();
    }
}
