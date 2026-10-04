/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cassandra;

import static java.lang.String.format;

import java.util.List;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.metadata.schema.KeyspaceMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.SchemaChangeListenerBase;
import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;
import com.datastax.oss.driver.api.core.session.Session;

import io.debezium.connector.SourceInfoStructMaker;
import io.debezium.connector.cassandra.CassandraSchemaFactory.RowData;

public class AbstractSchemaChangeListener extends SchemaChangeListenerBase {

    private static final Logger logger = LoggerFactory.getLogger(AbstractSchemaChangeListener.class);

    protected final String kafkaTopicPrefix;
    protected final SourceInfoStructMaker<SourceInfo> sourceInfoStructMaker;
    protected final SchemaHolder schemaHolder;

    public AbstractSchemaChangeListener(String kafkaTopicPrefix,
                                        SourceInfoStructMaker<SourceInfo> sourceInfoStructMaker,
                                        SchemaHolder schemaHolder) {
        this.kafkaTopicPrefix = kafkaTopicPrefix;
        this.sourceInfoStructMaker = sourceInfoStructMaker;
        this.schemaHolder = schemaHolder;
    }

    public List<TableMetadata> getCdcOptionedTableMetadataList(final Session session) {
        return session.getMetadata()
                .getKeyspaces()
                .values()
                .stream()
                .flatMap(kmd -> kmd.getTables().values().stream())
                .filter(tm -> {
                    if (tm.isVirtual()) {
                        logger.info(format("Skipping virtual table %s.%s", tm.getKeyspace().asInternal(), tm.getName()));
                        return false;
                    }
                    if (tm.getOptions().get(CqlIdentifier.fromCql("cdc")) == null) {
                        logger.warn(format("There is no cdc option for table %s.%s. Available options are: %s",
                                tm.getKeyspace().asInternal(), tm.getName(), tm.getOptions()));
                        return false;
                    }
                    return true;
                })
                .collect(Collectors.toList());
    }

    /**
     * Registers the cdc=true tables of the driver's metadata that the schema holder doesn't know. The
     * driver can miss a table when several are created in quick succession, and nothing would register
     * it until a restart, so its changes would be silently skipped.
     */
    public void registerMissingCdcTables(final Session session) {
        for (KeyspaceMetadata keyspace : session.getMetadata().getKeyspaces().values()) {
            for (TableMetadata tm : keyspace.getTables().values()) {
                Object cdc = tm.getOptions().get(CqlIdentifier.fromInternal("cdc"));
                if (tm.isVirtual() || cdc == null || !cdc.toString().equals("true")
                        || schemaHolder.getKeyValueSchema(new KeyspaceTable(tm)) != null) {
                    continue;
                }
                logger.warn("Table {}.{} has cdc enabled but isn't registered, registering it now",
                        tm.getKeyspace().asInternal(), tm.getName().asInternal());
                onKeyspaceCreated(keyspace);
                onTableCreated(tm);
            }
        }
    }

    public SchemaHolder getSchemaHolder() {
        return schemaHolder;
    }

    protected KeyValueSchema getKeyValueSchema(TableMetadata tm) {
        return new KeyValueSchema.KeyValueSchemaBuilder()
                .withTableMetadata(tm)
                .withKafkaTopicPrefix(kafkaTopicPrefix)
                .withPrimaryKeyNames(KeyValueSchema.getPrimaryKeyNames(tm))
                .withPrimaryKeySchemas(KeyValueSchema.getPrimaryKeySchemas(tm))
                .withSourceInfoStructMarker(sourceInfoStructMaker)
                .withRowSchema(RowData.rowSchema(tm))
                .build();
    }
}
