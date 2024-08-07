/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.connector.jdbc.core.table;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.core.table.sink.JdbcDynamicTableSink;
import org.apache.flink.connector.jdbc.core.table.source.JdbcDynamicTableSource;
import org.apache.flink.connector.jdbc.internal.options.InternalJdbcConnectionOptions;
import org.apache.flink.connector.jdbc.internal.options.JdbcDmlOptions;
import org.apache.flink.connector.jdbc.internal.options.JdbcReadOptions;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.catalog.UniqueConstraint;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.lookup.LookupOptions;
import org.apache.flink.table.connector.source.lookup.cache.DefaultLookupCache;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.apache.flink.table.factories.utils.FactoryMocks.createTableSink;
import static org.apache.flink.table.factories.utils.FactoryMocks.createTableSource;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Test for {@link JdbcDynamicTableSource} and {@link JdbcDynamicTableSink} created by {@link
 * JdbcDynamicTableFactory}.
 */
class JdbcDynamicTableFactoryTest {

    private static final ResolvedSchema SCHEMA =
            new ResolvedSchema(
                    Arrays.asList(
                            Column.physical("aaa", DataTypes.INT().notNull()),
                            Column.physical("bbb", DataTypes.STRING().notNull()),
                            Column.physical("ccc", DataTypes.DOUBLE()),
                            Column.physical("ddd", DataTypes.DECIMAL(31, 18)),
                            Column.physical("eee", DataTypes.TIMESTAMP(3))),
                    Collections.emptyList(),
                    UniqueConstraint.primaryKey("name", Arrays.asList("bbb", "aaa")));

    @Test
    void testJdbcCommonProperties() {
        Map<String, String> properties = getAllOptions();
        properties.put("driver", "org.apache.derby.jdbc.EmbeddedDriver");
        properties.put("username", "user");
        properties.put("password", "pass");
        properties.put("connection.max-retry-timeout", "120s");

        // validation for source
        DynamicTableSource actualSource = createTableSource(SCHEMA, properties);
        InternalJdbcConnectionOptions options =
                InternalJdbcConnectionOptions.builder()
                        .setDBUrl("jdbc:derby:memory:mydb")
                        .setTableName("mytable")
                        .setDriverName("org.apache.derby.jdbc.EmbeddedDriver")
                        .setUsername("user")
                        .setPassword("pass")
                        .setConnectionCheckTimeoutSeconds(120)
                        .build();
        JdbcDynamicTableSource expectedSource =
                new JdbcDynamicTableSource(
                        options,
                        JdbcReadOptions.builder().build(),
                        LookupOptions.MAX_RETRIES.defaultValue(),
                        null,
                        SCHEMA.toPhysicalRowDataType());
        assertThat(actualSource).isEqualTo(expectedSource);

        // validation for sink
        DynamicTableSink actualSink = createTableSink(SCHEMA, properties);
        // default flush configurations
        JdbcExecutionOptions executionOptions =
                JdbcExecutionOptions.builder()
                        .withBatchSize(100)
                        .withBatchIntervalMs(1000)
                        .withMaxRetries(3)
                        .build();
        JdbcDmlOptions dmlOptions =
                JdbcDmlOptions.builder()
                        .withTableName(options.getTableName())
                        .withDialect(options.getDialect())
                        .withFieldNames(SCHEMA.getColumnNames().toArray(new String[0]))
                        .withKeyFields("bbb", "aaa")
                        .build();
        JdbcDynamicTableSink expectedSink =
                new JdbcDynamicTableSink(
                        options, executionOptions, dmlOptions, SCHEMA.toPhysicalRowDataType());
        assertThat(actualSink).isEqualTo(expectedSink);
    }

    @Test
    void testJdbcReadProperties() {
        Map<String, String> properties = getAllOptions();
        properties.put("scan.partition.column", "aaa");
        properties.put("scan.partition.lower-bound", "-10");
        properties.put("scan.partition.upper-bound", "100");
        properties.put("scan.partition.num", "10");
        properties.put("scan.fetch-size", "20");
        properties.put("scan.auto-commit", "false");

        DynamicTableSource actual = createTableSource(SCHEMA, properties);

        InternalJdbcConnectionOptions options =
                InternalJdbcConnectionOptions.builder()
                        .setDBUrl("jdbc:derby:memory:mydb")
                        .setTableName("mytable")
                        .build();
        JdbcReadOptions readOptions =
                JdbcReadOptions.builder()
                        .setPartitionColumnName("aaa")
                        .setPartitionLowerBound(-10)
                        .setPartitionUpperBound(100)
                        .setNumPartitions(10)
                        .setFetchSize(20)
                        .setAutoCommit(false)
                        .build();
        JdbcDynamicTableSource expected =
                new JdbcDynamicTableSource(
                        options,
                        readOptions,
                        LookupOptions.MAX_RETRIES.defaultValue(),
                        null,
                        SCHEMA.toPhysicalRowDataType());

        assertThat(actual).isEqualTo(expected);
    }

    @Test
    void testJdbcLookupProperties() {
        Map<String, String> properties = getAllOptions();
        properties.put("lookup.cache", "PARTIAL");
        properties.put("lookup.partial-cache.expire-after-write", "10s");
        properties.put("lookup.partial-cache.expire-after-access", "20s");
        properties.put("lookup.partial-cache.cache-missing-key", "false");
        properties.put("lookup.partial-cache.max-rows", "15213");
        properties.put("lookup.max-retries", "10");

        DynamicTableSource actual = createTableSource(SCHEMA, properties);

        InternalJdbcConnectionOptions options =
                InternalJdbcConnectionOptions.builder()
                        .setDBUrl("jdbc:derby:memory:mydb")
                        .setTableName("mytable")
                        .build();
        JdbcDynamicTableSource expected =
                new JdbcDynamicTableSource(
                        options,
                        JdbcReadOptions.builder().build(),
                        10,
                        DefaultLookupCache.fromConfig(Configuration.fromMap(properties)),
                        SCHEMA.toPhysicalRowDataType());

        assertThat(actual).isEqualTo(expected);
    }

    @Test
    void testJdbcLookupPropertiesWithLegacyOptions() {
        Map<String, String> properties = getAllOptions();
        properties.put("lookup.cache.max-rows", "1000");
        properties.put("lookup.cache.ttl", "10s");
        properties.put("lookup.max-retries", "10");

        DynamicTableSource actual = createTableSource(SCHEMA, properties);

        InternalJdbcConnectionOptions options =
                InternalJdbcConnectionOptions.builder()
                        .setDBUrl("jdbc:derby:memory:mydb")
                        .setTableName("mytable")
                        .build();
        JdbcDynamicTableSource expected =
                new JdbcDynamicTableSource(
                        options,
                        JdbcReadOptions.builder().build(),
                        10,
                        DefaultLookupCache.newBuilder()
                                .maximumSize(1000L)
                                .expireAfterWrite(Duration.ofSeconds(10))
                                .build(),
                        SCHEMA.toPhysicalRowDataType());

        assertThat(actual).isEqualTo(expected);
    }

    @Test
    void testJdbcSinkProperties() {
        Map<String, String> properties = getAllOptions();
        properties.put("sink.buffer-flush.max-rows", "1000");
        properties.put("sink.buffer-flush.interval", "2min");
        properties.put("sink.max-retries", "5");

        DynamicTableSink actual = createTableSink(SCHEMA, properties);

        InternalJdbcConnectionOptions options =
                InternalJdbcConnectionOptions.builder()
                        .setDBUrl("jdbc:derby:memory:mydb")
                        .setTableName("mytable")
                        .build();
        JdbcExecutionOptions executionOptions =
                JdbcExecutionOptions.builder()
                        .withBatchSize(1000)
                        .withBatchIntervalMs(120_000)
                        .withMaxRetries(5)
                        .build();
        JdbcDmlOptions dmlOptions =
                JdbcDmlOptions.builder()
                        .withTableName(options.getTableName())
                        .withDialect(options.getDialect())
                        .withFieldNames(SCHEMA.getColumnNames().toArray(new String[0]))
                        .withKeyFields("bbb", "aaa")
                        .build();

        JdbcDynamicTableSink expected =
                new JdbcDynamicTableSink(
                        options, executionOptions, dmlOptions, SCHEMA.toPhysicalRowDataType());

        assertThat(actual).isEqualTo(expected);
    }

    @Test
    void testJDBCSinkWithParallelism() {
        Map<String, String> properties = getAllOptions();
        properties.put("sink.parallelism", "2");

        DynamicTableSink actual = createTableSink(SCHEMA, properties);

        InternalJdbcConnectionOptions options =
                InternalJdbcConnectionOptions.builder()
                        .setDBUrl("jdbc:derby:memory:mydb")
                        .setTableName("mytable")
                        .setParallelism(2)
                        .build();
        JdbcExecutionOptions executionOptions =
                JdbcExecutionOptions.builder()
                        .withBatchSize(100)
                        .withBatchIntervalMs(1000)
                        .withMaxRetries(3)
                        .build();
        JdbcDmlOptions dmlOptions =
                JdbcDmlOptions.builder()
                        .withTableName(options.getTableName())
                        .withDialect(options.getDialect())
                        .withFieldNames(SCHEMA.getColumnNames().toArray(new String[0]))
                        .withKeyFields("bbb", "aaa")
                        .build();

        JdbcDynamicTableSink expected =
                new JdbcDynamicTableSink(
                        options, executionOptions, dmlOptions, SCHEMA.toPhysicalRowDataType());

        assertThat(actual).isEqualTo(expected);
    }

    @Test
    void testJdbcValidation() {
        // only password, no username
        Map<String, String> properties = getAllOptions();
        properties.put("password", "pass");

        Map<String, String> finalProperties = properties;
        assertThatThrownBy(() -> createTableSource(SCHEMA, finalProperties))
                .hasStackTraceContaining(
                        "Either all or none of the following options should be provided:\n"
                                + "username\npassword");

        // read partition properties not complete
        properties = getAllOptions();
        properties.put("scan.partition.column", "aaa");
        properties.put("scan.partition.lower-bound", "-10");
        properties.put("scan.partition.upper-bound", "100");

        Map<String, String> finalProperties1 = properties;
        assertThatThrownBy(() -> createTableSource(SCHEMA, finalProperties1))
                .hasStackTraceContaining(
                        "Either all or none of the following options should be provided:\n"
                                + "scan.partition.column\n"
                                + "scan.partition.num\n"
                                + "scan.partition.lower-bound\n"
                                + "scan.partition.upper-bound");

        // read partition lower-bound > upper-bound
        properties = getAllOptions();
        properties.put("scan.partition.column", "aaa");
        properties.put("scan.partition.lower-bound", "100");
        properties.put("scan.partition.upper-bound", "-10");
        properties.put("scan.partition.num", "10");

        Map<String, String> finalProperties2 = properties;
        assertThatThrownBy(() -> createTableSource(SCHEMA, finalProperties2))
                .hasStackTraceContaining(
                        "'scan.partition.lower-bound'='100' must not be larger than "
                                + "'scan.partition.upper-bound'='-10'.");

        // lookup cache properties not complete
        properties = getAllOptions();
        properties.put("lookup.cache.max-rows", "10");

        Map<String, String> finalProperties3 = properties;
        assertThatThrownBy(() -> createTableSource(SCHEMA, finalProperties3))
                .hasStackTraceContaining(
                        "Either all or none of the following options should be provided:\n"
                                + "lookup.cache.max-rows\n"
                                + "lookup.cache.ttl");

        // lookup cache properties not complete
        properties = getAllOptions();
        properties.put("lookup.cache.ttl", "1s");

        Map<String, String> finalProperties4 = properties;
        assertThatThrownBy(() -> createTableSource(SCHEMA, finalProperties4))
                .hasStackTraceContaining(
                        "Either all or none of the following options should be provided:\n"
                                + "lookup.cache.max-rows\n"
                                + "lookup.cache.ttl");

        // lookup retries shouldn't be negative
        properties = getAllOptions();
        properties.put("lookup.max-retries", "-1");
        Map<String, String> finalProperties5 = properties;
        assertThatThrownBy(() -> createTableSource(SCHEMA, finalProperties5))
                .hasStackTraceContaining(
                        "The value of 'lookup.max-retries' option shouldn't be negative, but is -1.");

        // sink retries shouldn't be negative
        properties = getAllOptions();
        properties.put("sink.max-retries", "-1");
        Map<String, String> finalProperties6 = properties;
        assertThatThrownBy(() -> createTableSource(SCHEMA, finalProperties6))
                .hasStackTraceContaining(
                        "The value of 'sink.max-retries' option shouldn't be negative, but is -1.");

        // connection.max-retry-timeout shouldn't be smaller than 1 second
        properties = getAllOptions();
        properties.put("connection.max-retry-timeout", "100ms");
        Map<String, String> finalProperties7 = properties;
        assertThatThrownBy(() -> createTableSource(SCHEMA, finalProperties7))
                .hasStackTraceContaining(
                        "The value of 'connection.max-retry-timeout' option must be in second granularity and shouldn't be smaller than 1 second, but is 100ms.");
    }

    @Test
    void testJdbcLookupPropertiesWithExcludeEmptyResult() {
        Map<String, String> properties = getAllOptions();
        properties.put("lookup.cache.max-rows", "1000");
        properties.put("lookup.cache.ttl", "10s");
        properties.put("lookup.max-retries", "10");
        properties.put("lookup.cache.caching-missing-key", "true");

        DynamicTableSource actual = createTableSource(SCHEMA, properties);

        InternalJdbcConnectionOptions options =
                InternalJdbcConnectionOptions.builder()
                        .setDBUrl("jdbc:derby:memory:mydb")
                        .setTableName("mytable")
                        .build();
        JdbcDynamicTableSource expected =
                new JdbcDynamicTableSource(
                        options,
                        JdbcReadOptions.builder().build(),
                        10,
                        DefaultLookupCache.newBuilder()
                                .maximumSize(1000L)
                                .expireAfterWrite(Duration.ofSeconds(10))
                                .build(),
                        SCHEMA.toPhysicalRowDataType());

        assertThat(actual).isEqualTo(expected);
    }

    private Map<String, String> getAllOptions() {
        Map<String, String> options = new HashMap<>();
        options.put("connector", "jdbc");
        options.put("url", "jdbc:derby:memory:mydb");
        options.put("table-name", "mytable");
        return options;
    }
}
