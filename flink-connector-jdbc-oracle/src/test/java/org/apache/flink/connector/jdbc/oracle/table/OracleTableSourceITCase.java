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

package org.apache.flink.connector.jdbc.oracle.table;

import org.apache.flink.connector.jdbc.oracle.OracleTestBase;
import org.apache.flink.connector.jdbc.oracle.database.dialect.OracleDialect;
import org.apache.flink.connector.jdbc.testutils.TableManaged;
import org.apache.flink.connector.jdbc.testutils.tables.TableBuilder;
import org.apache.flink.connector.jdbc.testutils.tables.TableRow;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.test.util.AbstractTestBase;
import org.apache.flink.types.Row;
import org.apache.flink.util.CollectionUtil;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/** The Table Source ITCase for {@link OracleDialect}. */
class OracleTableSourceITCase extends AbstractTestBase implements OracleTestBase {

    private static final TableRow INPUT_TABLE =
            TableBuilder.tableRow(
                    "oracle_test_table",
                    TableBuilder.field(
                            "id", TableBuilder.dbType("INTEGER"), DataTypes.INT().notNull()),
                    TableBuilder.field(
                            "float_col", TableBuilder.dbType("FLOAT"), DataTypes.FLOAT()),
                    TableBuilder.field(
                            "double_col",
                            TableBuilder.dbType("DOUBLE PRECISION"),
                            DataTypes.DOUBLE()),
                    TableBuilder.field(
                            "decimal_col",
                            TableBuilder.dbType("NUMBER(10,4) NOT NULL"),
                            DataTypes.DECIMAL(10, 4).notNull()),
                    TableBuilder.field(
                            "binary_float_col",
                            TableBuilder.dbType("BINARY_FLOAT NOT NULL"),
                            DataTypes.FLOAT()),
                    TableBuilder.field(
                            "binary_double_col",
                            TableBuilder.dbType("BINARY_DOUBLE NOT NULL"),
                            DataTypes.DOUBLE()),
                    TableBuilder.field(
                            "char_col", TableBuilder.dbType("CHAR NOT NULL"), DataTypes.CHAR(1)),
                    TableBuilder.field(
                            "nchar_col",
                            TableBuilder.dbType("NCHAR(3) NOT NULL"),
                            DataTypes.VARCHAR(3)),
                    TableBuilder.field(
                            "varchar2_col",
                            TableBuilder.dbType("VARCHAR2(30) NOT NULL"),
                            DataTypes.VARCHAR(30)),
                    TableBuilder.field(
                            "date_col", TableBuilder.dbType("DATE NOT NULL"), DataTypes.DATE()),
                    TableBuilder.field(
                            "timestamp6_col",
                            TableBuilder.dbType("TIMESTAMP(6)"),
                            DataTypes.TIMESTAMP(6)),
                    TableBuilder.field(
                            "timestamp9_col",
                            TableBuilder.dbType("TIMESTAMP(9)"),
                            DataTypes.TIMESTAMP(9)),
                    TableBuilder.field("clob_col", TableBuilder.dbType("CLOB"), DataTypes.STRING()),
                    TableBuilder.field("blob_col", TableBuilder.dbType("BLOB"), DataTypes.BYTES()));

    private static final String INPUT_TABLE_NAME = INPUT_TABLE.getTableName();

    private static StreamExecutionEnvironment env;
    private static TableEnvironment tEnv;

    @Override
    public List<TableManaged> getManagedTables() {
        return Collections.singletonList(INPUT_TABLE);
    }

    @BeforeEach
    void before() throws Exception {
        try (Connection conn = getMetadata().getConnection()) {
            INPUT_TABLE.insertIntoTableValues(
                    conn,
                    "1, 1.12345, 2.12345678790, 100.1234, 1.175E-10, 1.79769E+40, 'a', 'abc', 'abcdef',"
                            + "TO_DATE('1997-01-01','yyyy-mm-dd'), TIMESTAMP '2020-01-01 15:35:00.123456',"
                            + "TIMESTAMP '2020-01-01 15:35:00.123456789', 'Hello World', hextoraw('453d7a34')");
            INPUT_TABLE.insertIntoTableValues(
                    conn,
                    "2, 1.12345, 2.12345678790, 101.1234, -1.175E-10, -1.79769E+40, 'a', 'abc', 'abcdef',"
                            + "TO_DATE('1997-01-02','yyyy-mm-dd'), TIMESTAMP '2020-01-01 15:36:01.123456',"
                            + "TIMESTAMP '2020-01-01 15:36:01.123456789', 'Hey Leonard', hextoraw('453d7a34')");
        }

        env = StreamExecutionEnvironment.getExecutionEnvironment();
        tEnv = StreamTableEnvironment.create(env);
    }

    @Test
    void testJdbcSource() {
        tEnv.executeSql(INPUT_TABLE.getCreateQueryForFlink(getMetadata(), INPUT_TABLE_NAME));
        Iterator<Row> collected = tEnv.executeSql("SELECT * FROM " + INPUT_TABLE_NAME).collect();
        List<String> result =
                CollectionUtil.iteratorToList(collected).stream()
                        .map(Row::toString)
                        .sorted()
                        .collect(Collectors.toList());
        List<String> expected =
                Stream.of(
                                "+I[1, 1.12345, 2.1234567879, 100.1234, 1.175E-10, 1.79769E40, a, abc, abcdef, 1997-01-01, 2020-01-01T15:35:00.123456, 2020-01-01T15:35:00.123456789, Hello World, [69, 61, 122, 52]]",
                                "+I[2, 1.12345, 2.1234567879, 101.1234, -1.175E-10, -1.79769E40, a, abc, abcdef, 1997-01-02, 2020-01-01T15:36:01.123456, 2020-01-01T15:36:01.123456789, Hey Leonard, [69, 61, 122, 52]]")
                        .sorted()
                        .collect(Collectors.toList());
        assertThat(result).isEqualTo(expected);
    }

    @Test
    void testProject() {
        tEnv.executeSql(
                INPUT_TABLE.getCreateQueryForFlink(
                        getMetadata(),
                        INPUT_TABLE_NAME,
                        Arrays.asList(
                                "id",
                                "timestamp6_col",
                                "timestamp9_col",
                                "binary_float_col",
                                "binary_double_col",
                                "decimal_col"),
                        Arrays.asList(
                                "'scan.partition.column'='id'",
                                "'scan.partition.num'='2'",
                                "'scan.partition.lower-bound'='0'",
                                "'scan.partition.upper-bound'='100'")));

        Iterator<Row> collected =
                tEnv.executeSql("SELECT id,timestamp6_col,decimal_col FROM " + INPUT_TABLE_NAME)
                        .collect();
        List<String> result =
                CollectionUtil.iteratorToList(collected).stream()
                        .map(Row::toString)
                        .sorted()
                        .collect(Collectors.toList());
        List<String> expected =
                Stream.of(
                                "+I[1, 2020-01-01T15:35:00.123456, 100.1234]",
                                "+I[2, 2020-01-01T15:36:01.123456, 101.1234]")
                        .collect(Collectors.toList());
        assertThat(result).containsExactlyInAnyOrderElementsOf(expected);
    }

    @Test
    void testLimit() {
        tEnv.executeSql(
                INPUT_TABLE.getCreateQueryForFlink(
                        getMetadata(),
                        INPUT_TABLE_NAME,
                        Arrays.asList(
                                "id",
                                "timestamp6_col",
                                "timestamp9_col",
                                "binary_float_col",
                                "binary_double_col",
                                "decimal_col"),
                        Arrays.asList(
                                "'scan.partition.column'='id'",
                                "'scan.partition.num'='2'",
                                "'scan.partition.lower-bound'='1'",
                                "'scan.partition.upper-bound'='2'")));

        Iterator<Row> collected =
                tEnv.executeSql("SELECT * FROM " + INPUT_TABLE_NAME + " LIMIT 1").collect();
        List<String> result =
                CollectionUtil.iteratorToList(collected).stream()
                        .map(Row::toString)
                        .sorted()
                        .collect(Collectors.toList());

        Set<String> expected = new HashSet<>();
        expected.add(
                "+I[1, 2020-01-01T15:35:00.123456, 2020-01-01T15:35:00.123456789, 1.175E-10, 1.79769E40, 100.1234]");
        expected.add(
                "+I[2, 2020-01-01T15:36:01.123456, 2020-01-01T15:36:01.123456789, -1.175E-10, -1.79769E40, 101.1234]");
        assertThat(result).hasSize(1);
        assertThat(expected)
                .as("The actual output is not a subset of the expected set.")
                .containsAll(result);
    }
}
