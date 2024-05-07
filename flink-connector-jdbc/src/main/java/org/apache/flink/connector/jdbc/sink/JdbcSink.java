/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.connector.jdbc.sink;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.jdbc.JdbcExactlyOnceOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.core.datastream.sink.JdbcSinkBuilder;
import org.apache.flink.connector.jdbc.datasource.connections.JdbcConnectionProvider;
import org.apache.flink.connector.jdbc.datasource.statements.JdbcQueryStatement;

/**
 * Flink Sink to produce data into a jdbc database.
 *
 * @see JdbcSinkBuilder on how to construct a JdbcSink
 * @deprecated use #{@link org.apache.flink.connector.jdbc.core.datastream.sink.JdbcSink}
 */
@Deprecated
@PublicEvolving
public class JdbcSink<IN>
        extends org.apache.flink.connector.jdbc.core.datastream.sink.JdbcSink<IN> {

    public JdbcSink(
            DeliveryGuarantee deliveryGuarantee,
            JdbcConnectionProvider connectionProvider,
            JdbcExecutionOptions executionOptions,
            JdbcExactlyOnceOptions exactlyOnceOptions,
            JdbcQueryStatement<IN> queryStatement) {
        super(
                deliveryGuarantee,
                connectionProvider,
                executionOptions,
                exactlyOnceOptions,
                queryStatement);
    }
}
