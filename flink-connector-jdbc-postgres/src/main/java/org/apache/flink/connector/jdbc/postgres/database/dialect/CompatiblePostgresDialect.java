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

package org.apache.flink.connector.jdbc.postgres.database.dialect;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.types.logical.RowType;

import java.util.Optional;

/** JDBC dialect for PostgreSQL compatible databases. */
@Internal
public abstract class CompatiblePostgresDialect extends PostgresDialect {

    private static final long serialVersionUID = 1L;

    protected abstract String compatibleDialectName();

    protected abstract CompatiblePostgresDialectConverter compatibleRowConverter(RowType rowType);

    protected abstract Optional<String> compatibleDriverName();

    @Override
    public String dialectName() {
        return compatibleDialectName();
    }

    @Override
    public CompatiblePostgresDialectConverter getRowConverter(RowType rowType) {
        return compatibleRowConverter(rowType);
    }

    @Override
    public Optional<String> defaultDriverName() {
        return compatibleDriverName();
    }
}
