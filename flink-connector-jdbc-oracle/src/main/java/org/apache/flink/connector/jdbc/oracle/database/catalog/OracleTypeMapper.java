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

package org.apache.flink.connector.jdbc.oracle.database.catalog;

import org.apache.flink.connector.jdbc.core.database.catalog.JdbcCatalogTypeMapper;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.DecimalType;

import oracle.jdbc.OracleTypes;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;

/** Oracle type mapper. */
public class OracleTypeMapper implements JdbcCatalogTypeMapper {

    private final String databaseVersion;

    private final String driverVersion;

    public OracleTypeMapper(String databaseVersion, String driverVersion) {
        this.databaseVersion = databaseVersion;
        this.driverVersion = driverVersion;
    }

    @Override
    public DataType mapping(ObjectPath tablePath, ResultSetMetaData metaData, int colIndex)
            throws SQLException {
        int jdbcType = metaData.getColumnType(colIndex);
        String columnName = metaData.getColumnName(colIndex);
        String oracleType = metaData.getColumnTypeName(colIndex).toUpperCase();
        int precision = metaData.getPrecision(colIndex);
        int scale = metaData.getScale(colIndex);

        switch (jdbcType) {
            case Types.CHAR:
            case Types.VARCHAR:
            case Types.NCHAR:
            case Types.NVARCHAR:
            case Types.STRUCT:
            case Types.CLOB:
                return DataTypes.STRING();
            case Types.BLOB:
                return DataTypes.BYTES();
            case Types.INTEGER:
            case Types.SMALLINT:
            case Types.TINYINT:
                return DataTypes.INT();
            case Types.FLOAT:
            case Types.REAL:
            case OracleTypes.BINARY_FLOAT:
                return DataTypes.FLOAT();
            case Types.DOUBLE:
            case OracleTypes.BINARY_DOUBLE:
                return DataTypes.DOUBLE();
            case Types.NUMERIC:
            case Types.DECIMAL:
                if (precision > 0 && precision < DecimalType.MAX_PRECISION) {
                    return DataTypes.DECIMAL(precision, metaData.getScale(colIndex));
                }
                return DataTypes.DECIMAL(DecimalType.MAX_PRECISION, 18);
            case Types.DATE:
                return DataTypes.DATE();
            case Types.TIMESTAMP:
            case Types.TIMESTAMP_WITH_TIMEZONE:
            case OracleTypes.TIMESTAMPTZ:
            case OracleTypes.TIMESTAMPLTZ:
                return scale > 0 ? DataTypes.TIMESTAMP(scale) : DataTypes.TIMESTAMP();
            case OracleTypes.INTERVALYM:
                return DataTypes.INTERVAL(DataTypes.YEAR(), DataTypes.MONTH());
            case OracleTypes.INTERVALDS:
                return DataTypes.INTERVAL(DataTypes.DAY(), DataTypes.SECOND());
            case Types.BOOLEAN:
                return DataTypes.BOOLEAN();
            default:
                final String jdbcColumnName = metaData.getColumnName(colIndex);
                throw new UnsupportedOperationException(
                        String.format(
                                "Doesn't support Oracle type '%s' on column '%s' in Oracle version %s, driver version %s yet.",
                                oracleType, jdbcColumnName, databaseVersion, driverVersion));
        }
    }
}
