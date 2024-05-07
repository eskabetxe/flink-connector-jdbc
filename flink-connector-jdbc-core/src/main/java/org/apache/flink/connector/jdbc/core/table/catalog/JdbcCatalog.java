package org.apache.flink.connector.jdbc.core.table.catalog;

import org.apache.flink.table.catalog.Catalog;

import java.io.Serializable;

/** Catalogs for relational databases via JDBC. */
public interface JdbcCatalog extends Catalog, Serializable {}
