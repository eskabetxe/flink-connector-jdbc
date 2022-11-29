package org.apache.flink.connector.jdbc.dialect.trino;

import org.apache.flink.connector.jdbc.internal.JdbcTableOutputFormatTest;
import org.apache.flink.test.util.AbstractTestBase;
import org.apache.flink.types.Row;

import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.TrinoContainer;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.MountableFile;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

/** Basic class for testing Trino jdbc. */
@Testcontainers
public abstract class TrinoTestBase extends AbstractTestBase {

    private static final String TRINO_DOCKER_IMAGE = "trinodb/trino:403";
    private static final String MYSQL_DOCKER_IMAGE = "mysql:8.0.32";

    public static final Network NETWORK = Network.newNetwork();

    private final static File file = new File("target/mysql.properties");
    protected static final MySQLContainer<?> MYSQL_CONTAINER =
            new MySQLContainer<>(DockerImageName.parse(MYSQL_DOCKER_IMAGE))
                    .withNetwork(NETWORK)
                    .withNetworkAliases("mysql");

    protected static final TrinoContainer TRINO_CONTAINER =
            new TrinoContainer(TRINO_DOCKER_IMAGE)
                    .withCopyFileToContainer(MountableFile.forHostPath(file.getPath()), "/etc/trino/catalog/mysql.properties")
                    .withDatabaseName("mysql/test")
                    .withNetwork(NETWORK)
                    .withNetworkAliases("trino")
                    .dependsOn(MYSQL_CONTAINER);

    static {
        try {
            MYSQL_CONTAINER.start();
            FileWriter myWriter = new FileWriter(file);
            myWriter.write("connector.name=mysql\n" +
                    String.format("connection-url=%s\n", String.format("jdbc:mysql://%s:%s", "mysql", 3306)) +
                    String.format("connection-user=%s\n",MYSQL_CONTAINER.getUsername() ) +
                    String.format("connection-password=%s\n",MYSQL_CONTAINER.getPassword() ) );
            myWriter.close();
            TRINO_CONTAINER.start();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static Connection getConnection() throws SQLException, ClassNotFoundException {
        Class.forName(TRINO_CONTAINER.getDriverClassName());
        return DriverManager.getConnection(getJdbcUrl(), getUsername(), getPassword());
    }

    public static String getJdbcUrl() {
        return TRINO_CONTAINER.getJdbcUrl();
    }

    public static String getUsername() {
        return TRINO_CONTAINER.getUsername();
    }

    public static String getPassword() {
        return TRINO_CONTAINER.getPassword();
    }

    public static void check(Row[] rows, String table, String[] fields)
            throws SQLException, ClassNotFoundException {
        JdbcTableOutputFormatTest.check(rows, getConnection(), table, fields);
    }
}
