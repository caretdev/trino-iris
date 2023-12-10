package com.caretdev.trino.plugin.iris;


import com.caretdev.testcontainers.IRISContainer;
import io.trino.testing.ResourcePresence;

import java.io.Closeable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

import static io.trino.testing.containers.TestContainers.exposeFixedPorts;
import static io.trino.testing.containers.TestContainers.startOrReuse;
import static java.lang.String.format;

public class TestingIRISServer implements AutoCloseable {

    private static final String USER = "trinouser";
    private static final String PASSWORD = "test";
    private static final String DATABASE = "TRINO";

    private final IRISContainer<?> dockerContainer;
    private final Closeable cleanup;

    public TestingIRISServer() {
        this(false);
    }

    public TestingIRISServer(boolean shouldExposeFixedPorts) {

//        "containers.intersystems.com/intersystems/iris:2023.3"
//        "intersystemsdc/iris-community:2023.3-zpm"
        dockerContainer = new IRISContainer("containers.intersystems.com/intersystems/iris:2023.3")
                .withDatabaseName(DATABASE)
                .withUsername(USER)
                .withPassword(PASSWORD)
                .withLicenseKey(System.getProperty("user.home") + "/iris.key")
        ;
        if (shouldExposeFixedPorts) {
            exposeFixedPorts(dockerContainer);
        }

        cleanup = startOrReuse(dockerContainer);
    }

    public void execute(String sql) {
        try (Connection connection = createConnection();
             Statement statement = connection.createStatement()) {
            statement.execute(sql);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public Connection createConnection()
            throws SQLException {
        return dockerContainer.createConnection("");
    }


    public String getUser() {
        return USER;
    }

    public String getPassword() {
        return PASSWORD;
    }

    public Properties getProperties() {
        Properties properties = new Properties();
        properties.setProperty("user", USER);
        properties.setProperty("password", PASSWORD);
        return properties;
    }

    public String getJdbcUrl() {
        return dockerContainer.getJdbcUrl();
    }

    @Override
    public void close() {
        try {
            cleanup.close();
        } catch (IOException ioe) {
            throw new UncheckedIOException(ioe);
        }
    }

    @ResourcePresence
    public boolean isRunning() {
        return dockerContainer.getContainerId() != null;
    }

}
