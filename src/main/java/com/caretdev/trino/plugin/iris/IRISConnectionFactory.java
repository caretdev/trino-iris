package com.caretdev.trino.plugin.iris;

import com.intersystems.jdbc.IRISConnection;
import com.intersystems.jdbc.IRISConnectionPoolDataSource;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.instrumentation.jdbc.datasource.OpenTelemetryDataSource;
import io.trino.plugin.jdbc.ConnectionFactory;
import io.trino.plugin.jdbc.credential.CredentialProvider;
import io.trino.spi.connector.ConnectorSession;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.time.Duration;
import java.util.Optional;
import java.util.Properties;

public class IRISConnectionFactory implements ConnectionFactory {

    private final DataSource dataSource;
//    private final OpenTelemetryDataSource dataSource;

    public IRISConnectionFactory(
            String connectionUrl,
            Properties connectionProperties,
            CredentialProvider credentialProvider,
            int connectionPoolMinSize,
            int connectionPoolMaxSize,
            OpenTelemetry openTelemetry) throws SQLException {
        IRISConnectionPoolDataSource dataSource = new IRISConnectionPoolDataSource();
        dataSource.restartConnectionPool();
        dataSource.setURL(connectionUrl);

        dataSource.setMinPoolSize(connectionPoolMinSize);
        dataSource.setMaxPoolSize(connectionPoolMaxSize);

        credentialProvider.getConnectionUser(Optional.empty())
                .ifPresent(user -> {
                    dataSource.setUser(user);
                });
        credentialProvider.getConnectionPassword(Optional.empty())
                .ifPresent(password -> {
                    dataSource.setPassword(password);
                });
        this.dataSource = new OpenTelemetryDataSource(dataSource, openTelemetry);
    }

    @Override
    public Connection openConnection(ConnectorSession session) throws SQLException {
        Connection connection = dataSource.getConnection();
        return connection;
    }

}