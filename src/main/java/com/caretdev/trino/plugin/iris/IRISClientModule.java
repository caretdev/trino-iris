package com.caretdev.trino.plugin.iris;

import com.google.common.base.Throwables;
import com.google.inject.*;

import com.google.inject.Module;
import com.intersystems.jdbc.IRISDriver;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.opentelemetry.api.OpenTelemetry;
import io.trino.plugin.base.classloader.ClassLoaderSafeConnectorMetadata;
import io.trino.plugin.base.classloader.ForClassLoaderSafe;
import io.trino.plugin.base.mapping.IdentifierMappingModule;
import io.trino.plugin.jdbc.*;
import io.trino.plugin.jdbc.credential.CredentialProvider;
import io.trino.plugin.jdbc.ptf.Query;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.function.table.ConnectorTableFunction;
import io.trino.plugin.jdbc.RetryingConnectionFactory.RetryStrategy;

import java.sql.SQLException;
import java.sql.SQLRecoverableException;
import java.util.Properties;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.inject.multibindings.Multibinder.newSetBinder;
import static com.google.inject.multibindings.OptionalBinder.newOptionalBinder;
import static io.airlift.configuration.ConfigBinder.configBinder;

public class IRISClientModule extends AbstractConfigurationAwareModule {

    @Override
    protected void setup(Binder binder) {
        configBinder(binder).bindConfig(IRISConfig.class);
        configBinder(binder).bindConfig(JdbcStatisticsConfig.class);

        binder.bind(JdbcClient.class).annotatedWith(ForBaseJdbc.class).to(IRISClient.class).in(Scopes.SINGLETON);
        newOptionalBinder(binder, JdbcMetadataFactory.class).setBinding().to(IRISMetadataFactory.class).in(Scopes.SINGLETON);

//        newSetBinder(binder, ConnectorTableFunction.class).addBinding().toProvider(Query.class).in(Scopes.SINGLETON);

        binder.install(new DecimalModule());
    }

    @Provides
    @Singleton
    @ForBaseJdbc
    public static ConnectionFactory connectionFactory(BaseJdbcConfig config, CredentialProvider credentialProvider, IRISConfig irisConfig, OpenTelemetry openTelemetry)
            throws SQLException {
        Properties connectionProperties = new Properties();

        if (irisConfig.isConnectionPoolEnabled()) {
            return new IRISConnectionFactory(
                    config.getConnectionUrl(),
                    connectionProperties,
                    credentialProvider,
                    irisConfig.getConnectionPoolMinSize(),
                    irisConfig.getConnectionPoolMaxSize(),
                    openTelemetry);
        }

        return new DriverConnectionFactory(
                new IRISDriver(),
                config.getConnectionUrl(),
                connectionProperties,
                credentialProvider,
                openTelemetry);
    }

    private static class IRISRetryStrategy
            implements RetryStrategy {
        @Override
        public boolean isExceptionRecoverable(Throwable exception) {
            return Throwables.getCausalChain(exception).stream()
                    .anyMatch(SQLRecoverableException.class::isInstance);
        }
    }

}
