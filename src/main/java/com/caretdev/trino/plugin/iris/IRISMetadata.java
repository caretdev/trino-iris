package com.caretdev.trino.plugin.iris;

import com.google.inject.Inject;
import io.trino.plugin.base.mapping.IdentifierMapping;
import io.trino.plugin.jdbc.DefaultJdbcMetadata;
import io.trino.plugin.jdbc.JdbcClient;
import io.trino.plugin.jdbc.JdbcQueryEventListener;
import io.trino.spi.connector.ConnectorMaterializedViewDefinition;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.security.TrinoPrincipal;

import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static java.util.Objects.requireNonNull;

public class IRISMetadata extends DefaultJdbcMetadata {

    private final JdbcClient irisClient;

    @Inject
    public IRISMetadata(JdbcClient irisClient, Set<JdbcQueryEventListener> jdbcQueryEventListeners) {
        super(irisClient, false, jdbcQueryEventListeners);
        this.irisClient = requireNonNull(irisClient, "irisClient is null");
    }

    @Override
    public boolean schemaExists(ConnectorSession session, String schemaName) {
//        IRIS does not create schemas, all schemas exists
        return true;
//        return irisClient.schemaExists(session, schemaName);
    }

    @Override
    public void createSchema(ConnectorSession session, String schemaName, Map<String, Object> properties, TrinoPrincipal owner)
    {
//        Do nothing
        assert true;
    }

    @Override
    public Optional<ConnectorMaterializedViewDefinition> getMaterializedView(ConnectorSession session, SchemaTableName viewName) {
        return super.getMaterializedView(session, viewName);
    }
}
