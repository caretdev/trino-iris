package com.caretdev.trino.plugin.iris;

import com.google.common.collect.ImmutableSet;
import com.google.inject.Inject;
import io.trino.plugin.jdbc.DefaultJdbcMetadataFactory;
import io.trino.plugin.jdbc.JdbcClient;
import io.trino.plugin.jdbc.JdbcMetadata;
import io.trino.plugin.jdbc.JdbcQueryEventListener;

import java.util.Set;

import static java.util.Objects.requireNonNull;

public class IRISMetadataFactory extends DefaultJdbcMetadataFactory {
    private final Set<JdbcQueryEventListener> jdbcQueryEventListeners;

    @Inject
    public IRISMetadataFactory(JdbcClient jdbcClient, Set<JdbcQueryEventListener> jdbcQueryEventListeners)
    {
        super(jdbcClient, jdbcQueryEventListeners);
        this.jdbcQueryEventListeners = ImmutableSet.copyOf(requireNonNull(jdbcQueryEventListeners, "jdbcQueryEventListeners is null"));
    }

    @Override
    protected JdbcMetadata create(JdbcClient transactionCachingJdbcClient)
    {
        return new IRISMetadata(transactionCachingJdbcClient, jdbcQueryEventListeners);
    }

}
