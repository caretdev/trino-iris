package com.caretdev.trino.plugin.iris;


import com.google.common.collect.ImmutableMap;
import io.airlift.log.Logger;
import io.trino.Session;
import io.trino.plugin.jmx.JmxPlugin;
import io.trino.plugin.tpch.TpchPlugin;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;
import io.trino.tpch.TpchTable;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;

import static io.airlift.testing.Closeables.closeAllSuppress;
import static io.trino.plugin.tpch.TpchMetadata.TINY_SCHEMA_NAME;
import static io.trino.testing.QueryAssertions.copyTpchTables;
import static io.trino.testing.TestingSession.testSessionBuilder;

public class IRISQueryRunner {

    public IRISQueryRunner() {
    }

    public static final String TPCH_SCHEMA = "tpch";

    public static DistributedQueryRunner createIRISQueryRunner(
            TestingIRISServer server,
            Map<String, String> extraProperties,
            Map<String, String> connectorProperties,
            Iterable<TpchTable<?>> tables)
            throws Exception {
        return createIRISQueryRunner(server, extraProperties, Map.of(), connectorProperties, tables, runner -> {
        });
    }

    public static DistributedQueryRunner createIRISQueryRunner(
            TestingIRISServer server,
            Map<String, String> extraProperties,
            Map<String, String> coordinatorProperties,
            Map<String, String> connectorProperties,
            Iterable<TpchTable<?>> tables,
            Consumer<QueryRunner> moreSetup)
            throws Exception {

        DistributedQueryRunner queryRunner = null;
        try {
            queryRunner = DistributedQueryRunner.builder(createSession())
                    .setExtraProperties(extraProperties)
                    .setCoordinatorProperties(coordinatorProperties)
                    .setAdditionalSetup(moreSetup)
                    .build();

            queryRunner.installPlugin(new TpchPlugin());
            queryRunner.createCatalog("tpch", "tpch");

            // note: additional copy via ImmutableList so that if fails on nulls
            connectorProperties = new HashMap<>(ImmutableMap.copyOf(connectorProperties));
            connectorProperties.putIfAbsent("connection-url", server.getJdbcUrl());
            connectorProperties.putIfAbsent("connection-user", server.getUser());
            connectorProperties.putIfAbsent("connection-password", server.getPassword());
//            connectorProperties.putIfAbsent("iris.include-system-tables", "true");

            queryRunner.installPlugin(new IRISPlugin());
            queryRunner.createCatalog("iris", "iris", connectorProperties);

            copyTpchTables(queryRunner, "tpch", TINY_SCHEMA_NAME, createSession(), tables);

            return queryRunner;
        } catch (Throwable e) {
            closeAllSuppress(e, queryRunner, server);
            throw e;
        }
    }

    public static Session createSession() {
        return testSessionBuilder()
                .setCatalog("iris")
                .setSchema(TPCH_SCHEMA)
                .build();
    }

    public static void main(String[] args)
            throws Exception {
        DistributedQueryRunner queryRunner = createIRISQueryRunner(
                new TestingIRISServer(true),
                ImmutableMap.of("http-server.http.port", "8080"),
                ImmutableMap.of(),
                TpchTable.getTables());

        queryRunner.installPlugin(new JmxPlugin());
        queryRunner.createCatalog("jmx", "jmx");

        Logger log = Logger.get(IRISQueryRunner.class);
        log.info("======== SERVER STARTED ========");
        log.info("\n====\n%s\n====", queryRunner.getCoordinator().getBaseUrl());
    }
}
