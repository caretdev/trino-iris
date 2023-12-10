package com.caretdev.trino.plugin.iris;

import io.trino.Session;
import io.trino.plugin.jdbc.BaseJdbcConnectorTest;
import io.trino.sql.planner.plan.TopNNode;
import io.trino.testing.QueryRunner;
import io.trino.testing.TestingConnectorBehavior;
import io.trino.testing.TestingNames;
import io.trino.testing.sql.SqlExecutor;
import org.testng.annotations.Test;

import java.util.Map;
import java.util.Optional;

import static com.caretdev.trino.plugin.iris.IRISQueryRunner.createIRISQueryRunner;
import static io.trino.testing.TestingConnectorBehavior.*;
import static org.assertj.core.api.Assertions.assertThat;

public class TestIRISConnectorTest extends BaseJdbcConnectorTest {

    protected TestingIRISServer irisServer;

    @Override
    protected boolean hasBehavior(TestingConnectorBehavior connectorBehavior) {
        return switch (connectorBehavior) {
            case SUPPORTS_AGGREGATION_PUSHDOWN,
                    SUPPORTS_JOIN_PUSHDOWN,
                    SUPPORTS_ADD_COLUMN_WITH_COMMENT,
                    SUPPORTS_AGGREGATION_PUSHDOWN_CORRELATION,
                    SUPPORTS_AGGREGATION_PUSHDOWN_COUNT_DISTINCT,
                    SUPPORTS_AGGREGATION_PUSHDOWN_COVARIANCE,
                    SUPPORTS_AGGREGATION_PUSHDOWN_REGRESSION,
                    SUPPORTS_ARRAY,
                    SUPPORTS_COMMENT_ON_COLUMN,
                    SUPPORTS_CREATE_TABLE_WITH_COLUMN_COMMENT,
//                    SUPPORTS_CREATE_TABLE_WITH_TABLE_COMMENT,
                    SUPPORTS_JOIN_PUSHDOWN_WITH_DISTINCT_FROM,
                    SUPPORTS_JOIN_PUSHDOWN_WITH_FULL_JOIN,
                    SUPPORTS_NEGATIVE_DATE,
                    SUPPORTS_PREDICATE_PUSHDOWN_WITH_VARCHAR_EQUALITY,
                    SUPPORTS_PREDICATE_PUSHDOWN_WITH_VARCHAR_INEQUALITY,
                    SUPPORTS_RENAME_SCHEMA,
                    SUPPORTS_ROW_TYPE -> false;
            default -> super.hasBehavior(connectorBehavior);
        };
    }

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception {
        irisServer = closeAfterClass(new TestingIRISServer());
        return IRISQueryRunner.createIRISQueryRunner(irisServer, Map.of(), Map.of(), REQUIRED_TPCH_TABLES);
    }

    @Override
    protected SqlExecutor onRemoteDatabase() {
        return irisServer::execute;
    }

    protected String tableDefinitionForAddColumn() {
//        Length is required, by default is just 1
        return "(x VARCHAR(4096))";
    }

    @Override
    protected Optional<String> filterColumnNameTestData(String columnName) {
        if ("a.dot".equals(columnName)
                || "a,comma".equals(columnName)
        ) {
            return Optional.empty();
        }

        return Optional.of(columnName);
    }

    @Override
    protected Optional<SetColumnTypeSetup> filterSetColumnTypesDataProvider(SetColumnTypeSetup setup)
    {
        switch ("%s -> %s".formatted(setup.sourceColumnType(), setup.newColumnType())) {
            case "varchar -> char(20)":
                return Optional.of(setup.asUnsupported());
        }
//        return Optional.of(setup);
        return Optional.of(setup.asUnsupported());
    }

    @Test
    public void testCreateTableAsSelectSchemaNotFound() {
//        not for IRIS
    }

    @Override
    protected Optional<DataMappingTestSetup> filterDataMappingSmokeTestData(DataMappingTestSetup dataMappingTestSetup)
    {
        String typeName = dataMappingTestSetup.getTrinoTypeName();
        if (typeName.equals("time(3)") || typeName.equals("time(6)")
                || typeName.equals("timestamp(3) with time zone")
                || typeName.equals("timestamp(6) with time zone")) {
            return Optional.of(dataMappingTestSetup.asUnsupported());
        }

        return Optional.of(dataMappingTestSetup);
    }

}
