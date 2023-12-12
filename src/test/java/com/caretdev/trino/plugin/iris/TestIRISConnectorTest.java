package com.caretdev.trino.plugin.iris;

import io.trino.plugin.jdbc.BaseJdbcConnectorTest;
import io.trino.testing.QueryRunner;
import io.trino.testing.TestingConnectorBehavior;
import io.trino.testing.sql.SqlExecutor;
import org.intellij.lang.annotations.Language;
import org.testng.annotations.Test;

import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;

import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;

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
//                    SUPPORTS_CREATE_TABLE_WITH_COLUMN_COMMENT,
                    SUPPORTS_COMMENT_ON_TABLE,
                    SUPPORTS_JOIN_PUSHDOWN_WITH_DISTINCT_FROM,
                    SUPPORTS_JOIN_PUSHDOWN_WITH_FULL_JOIN,
                    SUPPORTS_NEGATIVE_DATE,
                    SUPPORTS_PREDICATE_PUSHDOWN_WITH_VARCHAR_EQUALITY,
                    SUPPORTS_PREDICATE_PUSHDOWN_WITH_VARCHAR_INEQUALITY,
                    SUPPORTS_RENAME_SCHEMA,
                    SUPPORTS_CREATE_SCHEMA,
                    SUPPORTS_RENAME_TABLE_ACROSS_SCHEMAS,
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
    protected Optional<SetColumnTypeSetup> filterSetColumnTypesDataProvider(SetColumnTypeSetup setup) {
        switch (setup.newColumnType()) {
            case "decimal(38,3)": // -> decimal(22,3)
            case "varchar": // -> varchar(65535)
            case "char(20)": // -> varchar(20)
            case "timestamp(3)": // -> timestamp(3)
            case "timestamp(6)": // -> timestamp(3)
                return Optional.empty();
        }
        if (setup.sourceColumnType().startsWith("time(")) {
//            No milliseconds
            return Optional.of(setup.withNewValueLiteral("TIME '15:03:00'"));
        }
        return Optional.of(setup);
    }

    @Override
    public void testCreateTableAsSelectSchemaNotFound() {
//        not for IRIS
    }

    @Override
    public void testCaseSensitiveDataMapping(DataMappingTestSetup dataMappingTestSetup) {

    }


    @Override
    protected Optional<DataMappingTestSetup> filterDataMappingSmokeTestData(DataMappingTestSetup dataMappingTestSetup) {
        String typeName = dataMappingTestSetup.getTrinoTypeName();
        if (typeName.equals("time(3)") || typeName.equals("time(6)")
                || typeName.equals("timestamp(3) with time zone")
                || typeName.equals("timestamp(6) with time zone")) {
            return Optional.of(dataMappingTestSetup.asUnsupported());
        }

        return Optional.of(dataMappingTestSetup);
    }

    @Override
    protected OptionalInt maxSchemaNameLength() {
        return OptionalInt.of(128);
    }

    @Override
    protected OptionalInt maxTableNameLength() {
        return OptionalInt.of(512);
    }

    @Override
    public void testCreateTableWithLongColumnName() {
//        IRIS does not have fixed max length
    }

    @Override
    public void testCreateTableWithLongTableName() {
//        IRIS does not have fixed max length
    }

    @Override
    public void testRenameTableToLongTableName() {
//        IRIS does not have fixed max length
    }

    @Override
    @Language("RegExp")
    protected String errorMessageForInsertNegativeDate(String date) {
        return ".*SQLCODE: <-104>:<Field validation failed in INSERT>.*\n.* < MIN .*";
    }


    @Test
    public void testRenameSchema() {

    }

    @Override
    public void testCreateTableSchemaNotFound() {
        assertThatThrownBy(super::testCreateTableSchemaNotFound)
                .hasMessageContaining("Expected query to fail: CREATE TABLE test_schema_");

    }

    @Override
    public void testCreateSchema() {
        assertThatThrownBy(super::testCreateTableSchemaNotFound)
                .hasMessageContaining("Expected query to fail: CREATE TABLE test_schema_");
    }
}
