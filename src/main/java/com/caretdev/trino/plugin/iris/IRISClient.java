/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.caretdev.trino.plugin.iris;


import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Inject;
import io.trino.plugin.base.aggregation.AggregateFunctionRewriter;
import io.trino.plugin.base.aggregation.AggregateFunctionRule;
import io.trino.plugin.base.expression.ConnectorExpressionRewriter;
import io.trino.plugin.base.mapping.IdentifierMapping;
import io.trino.plugin.jdbc.*;
import io.trino.plugin.jdbc.aggregation.*;
import io.trino.plugin.jdbc.expression.JdbcConnectorExpressionRewriterBuilder;
import io.trino.plugin.jdbc.expression.ParameterizedExpression;
import io.trino.plugin.jdbc.expression.RewriteComparison;
import io.trino.plugin.jdbc.expression.RewriteIn;
import io.trino.plugin.jdbc.logging.RemoteQueryModifier;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.*;
import io.trino.spi.expression.ConnectorExpression;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Verify.verify;
import static io.trino.plugin.jdbc.StandardColumnMappings.*;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;

import io.trino.spi.statistics.TableStatistics;
import io.trino.spi.type.*;

import javax.swing.text.html.Option;
import java.sql.*;
import java.util.*;
import java.util.function.BiFunction;
import java.util.stream.Stream;

import static io.trino.plugin.jdbc.JdbcErrorCode.JDBC_ERROR;
import static io.trino.plugin.jdbc.PredicatePushdownController.FULL_PUSHDOWN;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.CharType.createCharType;
import static io.trino.spi.type.DateTimeEncoding.packDateTimeWithZone;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DecimalType.createDecimalType;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TimeType.createTimeType;
import static io.trino.spi.type.TimestampType.createTimestampType;
import static io.trino.spi.type.Timestamps.PICOSECONDS_PER_DAY;
import static io.trino.spi.type.Timestamps.round;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static io.trino.spi.type.VarcharType.createUnboundedVarcharType;
import static io.trino.spi.type.VarcharType.createVarcharType;

import static java.lang.Math.max;
import static java.lang.String.format;
import static java.lang.String.join;
import static java.math.RoundingMode.UNNECESSARY;
import static java.util.stream.Collectors.joining;
import static com.google.common.collect.Iterables.getOnlyElement;

public class IRISClient
        extends BaseJdbcClient {


    public static final int VARCHAR_UNBOUNDED_LENGTH = 65535;
    private static final int MAX_SUPPORTED_DATE_TIME_PRECISION = 9;

    // IRIS don't use CREATE SCHEMA, all schemas creates when needed
    String schemaTableNameOverrideExist;

    private final ConnectorExpressionRewriter<ParameterizedExpression> connectorExpressionRewriter;
    private final AggregateFunctionRewriter<JdbcExpression, ?> aggregateFunctionRewriter;

    @Inject
    public IRISClient(
            BaseJdbcConfig config,
            JdbcStatisticsConfig statisticsConfig,
            ConnectionFactory connectionFactory,
            QueryBuilder queryBuilder,
            TypeManager typeManager,
            IdentifierMapping identifierMapping,
            RemoteQueryModifier queryModifier) throws SQLException {
        super("\"", connectionFactory, queryBuilder, config.getJdbcTypesMappedToVarchar(), identifierMapping, queryModifier, true);

        this.connectorExpressionRewriter = JdbcConnectorExpressionRewriterBuilder.newBuilder()
                .addStandardRules(this::quoted)
                .add(new RewriteComparison(ImmutableSet.of(RewriteComparison.ComparisonOperator.EQUAL, RewriteComparison.ComparisonOperator.NOT_EQUAL)))
                .add(new RewriteIn())
                .withTypeClass("integer_type", ImmutableSet.of("tinyint", "smallint", "integer", "bigint"))
                .map("$add(left: integer_type, right: integer_type)").to("left + right")
                .map("$subtract(left: integer_type, right: integer_type)").to("left - right")
                .map("$multiply(left: integer_type, right: integer_type)").to("left * right")
                .map("$divide(left: integer_type, right: integer_type)").to("left / right")
                .map("$modulus(left: integer_type, right: integer_type)").to("left % right")
                .map("$negate(value: integer_type)").to("-value")
                .map("$like(value: varchar, pattern: varchar): boolean").to("value LIKE pattern")
                .map("$like(value: varchar, pattern: varchar, escape: varchar(1)): boolean").to("value LIKE pattern ESCAPE escape")
                .map("$not($is_null(value))").to("value IS NOT NULL")
                .map("$not(value: boolean)").to("NOT value")
                .map("$is_null(value)").to("value IS NULL")
                .map("$nullif(first, second)").to("NULLIF(first, second)")
                .build();

        JdbcTypeHandle bigintTypeHandle = new JdbcTypeHandle(Types.BIGINT, Optional.of("bigint"), Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty());
        this.aggregateFunctionRewriter = new AggregateFunctionRewriter<>(
                this.connectorExpressionRewriter,
                ImmutableSet.<AggregateFunctionRule<JdbcExpression, ParameterizedExpression>>builder()
                        .add(new ImplementCountAll(bigintTypeHandle))
                        .add(new ImplementMinMax(false))
                        .add(new ImplementCount(bigintTypeHandle))
                        .add(new ImplementCountDistinct(bigintTypeHandle, false))
                        .add(new ImplementSum(IRISClient::toTypeHandle))
                        .add(new ImplementAvgFloatingPoint())
                        .add(new ImplementAvgDecimal())
//                        .add(new ImplementStddevSamp())
//                        .add(new ImplementStddevPop())
//                        .add(new ImplementVarianceSamp())
//                        .add(new ImplementVariancePop())
//                        .add(new ImplementCovarianceSamp())
//                        .add(new ImplementCovariancePop())
//                        .add(new ImplementCorr())
//                        .add(new ImplementRegrIntercept())
//                        .add(new ImplementRegrSlope())
                        .build());
    }

    @Override
    public boolean schemaExists(ConnectorSession session, String schema) {
        return true;
//        return getSchemaNames(session).contains(schema);
    }



    @Override
    protected void createSchema(ConnectorSession session, Connection connection, String remoteSchemaName) throws SQLException {
        assert true;
    }

    @Override
    public void createTable(ConnectorSession session, ConnectorTableMetadata tableMetadata) {
        SchemaTableName schemaTableName = tableMetadata.getTable();
        schemaTableNameOverrideExist = schemaTableName.getSchemaName();

        super.createTable(session, tableMetadata);
    }

    @Override
    public JdbcOutputTableHandle beginCreateTable(ConnectorSession session, ConnectorTableMetadata tableMetadata) {
        SchemaTableName schemaTableName = tableMetadata.getTable();
        schemaTableNameOverrideExist = schemaTableName.getSchemaName();

        return super.beginCreateTable(session, tableMetadata);
    }

    @Override
    protected List<String> createTableSqls(RemoteTableName remoteTableName, List<String> columns, ConnectorTableMetadata tableMetadata) {
        checkArgument(tableMetadata.getProperties().isEmpty(), "Unsupported table properties: %s", tableMetadata.getProperties());
        ImmutableList.Builder<String> createTableSqlsBuilder = ImmutableList.builder();
        Optional<String> tableComment = tableMetadata.getComment();
        String comment = "";
        if (tableComment.isPresent()) {
            comment = format("%%Description %s,", tableComment.map(BaseJdbcClient::varcharLiteral).orElse("NULL"));
        }
        createTableSqlsBuilder.add(
                format("CREATE TABLE %s (%s %s)",
                        quoted(remoteTableName),
                        comment,
                        join(", ", columns)
                ));
        return createTableSqlsBuilder.build();
    }

    @Override
    public Collection<String> listSchemas(Connection connection) {
        ImmutableSet.Builder<String> schemaNames = ImmutableSet.builder();
        schemaNames.addAll(super.listSchemas(connection).stream().filter(
                schema ->
                        !schema.startsWith("%")
                                && !schema.startsWith("ens.")
                                && !schema.startsWith("enslib_")
                                && !schema.startsWith("hsfhir_")
                                && !schema.startsWith("hs_")
        ).toList());
        if (schemaTableNameOverrideExist != null) {
            schemaNames.add(schemaTableNameOverrideExist);
        }
//        schemaNames.add("sqluser");
//        schemaNames.add("tpch");
        return schemaNames.build();
    }

    @Override
    public void renameSchema(ConnectorSession session, String schemaName, String newSchemaName) {
        throw new TrinoException(NOT_SUPPORTED, "This connector does not support renaming schemas");
    }

    private static ColumnMapping charColumnMapping(int charLength) {
        if (charLength > CharType.MAX_LENGTH) {
            return varcharColumnMapping(charLength);
        }
        CharType charType = createCharType(charLength);
        return ColumnMapping.sliceMapping(
                charType,
                charReadFunction(charType),
                charWriteFunction(),
                FULL_PUSHDOWN);
    }

    private static ColumnMapping varcharColumnMapping(int varcharLength) {
        VarcharType varcharType = varcharLength <= VarcharType.MAX_LENGTH
                ? createVarcharType(varcharLength)
                : createUnboundedVarcharType();
        return ColumnMapping.sliceMapping(
                varcharType,
                varcharReadFunction(varcharType),
                varcharWriteFunction(),
                FULL_PUSHDOWN);
    }

    @Override
    public Optional<ColumnMapping> toColumnMapping(ConnectorSession session, Connection connection, JdbcTypeHandle typeHandle) {
        String jdbcTypeName = typeHandle.getJdbcTypeName()
                .orElseThrow(() -> new TrinoException(JDBC_ERROR, "Type name is missing: " + typeHandle));

        Optional<ColumnMapping> mapping = getForcedMappingToVarchar(typeHandle);
        if (mapping.isPresent()) {
            return mapping;
        }
        switch (typeHandle.getJdbcType()) {
            case Types.BOOLEAN:
            case Types.BIT:
                return Optional.of(booleanColumnMapping());

            case Types.SMALLINT:
                return Optional.of(smallintColumnMapping());

            case Types.TINYINT:
                return Optional.of(tinyintColumnMapping());

            case Types.INTEGER:
                return Optional.of(integerColumnMapping());

            case Types.BIGINT:
                return Optional.of(bigintColumnMapping());

            case Types.REAL:
                return Optional.of(realColumnMapping());
            case Types.DECIMAL:
            case Types.NUMERIC:
                int columnSize = typeHandle.getRequiredColumnSize();
                int decimalDigits = typeHandle.getRequiredDecimalDigits();
                int precision = columnSize + max(-decimalDigits, 0);
                if (precision > Decimals.MAX_PRECISION) {
                    break;
                }
                return Optional.of(decimalColumnMapping(createDecimalType(precision, max(decimalDigits, 0)), UNNECESSARY));
            case Types.DOUBLE:
                return Optional.of(doubleColumnMapping());

            case Types.DATE:
                return Optional.of(dateColumnMappingUsingSqlDate());
//                return Optional.of(dateColumnMappingUsingLocalDate());

            case Types.CHAR:
                return Optional.of(charColumnMapping(typeHandle.getRequiredColumnSize()));

            case Types.VARCHAR:
            case Types.NVARCHAR:
            case Types.LONGVARCHAR:
            case Types.LONGNVARCHAR:
                return Optional.of(varcharColumnMapping(typeHandle.getRequiredColumnSize()));

            case Types.BINARY:
            case Types.VARBINARY:
            case Types.LONGVARBINARY:
                return Optional.of(varbinaryColumnMapping());
            case Types.TIME:
                return Optional.of(timeColumnMapping(createTimeType(typeHandle.getRequiredDecimalDigits())));
            case Types.TIMESTAMP:
                TimestampType timestampType = createTimestampType(typeHandle.getRequiredDecimalDigits());
                return Optional.of(timestampColumnMappingUsingSqlTimestampWithRounding(timestampType));
        }

        return Optional.empty();
    }

    @Override
    public WriteMapping toWriteMapping(ConnectorSession session, Type type) {

        if (type == BOOLEAN) {
            return WriteMapping.booleanMapping("bit", booleanWriteFunction());
        }
        if (type == TINYINT) {
            return WriteMapping.longMapping("tinyint", tinyintWriteFunction());
        }
        if (type == SMALLINT) {
            return WriteMapping.longMapping("smallint", smallintWriteFunction());
        }
        if (type == INTEGER) {
            return WriteMapping.longMapping("integer", integerWriteFunction());
        }
        if (type == BIGINT) {
            return WriteMapping.longMapping("bigint", bigintWriteFunction());
        }

        if (type == REAL) {
            return WriteMapping.longMapping("real", realWriteFunction());
        }
        if (type == DOUBLE) {
            return WriteMapping.doubleMapping("double precision", doubleWriteFunction());
        }

        if (VARBINARY.equals(type)) {
            return WriteMapping.sliceMapping("varbinary(max)", varbinaryWriteFunction());
        }

        if (type instanceof DecimalType decimalType) {
            String dataType = format("decimal(%s, %s)", decimalType.getPrecision(), decimalType.getScale());
            if (decimalType.isShort()) {
                return WriteMapping.longMapping(dataType, shortDecimalWriteFunction(decimalType));
            }
            return WriteMapping.objectMapping(dataType, longDecimalWriteFunction(decimalType));
        }

        if (type instanceof VarbinaryType) {
            return WriteMapping.sliceMapping("varbinary(max)", varbinaryWriteFunction());
        }

        if (type instanceof VarcharType varcharType) {
            String dataType;
            if (varcharType.isUnbounded()) {
                dataType = "varchar(" + VARCHAR_UNBOUNDED_LENGTH + ")";
            } else {
                dataType = "varchar(" + varcharType.getBoundedLength() + ")";
            }
            return WriteMapping.sliceMapping(dataType, varcharWriteFunction());
        }

        if (type == DATE) {
            return WriteMapping.longMapping("date", dateWriteFunctionUsingSqlDate());
//            return WriteMapping.longMapping("date", dateWriteFunctionUsingLocalDate());
        }
        if (type instanceof TimeType timeType) {
//            return WriteMapping.longMapping("time", timeWriteFunctionUsingSqlTime());
            verify(timeType.getPrecision() <= 6);
            return WriteMapping.longMapping(format("time(%s)", timeType.getPrecision()), irisTimeWriteFunction(timeType.getPrecision()));
        }
        if (type instanceof TimestampType timestampType) {
            return WriteMapping.longMapping("timestamp", timestampWriteFunctionUsingSqlTimestamp(timestampType));
        }

        throw new TrinoException(NOT_SUPPORTED, "Unsupported column type: " + type.getDisplayName());
    }

    private static LongWriteFunction irisTimeWriteFunction(int precision)
    {
        checkArgument(precision <= 12, "Unsupported precision: %s", precision);

        return LongWriteFunction.of(Types.TIME, (statement, index, picosOfDay) -> {
            if (picosOfDay == PICOSECONDS_PER_DAY) {
                picosOfDay = 0;
            }
            statement.setObject(index, picosOfDay / 1e12);
        });
    }

    private static Optional<JdbcTypeHandle> toTypeHandle(DecimalType decimalType) {
        return Optional.of(new JdbcTypeHandle(Types.NUMERIC, Optional.of("decimal"), Optional.of(decimalType.getPrecision()), Optional.of(decimalType.getScale()), Optional.empty(), Optional.empty()));
    }

    @Override
    protected Optional<BiFunction<String, Long, String>> limitFunction() {
        return Optional.of((sql, limit) -> format("SELECT TOP %s * FROM (%s) o", limit, sql));
    }

    @Override
    public boolean isLimitGuaranteed(ConnectorSession session) {
        return true;
    }

    @Override
    protected Optional<TopNFunction> topNFunction() {
        return Optional.of((query, sortItems, limit) -> {
            String orderBy = sortItems.stream()
                    .flatMap(sortItem -> {
                        String ordering = sortItem.getSortOrder().isAscending() ? "ASC" : "DESC";
                        String columnSorting = format("%s %s", quoted(sortItem.getColumn().getColumnName()), ordering);

                        return Stream.of(columnSorting);
//                        switch (sortItem.getSortOrder()) {
//                            case ASC_NULLS_FIRST:
//                            case DESC_NULLS_LAST:
//                                return Stream.of(columnSorting);
//
//                            case ASC_NULLS_LAST:
//                                return Stream.of(
//                                        format("ISNULL(%s) ASC", quoted(sortItem.getColumn().getColumnName())),
//                                        columnSorting);
//                            case DESC_NULLS_FIRST:
//                                return Stream.of(
//                                        format("ISNULL(%s) DESC", quoted(sortItem.getColumn().getColumnName())),
//                                        columnSorting);
//                        }
//                        throw new UnsupportedOperationException("Unsupported sort order: " + sortItem.getSortOrder());
                    })
                    .collect(joining(", "));
            return format("SELECT TOP %s %s ORDER BY %s", limit, query.substring(7), orderBy);
        });
    }

    @Override
    public List<JdbcColumnHandle> getColumns(ConnectorSession session, JdbcTableHandle tableHandle) {
        List<JdbcColumnHandle> columns = new ArrayList<>();
        for (JdbcColumnHandle column : super.getColumns(session, tableHandle)) {
//            IRIS Always returns REMARKS even if it was not set, let's empty it if it equals to columnName
            if (column.getComment().isPresent() && column.getComment().get().equals(column.getColumnName())) {
                columns.add(JdbcColumnHandle.builderFrom(column).setComment(Optional.empty()).build());
            } else {
                columns.add(column);
            }
        }
        return ImmutableList.copyOf(columns);
    }

    @Override
    protected ResultSet getColumns(JdbcTableHandle tableHandle, DatabaseMetaData metadata) throws SQLException {
        return super.getColumns(tableHandle, metadata);
    }

    @Override
    public boolean supportsTopN(ConnectorSession session, JdbcTableHandle handle, List<JdbcSortItem> sortOrder) {
        return true;
    }

    @Override
    public boolean isTopNGuaranteed(ConnectorSession session) {
        return true;
    }

    @Override
    public Optional<Type> getSupportedType(ConnectorSession session, Type type) {
        return super.getSupportedType(session, type);
    }

    @Override
    public boolean supportsAggregationPushdown(ConnectorSession session, JdbcTableHandle table, List<AggregateFunction> aggregates, Map<String, ColumnHandle> assignments, List<List<ColumnHandle>> groupingSets) {
        return super.supportsAggregationPushdown(session, table, aggregates, assignments, groupingSets);
    }

    @Override
    public Optional<JdbcExpression> implementAggregation(ConnectorSession session, AggregateFunction aggregate, Map<String, ColumnHandle> assignments) {
        return aggregateFunctionRewriter.rewrite(session, aggregate, assignments);
    }

    @Override
    public Optional<ParameterizedExpression> convertPredicate(ConnectorSession session, ConnectorExpression expression, Map<String, ColumnHandle> assignments) {
        return connectorExpressionRewriter.rewrite(session, expression, assignments);
    }

    @Override
    public PreparedQuery prepareQuery(ConnectorSession session, JdbcTableHandle table, Optional<List<List<JdbcColumnHandle>>> groupingSets, List<JdbcColumnHandle> columns, Map<String, ParameterizedExpression> columnExpressions) {
        List<JdbcColumnHandle> groupingSet = groupingSets.isEmpty() ? null : getOnlyElement(groupingSets.get(), null);
        if (groupingSet == null || groupingSet.isEmpty()) {
            return super.prepareQuery(session, table, groupingSets, columns, columnExpressions);
        }
        ImmutableMap.Builder<String, ParameterizedExpression> expressions = ImmutableMap.builder();
        expressions.putAll(columnExpressions);
//        Columns in GROUP BY with type VARCHAR, will be in UPPER case on the server by default, wrap them with %EXACT to override
        for (JdbcColumnHandle groupingField : groupingSet) {
            if (groupingField.getJdbcTypeHandle().getJdbcType() == Types.VARCHAR) {
                expressions.put(groupingField.getColumnName(),
                        new ParameterizedExpression(
                                format("%%EXACT(%s)", quoted(groupingField.getColumnName())),
                                List.of())
                );
            }
        }
        return super.prepareQuery(session, table, groupingSets, columns, expressions.build());
    }

    @Override
    public void abortReadConnection(Connection connection, ResultSet resultSet) throws SQLException {
        super.abortReadConnection(connection, resultSet);
    }

    @Override
    public Optional<String> getTableComment(ResultSet resultSet) throws SQLException {
        return super.getTableComment(resultSet);
    }

    @Override
    public void setColumnComment(ConnectorSession session, JdbcTableHandle handle, JdbcColumnHandle column, Optional<String> comment) {
        super.setColumnComment(session, handle, column, comment);
    }

    @Override
    public void setTableProperties(ConnectorSession session, JdbcTableHandle handle, Map<String, Optional<Object>> properties) {
        super.setTableProperties(session, handle, properties);
    }

    @Override
    public TableStatistics getTableStatistics(ConnectorSession session, JdbcTableHandle handle) {
        return super.getTableStatistics(session, handle);
    }

    @Override
    public Optional<SystemTable> getSystemTable(ConnectorSession session, SchemaTableName tableName) {
        return super.getSystemTable(session, tableName);
    }

    @Override
    public Optional<TableScanRedirectApplicationResult> getTableScanRedirection(ConnectorSession session, JdbcTableHandle tableHandle) {
        return super.getTableScanRedirection(session, tableHandle);
    }

    @Override
    protected void renameColumn(ConnectorSession session, Connection connection, RemoteTableName remoteTableName, String remoteColumnName, String newRemoteColumnName) throws SQLException {
        execute(session, connection, format(
                "ALTER TABLE %s ALTER COLUMN %s RENAME %s",
                quoted(remoteTableName),
                quoted(remoteColumnName),
                quoted(newRemoteColumnName)));
    }

    @Override
    public void setColumnType(ConnectorSession session, JdbcTableHandle handle, JdbcColumnHandle column, Type type) {
        try (Connection connection = connectionFactory.openConnection(session)) {
            verify(connection.getAutoCommit());
            String remoteColumnName = getIdentifierMapping().toRemoteColumnName(getRemoteIdentifiers(connection), column.getColumnName());
            String sql = format(
                    "ALTER TABLE %s ALTER COLUMN %s %s",
                    quoted(handle.asPlainTable().getRemoteTableName()),
                    quoted(remoteColumnName),
                    toWriteMapping(session, type).getDataType());
            execute(session, connection, sql);
        } catch (SQLException e) {
            throw new TrinoException(JDBC_ERROR, e);
        }
    }

}
