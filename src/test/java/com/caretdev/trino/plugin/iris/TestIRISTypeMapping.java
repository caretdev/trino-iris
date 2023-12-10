package com.caretdev.trino.plugin.iris;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.Session;
import io.trino.spi.type.TimeZoneKey;
import io.trino.spi.type.VarcharType;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import io.trino.testing.TestingSession;
import io.trino.testing.datatype.*;
import io.trino.testing.sql.JdbcSqlExecutor;
import io.trino.testing.sql.SqlExecutor;
import io.trino.testing.sql.TrinoSqlExecutor;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.time.ZoneId;
import java.util.function.Consumer;
import java.util.function.Function;

import static com.caretdev.trino.plugin.iris.IRISClient.VARCHAR_UNBOUNDED_LENGTH;
import static com.caretdev.trino.plugin.iris.IRISQueryRunner.createIRISQueryRunner;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static io.trino.spi.type.DecimalType.createDecimalType;
import static io.trino.spi.type.VarcharType.createUnboundedVarcharType;
import static io.trino.spi.type.VarcharType.createVarcharType;
import static java.lang.String.format;
import static java.time.ZoneOffset.UTC;

public class TestIRISTypeMapping extends AbstractTestQueryFramework {
    protected TestingIRISServer irisServer;

    @Override
    protected QueryRunner createQueryRunner() throws Exception {
        irisServer = closeAfterClass(new TestingIRISServer());
        return createIRISQueryRunner(
                irisServer,
                ImmutableMap.of(),
                ImmutableMap.of(),
                ImmutableList.of()
        );
    }

    @Test
    public void testTrinoBoolean()
    {
        SqlDataTypeTest.create()
                .addRoundTrip("boolean", "null", BOOLEAN, "CAST(NULL AS BOOLEAN)")
                .addRoundTrip("boolean", "true", BOOLEAN)
                .addRoundTrip("boolean", "false", BOOLEAN)
                .execute(getQueryRunner(), trinoCreateAsSelect("test_boolean"))
                .execute(getQueryRunner(), trinoCreateAndInsert("test_boolean"));
    }

    @Test
    public void testIRISBit()
    {
        SqlDataTypeTest.create()
                .addRoundTrip("bit", "null", BOOLEAN, "CAST(NULL AS BOOLEAN)")
                .addRoundTrip("bit", "1", BOOLEAN, "true")
                .addRoundTrip("bit", "0", BOOLEAN, "false")
                .execute(getQueryRunner(), irisCreateAndInsert( IRISQueryRunner.TPCH_SCHEMA + ".test_bit"));
    }
    @Test
    public void testVarchar()
    {
        SqlDataTypeTest.create()
                .addRoundTrip("varchar(10)", "NULL", createVarcharType(10), "CAST(NULL AS varchar(10))")
                .addRoundTrip("varchar(10)", "'text_a'", createVarcharType(10), "CAST('text_a' AS varchar(10))")
                .addRoundTrip("varchar(255)", "'text_b'", createVarcharType(255), "CAST('text_b' AS varchar(255))")
                .addRoundTrip("varchar(65535)", "'text_d'", createVarcharType(65535), "CAST('text_d' AS varchar(65535))")
                .addRoundTrip("varchar(5)", "'攻殻機動隊'", createVarcharType(5), "CAST('攻殻機動隊' AS varchar(5))")
                .addRoundTrip("varchar(32)", "'攻殻機動隊'", createVarcharType(32), "CAST('攻殻機動隊' AS varchar(32))")
                .addRoundTrip("varchar(20000)", "'攻殻機動隊'", createVarcharType(20000), "CAST('攻殻機動隊' AS varchar(20000))")
                .addRoundTrip("varchar(77)", "'Ну, погоди!'", createVarcharType(77), "CAST('Ну, погоди!' AS varchar(77))")
                .addRoundTrip("varchar(10485760)", "'text_f'", createVarcharType(10485760), "CAST('text_f' AS varchar(10485760))") // too long for a char in Trino

//java.lang.RuntimeException: java.sql.SQLException: [SQLCODE: <-104>:<Field validation failed in INSERT, or value failed to convert in DisplayToLogical or OdbcToLogical>]
//[Location: <ServerLoop>]
//[%msg: <Field 'SQLUser.test_varchar7prv7y8gkf.col_7' (value '😂') failed validation>]
//                .addRoundTrip("varchar(1)", "'😂'", createVarcharType(1), "CAST('😂' AS varchar(1))")

                .execute(getQueryRunner(), irisCreateAndInsert(IRISQueryRunner.TPCH_SCHEMA + ".test_varchar"))
                .execute(getQueryRunner(), trinoCreateAsSelect("test_varchar"))
                .execute(getQueryRunner(), trinoCreateAndInsert("test_varchar"));
    }

    @Test
    public void testTrinoUnboundedVarchar()
    {
        /*
        * In IRIS unbounded VARCHAR is equals to VARCHAR(1)
        * So, for trino, forcefully change it to VARCHAR_UNBOUNDED_LENGTH = 65535
        * */
        Function<String, String> cast = (String v) -> format("CAST(%s AS VARCHAR(%d))", v, VARCHAR_UNBOUNDED_LENGTH);
        VarcharType varcharType = createVarcharType(VARCHAR_UNBOUNDED_LENGTH);
        SqlDataTypeTest.create()
                .addRoundTrip("varchar", "NULL", varcharType, cast.apply("NULL"))
                .addRoundTrip("varchar", "'text_a'", varcharType, cast.apply("'text_a'"))
                .addRoundTrip("varchar", "'text_b'", varcharType, cast.apply("'text_b'"))
                .addRoundTrip("varchar", "'text_d'", varcharType, cast.apply("'text_d'"))
                .addRoundTrip("varchar", "'攻殻機動隊'", varcharType, cast.apply("'攻殻機動隊'"))
                .addRoundTrip("varchar", "'攻殻機動隊'", varcharType, cast.apply("'攻殻機動隊'"))
                .addRoundTrip("varchar", "'攻殻機動隊'", varcharType, cast.apply("'攻殻機動隊'"))
                .addRoundTrip("varchar", "'😂'", varcharType, cast.apply("'😂'"))
                .addRoundTrip("varchar", "'Ну, погоди!'", varcharType, cast.apply("'Ну, погоди!'"))
                .addRoundTrip("varchar", "'text_f'", varcharType, cast.apply("'text_f'"))
                .execute(getQueryRunner(), trinoCreateAndInsert("test_unbounded_varchar"))
                .execute(getQueryRunner(), trinoCreateAsSelect("test_unbounded_varchar"));
    }

    @DataProvider
    public Object[][] sessionZonesDataProvider()
    {
        return new Object[][] {
                {UTC},
                {ZoneId.systemDefault()},
                // using two non-JVM zones so that we don't need to worry what SQL Server system zone is
                // no DST in 1970, but has DST in later years (e.g. 2018)
                {ZoneId.of("Europe/Vilnius")},
                // minutes offset change since 1970-01-01, no DST
                {ZoneId.of("Asia/Kathmandu")},
                {TestingSession.DEFAULT_TIME_ZONE_KEY.getZoneId()},
        };
    }

    @Test
    public void testDecimal()
    {
        SqlDataTypeTest.create()
                .addRoundTrip("decimal(3, 0)", "193", createDecimalType(3, 0), "CAST('193' AS decimal(3, 0))")
                .addRoundTrip("decimal(3, 0)", "19", createDecimalType(3, 0), "CAST('19' AS decimal(3, 0))")
                .addRoundTrip("decimal(3, 0)", "-193", createDecimalType(3, 0), "CAST('-193' AS decimal(3, 0))")
                .addRoundTrip("decimal(3, 1)", "10.0", createDecimalType(3, 1), "CAST('10.0' AS decimal(3, 1))")
                .addRoundTrip("decimal(3, 1)", "10.1", createDecimalType(3, 1), "CAST('10.1' AS decimal(3, 1))")
                .addRoundTrip("decimal(3, 1)", "-10.1", createDecimalType(3, 1), "CAST('-10.1' AS decimal(3, 1))")
                .addRoundTrip("decimal(4, 2)", "2", createDecimalType(4, 2), "CAST('2' AS decimal(4, 2))")
                .addRoundTrip("decimal(4, 2)", "2.3", createDecimalType(4, 2), "CAST('2.3' AS decimal(4, 2))")
                .addRoundTrip("decimal(21, 2)", "2", createDecimalType(21, 2), "CAST('2' AS decimal(21, 2))")
                .addRoundTrip("decimal(21, 2)", "2.3", createDecimalType(21, 2), "CAST('2.3' AS decimal(21, 2))")
                .addRoundTrip("decimal(21, 2)", "123456789.3", createDecimalType(21, 2), "CAST('123456789.3' AS decimal(21, 2))")
                .execute(getQueryRunner(), irisCreateAndInsert(IRISQueryRunner.TPCH_SCHEMA + ".test_decimal"))
                .execute(getQueryRunner(), trinoCreateAsSelect("test_decimal"))
                .execute(getQueryRunner(), trinoCreateAndInsert("test_decimal"));
    }

    @Test(dataProvider = "sessionZonesDataProvider")
    public void testTimestamp(ZoneId sessionZone)
    {
        SqlDataTypeTest tests = SqlDataTypeTest.create()

                // before epoch
                .addRoundTrip("TIMESTAMP '1958-01-01 13:18:03.123'", "TIMESTAMP '1958-01-01 13:18:03.123'")
                // after epoch
                .addRoundTrip("TIMESTAMP '2019-03-18 10:01:17.987'", "TIMESTAMP '2019-03-18 10:01:17.987'")
                // time doubled in JVM zone
                .addRoundTrip("TIMESTAMP '2018-10-28 01:33:17.456'", "TIMESTAMP '2018-10-28 01:33:17.456'")
                // time double in Vilnius
                .addRoundTrip("TIMESTAMP '2018-10-28 03:33:33.333'", "TIMESTAMP '2018-10-28 03:33:33.333'")
                // epoch
                .addRoundTrip("TIMESTAMP '1970-01-01 00:00:00.000'", "TIMESTAMP '1970-01-01 00:00:00.000'")
                // time gap in JVM zone
                .addRoundTrip("TIMESTAMP '1970-01-01 00:13:42.000'", "TIMESTAMP '1970-01-01 00:13:42.000'")
                .addRoundTrip("TIMESTAMP '2018-04-01 02:13:55.123'", "TIMESTAMP '2018-04-01 02:13:55.123'")
                // time gap in Vilnius
                .addRoundTrip("TIMESTAMP '2018-03-25 03:17:17.000'", "TIMESTAMP '2018-03-25 03:17:17.000'")
                // time gap in Kathmandu
                .addRoundTrip("TIMESTAMP '1986-01-01 00:13:07.000'", "TIMESTAMP '1986-01-01 00:13:07.000'")

                .addRoundTrip("TIMESTAMP '1958-01-01 13:18:03.123000'", "TIMESTAMP '1958-01-01 13:18:03.123'")
                .addRoundTrip("TIMESTAMP '2019-03-18 10:01:17.987000'", "TIMESTAMP '2019-03-18 10:01:17.987'")
                .addRoundTrip("TIMESTAMP '2018-10-28 01:33:17.456000'", "TIMESTAMP '2018-10-28 01:33:17.456'")
                .addRoundTrip("TIMESTAMP '2018-10-28 03:33:33.333000'", "TIMESTAMP '2018-10-28 03:33:33.333'")
                .addRoundTrip("TIMESTAMP '1970-01-01 00:00:00.000000'", "TIMESTAMP '1970-01-01 00:00:00.000'")
                .addRoundTrip("TIMESTAMP '1970-01-01 00:13:42.000000'", "TIMESTAMP '1970-01-01 00:13:42.000'")
                .addRoundTrip("TIMESTAMP '2018-04-01 02:13:55.123000'", "TIMESTAMP '2018-04-01 02:13:55.123'")
                .addRoundTrip("TIMESTAMP '2018-03-25 03:17:17.000000'", "TIMESTAMP '2018-03-25 03:17:17.000'")
                .addRoundTrip("TIMESTAMP '1986-01-01 00:13:07.000000'", "TIMESTAMP '1986-01-01 00:13:07.000'")

                .addRoundTrip("TIMESTAMP '1970-01-01 00:00:00'", "TIMESTAMP '1970-01-01 00:00:00.000'")
                .addRoundTrip("TIMESTAMP '1970-01-01 00:00:00.1'", "TIMESTAMP '1970-01-01 00:00:00.100'")
                .addRoundTrip("TIMESTAMP '1970-01-01 00:00:00.12'", "TIMESTAMP '1970-01-01 00:00:00.120'")
                .addRoundTrip("TIMESTAMP '1970-01-01 00:00:00.123'", "TIMESTAMP '1970-01-01 00:00:00.123'")
                .addRoundTrip("TIMESTAMP '1970-01-01 00:00:00.1234'", "TIMESTAMP '1970-01-01 00:00:00.123'")
                .addRoundTrip("TIMESTAMP '1970-01-01 00:00:00.12345'", "TIMESTAMP '1970-01-01 00:00:00.123'")
                .addRoundTrip("TIMESTAMP '1970-01-01 00:00:00.123456'", "TIMESTAMP '1970-01-01 00:00:00.123'");

        Session session = Session.builder(getSession())
                .setTimeZoneKey(TimeZoneKey.getTimeZoneKey(sessionZone.getId()))
                .build();

        tests.execute(getQueryRunner(), session, trinoCreateAsSelect(session, "test_timestamp"));
        tests.execute(getQueryRunner(), session, trinoCreateAsSelect("test_timestamp"));
        tests.execute(getQueryRunner(), session, trinoCreateAndInsert(session, "test_timestamp"));
        tests.execute(getQueryRunner(), session, trinoCreateAndInsert("test_timestamp"));
    }

    @Test
    public void testTime()
    {
        SqlDataTypeTest.create()
                .addRoundTrip("TIME '00:00:00'", "TIME '00:00:00'")
                .addRoundTrip("TIME '00:00:00.000000'", "TIME '00:00:00.000000'")
                .addRoundTrip("TIME '00:00:00.123456'", "TIME '00:00:00.123456'")
                .addRoundTrip("TIME '12:34:56'", "TIME '12:34:56'")
                .addRoundTrip("TIME '12:34:56.123456'", "TIME '12:34:56.123456'")

                // maximal value for a precision
                .addRoundTrip("TIME '23:59:59'", "TIME '23:59:59'")
                .addRoundTrip("TIME '23:59:59.9'", "TIME '23:59:59.9'")
                .addRoundTrip("TIME '23:59:59.99'", "TIME '23:59:59.99'")
                .addRoundTrip("TIME '23:59:59.999'", "TIME '23:59:59.999'")
                .addRoundTrip("TIME '23:59:59.9999'", "TIME '23:59:59.9999'")
                .addRoundTrip("TIME '23:59:59.99999'", "TIME '23:59:59.99999'")
                .addRoundTrip("TIME '23:59:59.999999'", "TIME '23:59:59.999999'")
                .addRoundTrip("TIME '23:59:59.9999999'", "TIME '23:59:59.9999999'")

                .execute(getQueryRunner(), trinoCreateAsSelect("test_time"))
                .execute(getQueryRunner(), trinoCreateAndInsert("test_time"));
    }

    private DataSetup trinoCreateAndInsert(String tableNamePrefix)
    {
        return trinoCreateAndInsert(getSession(), tableNamePrefix);
    }

    private DataSetup trinoCreateAndInsert(Session session, String tableNamePrefix)
    {
        return new CreateAndInsertDataSetup(new TrinoSqlExecutor(getQueryRunner(), session), tableNamePrefix);
    }


    private DataSetup trinoCreateAsSelect(String tableNamePrefix)
    {
        return trinoCreateAsSelect(getSession(), tableNamePrefix);
    }

    private DataSetup trinoCreateAsSelect(Session session, String tableNamePrefix)
    {
        return new CreateAsSelectDataSetup(new TrinoSqlExecutor(getQueryRunner(), session), tableNamePrefix);
    }

    private DataSetup irisCreateAndInsert(String tableNamePrefix) {
        return new CreateAndInsertDataSetup(onRemoteDatabase(), tableNamePrefix);
    }

    protected DataSetup sqlServerCreateAndTrinoInsert(Session session, String tableNamePrefix)
    {
        return new CreateAndTrinoInsertDataSetup(onRemoteDatabase(), new TrinoSqlExecutor(getQueryRunner(), session), tableNamePrefix);
    }

    protected SqlExecutor onRemoteDatabase()
    {
        return irisServer::execute;
    }
}
