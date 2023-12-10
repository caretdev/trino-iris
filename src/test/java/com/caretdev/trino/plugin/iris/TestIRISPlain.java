package com.caretdev.trino.plugin.iris;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.Session;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import io.trino.testing.datatype.*;
import io.trino.testing.sql.SqlExecutor;
import io.trino.testing.sql.TrinoSqlExecutor;
import org.testng.annotations.Test;

import static com.caretdev.trino.plugin.iris.IRISQueryRunner.createIRISQueryRunner;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.VarcharType.createVarcharType;

public class TestIRISPlain extends AbstractTestQueryFramework {
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
    public void testCreateTable()
    {
        assertQueryReturnsEmptyResult("create table demo.test (id bigint)");
    }

    protected SqlExecutor onRemoteDatabase()
    {
        return irisServer::execute;
    }
}
