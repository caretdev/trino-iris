package com.caretdev.trino.plugin.iris;

import io.trino.plugin.jdbc.RemoteDatabaseEvent;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.airlift.testing.Closeables.closeAll;
import static io.trino.plugin.jdbc.RemoteDatabaseEvent.Status.CANCELLED;
import static io.trino.plugin.jdbc.RemoteDatabaseEvent.Status.RUNNING;
import static io.trino.testing.assertions.Assert.assertEventually;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestTestingIRISServer {

    private final ExecutorService threadPool = newCachedThreadPool(daemonThreadsNamed("TestTestingIRISServer-%d"));

    private TestingIRISServer irisServer;

    @BeforeClass
    public void setUp() {
        irisServer = new TestingIRISServer();
    }

    @AfterClass(alwaysRun = true)
    public void tearDown()
            throws Exception {
        closeAll(
                irisServer,
                threadPool::shutdownNow);
    }

    @Test
    public void testStatement()
    {
        String sql = "SELECT 1";
        irisServer.execute("SELECT $ZVERSION");
    }
}

