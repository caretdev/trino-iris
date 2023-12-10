package com.caretdev.trino.plugin.iris;

import io.trino.spi.Plugin;
import io.trino.spi.connector.ConnectorFactory;
import io.trino.testing.TestingConnectorContext;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import static com.google.common.collect.Iterables.getOnlyElement;

public class TestIRISPlugin {

    @Test
    public void testCreateConnector()
    {
        Plugin plugin = new IRISPlugin();
        ConnectorFactory factory = getOnlyElement(plugin.getConnectorFactories());
        factory.create("test", ImmutableMap.of("connection-url", "jdbc:IRIS://test"), new TestingConnectorContext());
    }
}
