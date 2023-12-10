package com.caretdev.trino.plugin.iris;

import io.trino.plugin.jdbc.JdbcPlugin;

public class IRISPlugin extends JdbcPlugin {
    public IRISPlugin() {
        super("iris", new IRISClientModule());
    }
}
