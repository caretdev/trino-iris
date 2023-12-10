package com.caretdev.trino.plugin.iris;

import io.trino.testing.sql.SqlExecutor;
import io.trino.testing.sql.TestTable;

import java.util.List;

import static java.lang.String.format;
import static java.util.stream.Collectors.joining;

public class IRISTestTable extends TestTable {

    public IRISTestTable(SqlExecutor sqlExecutor, String namePrefix, String tableDefinition, List<String> rowsToInsert) {
        super(sqlExecutor, namePrefix, tableDefinition, rowsToInsert);
    }

    @Override
    protected void createAndInsert(List<String> rowsToInsert) {
        sqlExecutor.execute(format("CREATE TABLE %s %s", name, tableDefinition));
        try {
            if (!rowsToInsert.isEmpty()) {
                if (sqlExecutor.supportsMultiRowInsert()) {
                    sqlExecutor.execute(format("INSERT INTO %s VALUES %s", name, rowsToInsert.stream()
                            .map("(%s)"::formatted)
                            .collect(joining(", "))));
                }
                else {
                    for (String row : rowsToInsert) {
                        sqlExecutor.execute(format("INSERT INTO %s VALUES (%s)", name, row));
                    }
                }
            }
        }
        catch (Exception e) {
            try (TestTable ignored = this) {
                throw e;
            }
        }
    }
}
