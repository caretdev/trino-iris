package com.caretdev.trino.plugin.iris;

import io.trino.testing.datatype.CreateAndInsertDataSetup;
import io.trino.testing.sql.SqlExecutor;

public class IRISCreateAndInsertDataSetup extends CreateAndInsertDataSetup {
    public IRISCreateAndInsertDataSetup(SqlExecutor sqlExecutor, String tableNamePrefix) {
        super(sqlExecutor, tableNamePrefix);
    }


}
