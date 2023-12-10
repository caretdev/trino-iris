package com.caretdev.trino.plugin.iris;

import io.trino.operator.RetryPolicy;

public class TestIRISTaskFailureRecoveryTest extends BaseIRISFailureRecoveryTest {
    public TestIRISTaskFailureRecoveryTest(RetryPolicy retryPolicy) {
        super(RetryPolicy.TASK);
    }
}
