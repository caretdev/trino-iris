package com.caretdev.trino.plugin.iris;

import io.trino.operator.RetryPolicy;

public class TestIRISQueryFailureRecoveryTest extends BaseIRISFailureRecoveryTest {
    public TestIRISQueryFailureRecoveryTest(RetryPolicy retryPolicy) {
        super(RetryPolicy.QUERY);
    }
}
