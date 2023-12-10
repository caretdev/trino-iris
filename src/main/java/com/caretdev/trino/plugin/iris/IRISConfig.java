package com.caretdev.trino.plugin.iris;

import io.airlift.configuration.Config;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotNull;

public class IRISConfig {

    private boolean connectionPoolEnabled = false;
    private int connectionPoolMinSize = 1;
    private int connectionPoolMaxSize = 5;

    @NotNull
    public boolean isConnectionPoolEnabled()
    {
        return connectionPoolEnabled;
    }

    @Config("iris.connection-pool.enabled")
    public IRISConfig setConnectionPoolEnabled(boolean connectionPoolEnabled)
    {
        this.connectionPoolEnabled = connectionPoolEnabled;
        return this;
    }

    @Min(0)
    public int getConnectionPoolMinSize()
    {
        return connectionPoolMinSize;
    }

    @Config("iris.connection-pool.min-size")
    public IRISConfig setConnectionPoolMinSize(int connectionPoolMinSize)
    {
        this.connectionPoolMinSize = connectionPoolMinSize;
        return this;
    }

    @Min(1)
    public int getConnectionPoolMaxSize()
    {
        return connectionPoolMaxSize;
    }

    @Config("iris.connection-pool.max-size")
    public IRISConfig setConnectionPoolMaxSize(int connectionPoolMaxSize)
    {
        this.connectionPoolMaxSize = connectionPoolMaxSize;
        return this;
    }

    private int featureOption;

    public int getFeatureOption() {
        return featureOption;
    }

    @Config("iris.jdbc.feature-option")
    public IRISConfig setFeatureOption(int featureOption) {
        this.featureOption = featureOption;
        return this;
    }

}
