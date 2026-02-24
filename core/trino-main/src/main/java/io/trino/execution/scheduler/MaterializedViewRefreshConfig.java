/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.execution.scheduler;

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.units.Duration;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotNull;

import java.util.concurrent.TimeUnit;

public class MaterializedViewRefreshConfig
{
    private boolean enabled;
    private Duration scanInterval = new Duration(60, TimeUnit.SECONDS);
    private String coordinatorId = "default";
    private int maxConcurrentRefreshes = 4;
    private Duration lockTimeout = new Duration(30, TimeUnit.MINUTES);

    public boolean isEnabled()
    {
        return enabled;
    }

    @Config("materialized-view-refresh.enabled")
    @ConfigDescription("Enable the internal materialized view refresh scheduler")
    public MaterializedViewRefreshConfig setEnabled(boolean enabled)
    {
        this.enabled = enabled;
        return this;
    }

    @NotNull
    public Duration getScanInterval()
    {
        return scanInterval;
    }

    @Config("materialized-view-refresh.scan-interval")
    @ConfigDescription("How often to scan for materialized views that need refreshing")
    public MaterializedViewRefreshConfig setScanInterval(Duration scanInterval)
    {
        this.scanInterval = scanInterval;
        return this;
    }

    @NotNull
    public String getCoordinatorId()
    {
        return coordinatorId;
    }

    @Config("materialized-view-refresh.coordinator-id")
    @ConfigDescription("Unique identifier for this coordinator instance (for distributed locking)")
    public MaterializedViewRefreshConfig setCoordinatorId(String coordinatorId)
    {
        this.coordinatorId = coordinatorId;
        return this;
    }

    @Min(1)
    public int getMaxConcurrentRefreshes()
    {
        return maxConcurrentRefreshes;
    }

    @Config("materialized-view-refresh.max-concurrent-refreshes")
    @ConfigDescription("Maximum number of concurrent materialized view refreshes")
    public MaterializedViewRefreshConfig setMaxConcurrentRefreshes(int maxConcurrentRefreshes)
    {
        this.maxConcurrentRefreshes = maxConcurrentRefreshes;
        return this;
    }

    @NotNull
    public Duration getLockTimeout()
    {
        return lockTimeout;
    }

    @Config("materialized-view-refresh.lock-timeout")
    @ConfigDescription("Duration after which a refresh lock is considered expired")
    public MaterializedViewRefreshConfig setLockTimeout(Duration lockTimeout)
    {
        this.lockTimeout = lockTimeout;
        return this;
    }
}
