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

import java.util.concurrent.TimeUnit;

public class MaterializedViewSchedulerConfig
{
    private boolean enabled = true;
    private Duration refreshCheckInterval = new Duration(1, TimeUnit.MINUTES);
    private int maxConcurrentRefreshes = 5;

    public boolean isEnabled()
    {
        return enabled;
    }

    @Config("materialized-view-scheduler.enabled")
    @ConfigDescription("Enable automatic scheduling of materialized view refreshes")
    public MaterializedViewSchedulerConfig setEnabled(boolean enabled)
    {
        this.enabled = enabled;
        return this;
    }

    public Duration getRefreshCheckInterval()
    {
        return refreshCheckInterval;
    }

    @Config("materialized-view-scheduler.refresh-check-interval")
    @ConfigDescription("How often to check for materialized views that need refreshing")
    public MaterializedViewSchedulerConfig setRefreshCheckInterval(Duration refreshCheckInterval)
    {
        this.refreshCheckInterval = refreshCheckInterval;
        return this;
    }

    public int getMaxConcurrentRefreshes()
    {
        return maxConcurrentRefreshes;
    }

    @Config("materialized-view-scheduler.max-concurrent-refreshes")
    @ConfigDescription("Maximum number of materialized views that can be refreshed concurrently")
    public MaterializedViewSchedulerConfig setMaxConcurrentRefreshes(int maxConcurrentRefreshes)
    {
        this.maxConcurrentRefreshes = maxConcurrentRefreshes;
        return this;
    }
}
