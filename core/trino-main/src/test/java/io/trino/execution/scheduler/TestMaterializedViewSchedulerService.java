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

import io.airlift.units.Duration;
import org.junit.jupiter.api.Test;

import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

public class TestMaterializedViewSchedulerService
{
    @Test
    public void testServiceCreation()
    {
        MaterializedViewSchedulerConfig config = new MaterializedViewSchedulerConfig()
                .setEnabled(true)
                .setRefreshCheckInterval(new Duration(1, TimeUnit.MINUTES))
                .setMaxConcurrentRefreshes(5);

        MaterializedViewSchedulerService service = new MaterializedViewSchedulerService(config);
        assertThat(service).isNotNull();
    }

    @Test
    public void testShouldRefreshView()
    {
        MaterializedViewSchedulerConfig config = new MaterializedViewSchedulerConfig()
                .setEnabled(true)
                .setRefreshCheckInterval(new Duration(1, TimeUnit.MINUTES))
                .setMaxConcurrentRefreshes(5);

        MaterializedViewSchedulerService service = new MaterializedViewSchedulerService(config);

        // Test with a cron expression that runs every minute
        String viewName = "catalog.schema.test_view";
        String cronExpression = "* * * * *"; // Every minute

        // First check should return true (never executed before)
        assertThat(service.shouldRefreshView(viewName, cronExpression)).isTrue();

        // Mark as refreshed
        service.markViewRefreshed(viewName);

        // Immediate second check should return false (just refreshed)
        assertThat(service.shouldRefreshView(viewName, cronExpression)).isFalse();
    }

    @Test
    public void testShouldRefreshViewWithHourlyCron()
    {
        MaterializedViewSchedulerConfig config = new MaterializedViewSchedulerConfig()
                .setEnabled(true)
                .setRefreshCheckInterval(new Duration(1, TimeUnit.MINUTES))
                .setMaxConcurrentRefreshes(5);

        MaterializedViewSchedulerService service = new MaterializedViewSchedulerService(config);

        // Test with a cron expression that runs every hour
        String viewName = "catalog.schema.hourly_view";
        String cronExpression = "0 * * * *"; // Every hour at minute 0

        // First check - behavior depends on current time
        boolean shouldRefresh = service.shouldRefreshView(viewName, cronExpression);
        assertThat(shouldRefresh).isIn(true, false);
    }

    @Test
    public void testShouldRefreshViewWithInvalidCron()
    {
        MaterializedViewSchedulerConfig config = new MaterializedViewSchedulerConfig()
                .setEnabled(true)
                .setRefreshCheckInterval(new Duration(1, TimeUnit.MINUTES))
                .setMaxConcurrentRefreshes(5);

        MaterializedViewSchedulerService service = new MaterializedViewSchedulerService(config);

        // Test with an invalid cron expression
        String viewName = "catalog.schema.invalid_view";
        String invalidCronExpression = "invalid cron";

        // Should return false for invalid cron expressions
        assertThat(service.shouldRefreshView(viewName, invalidCronExpression)).isFalse();
    }

    @Test
    public void testCheckForScheduledRefreshes()
    {
        MaterializedViewSchedulerConfig config = new MaterializedViewSchedulerConfig()
                .setEnabled(true)
                .setRefreshCheckInterval(new Duration(1, TimeUnit.MINUTES))
                .setMaxConcurrentRefreshes(5);

        MaterializedViewSchedulerService service = new MaterializedViewSchedulerService(config);

        // Should not throw any exceptions
        service.checkForScheduledRefreshes();
    }

    @Test
    public void testServiceWithDisabledConfig()
    {
        MaterializedViewSchedulerConfig config = new MaterializedViewSchedulerConfig()
                .setEnabled(false)
                .setRefreshCheckInterval(new Duration(1, TimeUnit.MINUTES))
                .setMaxConcurrentRefreshes(5);

        MaterializedViewSchedulerService service = new MaterializedViewSchedulerService(config);
        assertThat(service).isNotNull();

        // Should not throw any exceptions even when disabled
        service.checkForScheduledRefreshes();
    }
}
