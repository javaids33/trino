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

import com.cronutils.model.Cron;
import com.cronutils.model.CronType;
import com.cronutils.model.definition.CronDefinitionBuilder;
import com.cronutils.model.time.ExecutionTime;
import com.cronutils.parser.CronParser;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.AbstractScheduledService;
import com.google.inject.Inject;
import io.airlift.log.Logger;

import java.time.ZonedDateTime;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import static java.util.Objects.requireNonNull;

public class MaterializedViewSchedulerService
        extends AbstractScheduledService
{
    private static final Logger log = Logger.get(MaterializedViewSchedulerService.class);

    private final MaterializedViewSchedulerConfig config;
    private final CronParser cronParser;

    // Track last execution time for each materialized view by qualified name
    private final Map<String, ZonedDateTime> lastExecutionTimes = new ConcurrentHashMap<>();

    @Inject
    public MaterializedViewSchedulerService(MaterializedViewSchedulerConfig config)
    {
        this.config = requireNonNull(config, "config is null");
        this.cronParser = new CronParser(CronDefinitionBuilder.instanceDefinitionFor(CronType.UNIX));
    }

    @Override
    protected void startUp()
    {
        if (config.isEnabled()) {
            log.info("Starting Materialized View Scheduler Service");
            log.info("Refresh check interval: %s", config.getRefreshCheckInterval());
            log.info("Max concurrent refreshes: %s", config.getMaxConcurrentRefreshes());
        }
        else {
            log.info("Materialized View Scheduler Service is disabled");
        }
    }

    @Override
    protected void shutDown()
    {
        log.info("Shutting down Materialized View Scheduler Service");
    }

    @Override
    protected void runOneIteration()
    {
        if (!config.isEnabled()) {
            return;
        }

        try {
            checkForScheduledRefreshes();
        }
        catch (Exception e) {
            log.error(e, "Error checking for scheduled materialized view refreshes");
        }
    }

    @Override
    protected Scheduler scheduler()
    {
        return Scheduler.newFixedDelaySchedule(
                config.getRefreshCheckInterval().toMillis(),
                config.getRefreshCheckInterval().toMillis(),
                TimeUnit.MILLISECONDS);
    }

    @VisibleForTesting
    void checkForScheduledRefreshes()
    {
        log.debug("Checking for materialized views that need refreshing");

        // This is a placeholder implementation that demonstrates the infrastructure.
        // In a future enhancement, this method would:
        // 1. Query the metadata to get all materialized views with refresh schedules
        // 2. Parse their cron expressions
        // 3. Determine which ones need to be refreshed now
        // 4. Execute refresh queries for those views

        // Example of checking a cron schedule:
        String exampleCronSchedule = "0 * * * *"; // Every hour at minute 0
        ZonedDateTime now = ZonedDateTime.now();

        try {
            Cron cron = cronParser.parse(exampleCronSchedule);
            ExecutionTime executionTime = ExecutionTime.forCron(cron);
            log.debug("Example: Next execution for '%s' would be at %s",
                    exampleCronSchedule,
                    executionTime.nextExecution(now).orElse(now));
        }
        catch (Exception e) {
            log.debug(e, "Example cron parsing");
        }
    }

    @VisibleForTesting
    boolean shouldRefreshView(String viewQualifiedName, String cronExpression)
    {
        ZonedDateTime now = ZonedDateTime.now();

        try {
            Cron cron = cronParser.parse(cronExpression);
            ExecutionTime executionTime = ExecutionTime.forCron(cron);

            // Get the last execution time for this view
            ZonedDateTime lastExecution = lastExecutionTimes.get(viewQualifiedName);

            // If never executed, check if we should execute now
            if (lastExecution == null) {
                return executionTime.nextExecution(now.minusYears(1))
                        .map(next -> !next.isAfter(now))
                        .orElse(false);
            }

            // Check if the next scheduled execution is now or in the past
            return executionTime.nextExecution(lastExecution)
                    .map(next -> !next.isAfter(now))
                    .orElse(false);
        }
        catch (Exception e) {
            log.error(e, "Error parsing cron expression '%s' for view %s", cronExpression, viewQualifiedName);
            return false;
        }
    }

    @VisibleForTesting
    void markViewRefreshed(String viewQualifiedName)
    {
        lastExecutionTimes.put(viewQualifiedName, ZonedDateTime.now());
    }
}
