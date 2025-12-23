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
import io.trino.Session;
import io.trino.dispatcher.DispatchManager;
import io.trino.metadata.MaterializedViewDefinition;
import io.trino.metadata.Metadata;
import io.trino.metadata.QualifiedObjectName;
import io.trino.metadata.QualifiedTablePrefix;
import io.trino.spi.security.Identity;
import io.trino.sql.tree.QualifiedName;
import io.trino.sql.tree.RefreshMaterializedView;
import io.trino.sql.tree.Table;

import java.time.ZonedDateTime;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.trino.SystemSessionProperties.QUERY_MAX_MEMORY;
import static io.trino.SystemSessionProperties.QUERY_MAX_TOTAL_MEMORY;
import static java.util.Objects.requireNonNull;

public class MaterializedViewSchedulerService
        extends AbstractScheduledService
{
    private static final Logger log = Logger.get(MaterializedViewSchedulerService.class);

    private final MaterializedViewSchedulerConfig config;
    private final Metadata metadata;
    private final DispatchManager dispatchManager;
    private final ExecutorService executor;
    private final Semaphore concurrentRefreshSemaphore;
    private final CronParser cronParser;

    // Track last execution time for each materialized view
    private final Map<QualifiedObjectName, ZonedDateTime> lastExecutionTimes = new ConcurrentHashMap<>();

    @Inject
    public MaterializedViewSchedulerService(
            MaterializedViewSchedulerConfig config,
            Metadata metadata,
            DispatchManager dispatchManager)
    {
        this.config = requireNonNull(config, "config is null");
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.dispatchManager = requireNonNull(dispatchManager, "dispatchManager is null");
        this.executor = Executors.newCachedThreadPool(daemonThreadsNamed("materialized-view-refresh-%s"));
        this.concurrentRefreshSemaphore = new Semaphore(config.getMaxConcurrentRefreshes());
        this.cronParser = new CronParser(CronDefinitionBuilder.instanceDefinitionFor(CronType.UNIX));
    }

    @Override
    protected void startUp()
    {
        if (config.isEnabled()) {
            log.info("Starting Materialized View Scheduler Service");
        }
        else {
            log.info("Materialized View Scheduler Service is disabled");
        }
    }

    @Override
    protected void shutDown()
    {
        log.info("Shutting down Materialized View Scheduler Service");
        executor.shutdown();
        try {
            if (!executor.awaitTermination(30, TimeUnit.SECONDS)) {
                executor.shutdownNow();
            }
        }
        catch (InterruptedException e) {
            executor.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }

    @Override
    protected void runOneIteration()
    {
        if (!config.isEnabled()) {
            return;
        }

        try {
            checkAndRefreshMaterializedViews();
        }
        catch (Exception e) {
            log.error(e, "Error checking materialized views for refresh");
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
    void checkAndRefreshMaterializedViews()
    {
        log.debug("Checking for materialized views that need refreshing");

        // Get all materialized views from all catalogs
        Set<String> catalogNames = metadata.getCatalogNames().keySet();

        for (String catalogName : catalogNames) {
            try {
                checkCatalogForRefreshableViews(catalogName);
            }
            catch (Exception e) {
                log.error(e, "Error checking catalog %s for refreshable views", catalogName);
            }
        }
    }

    private void checkCatalogForRefreshableViews(String catalogName)
    {
        // Create a session for querying metadata
        Session session = createMetadataSession(catalogName);

        // Get all materialized views in this catalog
        QualifiedTablePrefix prefix = new QualifiedTablePrefix(catalogName, Optional.empty(), Optional.empty());
        Map<QualifiedObjectName, io.trino.metadata.ViewInfo> materializedViews = metadata.getMaterializedViews(session, prefix);

        for (QualifiedObjectName viewName : materializedViews.keySet()) {
            try {
                Optional<MaterializedViewDefinition> viewDefinition = metadata.getMaterializedView(session, viewName);
                if (viewDefinition.isPresent() && viewDefinition.get().getRefreshSchedule().isPresent()) {
                    checkAndScheduleRefresh(viewName, viewDefinition.get());
                }
            }
            catch (Exception e) {
                log.error(e, "Error checking materialized view %s", viewName);
            }
        }
    }

    private void checkAndScheduleRefresh(QualifiedObjectName viewName, MaterializedViewDefinition definition)
    {
        String cronExpression = definition.getRefreshSchedule().get();
        ZonedDateTime now = ZonedDateTime.now();

        try {
            Cron cron = cronParser.parse(cronExpression);
            ExecutionTime executionTime = ExecutionTime.forCron(cron);

            // Get the last execution time for this view
            ZonedDateTime lastExecution = lastExecutionTimes.get(viewName);

            // Determine if refresh is needed
            Optional<ZonedDateTime> nextExecution = executionTime.nextExecution(lastExecution != null ? lastExecution : now.minusYears(1));

            if (nextExecution.isPresent() && (lastExecution == null || nextExecution.get().isBefore(now) || nextExecution.get().isEqual(now))) {
                log.info("Scheduling refresh for materialized view: %s (cron: %s)", viewName, cronExpression);
                scheduleRefresh(viewName, definition);
                lastExecutionTimes.put(viewName, now);
            }
        }
        catch (Exception e) {
            log.error(e, "Error parsing cron expression '%s' for materialized view %s", cronExpression, viewName);
        }
    }

    private void scheduleRefresh(QualifiedObjectName viewName, MaterializedViewDefinition definition)
    {
        // Try to acquire permit for concurrent refresh
        if (!concurrentRefreshSemaphore.tryAcquire()) {
            log.warn("Maximum concurrent refreshes reached, skipping refresh of %s", viewName);
            return;
        }

        executor.submit(() -> {
            try {
                refreshMaterializedView(viewName, definition);
            }
            catch (Exception e) {
                log.error(e, "Error refreshing materialized view %s", viewName);
            }
            finally {
                concurrentRefreshSemaphore.release();
            }
        });
    }

    private void refreshMaterializedView(QualifiedObjectName viewName, MaterializedViewDefinition definition)
    {
        log.info("Refreshing materialized view: %s", viewName);

        try {
            // Create a session for the refresh operation using the view owner's identity
            Session session = createRefreshSession(viewName, definition);

            // Create the REFRESH MATERIALIZED VIEW statement
            RefreshMaterializedView refreshStatement = new RefreshMaterializedView(
                    Optional.empty(),
                    new Table(QualifiedName.of(viewName.catalogName(), viewName.schemaName(), viewName.objectName())));

            // Execute the refresh through the dispatcher
            dispatchManager.createQuery(
                    session.getQueryId(),
                    session.toSessionRepresentation(),
                    refreshStatement.toString());

            log.info("Successfully initiated refresh for materialized view: %s", viewName);
        }
        catch (Exception e) {
            log.error(e, "Failed to refresh materialized view %s", viewName);
        }
    }

    private Session createMetadataSession(String catalogName)
    {
        return Session.builder(metadata.getSessionPropertyManager())
                .setQueryId(dispatchManager.createQueryId())
                .setIdentity(Identity.ofUser("trino-system"))
                .setCatalog(catalogName)
                .setSchema("information_schema")
                .setSystemProperty(QUERY_MAX_MEMORY, "1GB")
                .setSystemProperty(QUERY_MAX_TOTAL_MEMORY, "1GB")
                .build();
    }

    private Session createRefreshSession(QualifiedObjectName viewName, MaterializedViewDefinition definition)
    {
        // Use the view owner's identity if available, otherwise use system identity
        Identity identity = definition.getRunAsIdentity()
                .map(Identity::from)
                .orElse(Identity.ofUser("trino-system"));

        return Session.builder(metadata.getSessionPropertyManager())
                .setQueryId(dispatchManager.createQueryId())
                .setIdentity(identity)
                .setCatalog(viewName.catalogName())
                .setSchema(viewName.schemaName())
                .setSystemProperty(QUERY_MAX_MEMORY, "10GB")
                .setSystemProperty(QUERY_MAX_TOTAL_MEMORY, "10GB")
                .build();
    }
}
