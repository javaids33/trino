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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.inject.Inject;
import io.airlift.log.Logger;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.Tracer;
import io.trino.Session;
import io.trino.dispatcher.DispatchManager;
import io.trino.execution.QueryIdGenerator;
import io.trino.metadata.MaterializedViewDefinition;
import io.trino.metadata.Metadata;
import io.trino.metadata.QualifiedObjectName;
import io.trino.metadata.QualifiedTablePrefix;
import io.trino.metadata.SessionPropertyManager;
import io.trino.security.AccessControl;
import io.trino.server.SessionContext;
import io.trino.server.protocol.Slug;
import io.trino.spi.QueryId;
import io.trino.spi.security.Identity;
import io.trino.spi.security.SelectedRole;
import io.trino.spi.session.ResourceEstimates;
import io.trino.transaction.TransactionId;
import io.trino.transaction.TransactionManager;
import io.trino.util.CronExpressionParser;
import io.trino.util.CronExpressionParser.CronExpression;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;

import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import static com.google.common.util.concurrent.Futures.addCallback;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static io.trino.client.ProtocolHeaders.TRINO_HEADERS;
import static io.trino.spi.transaction.IsolationLevel.READ_COMMITTED;
import static java.util.Objects.requireNonNull;

public class MaterializedViewRefreshScheduler
{
    private static final Logger log = Logger.get(MaterializedViewRefreshScheduler.class);
    private static final Set<String> SKIP_SCHEMAS = Set.of("information_schema", "system");
    private static final Identity SYSTEM_IDENTITY = Identity.ofUser("trino-mv-refresh");

    private final Metadata metadata;
    private final MaterializedViewRefreshConfig config;
    private final ScheduledExecutorService executor;
    private final Semaphore refreshSemaphore;
    private final DispatchManager dispatchManager;
    private final SessionPropertyManager sessionPropertyManager;
    private final QueryIdGenerator queryIdGenerator;
    private final TransactionManager transactionManager;
    private final AccessControl accessControl;
    private final Tracer tracer;
    private final ConcurrentHashMap<QualifiedObjectName, ZonedDateTime> lastRefreshAttempt = new ConcurrentHashMap<>();

    @Inject
    public MaterializedViewRefreshScheduler(
            Metadata metadata,
            MaterializedViewRefreshConfig config,
            @ForScheduledRefresh ScheduledExecutorService executor,
            DispatchManager dispatchManager,
            SessionPropertyManager sessionPropertyManager,
            QueryIdGenerator queryIdGenerator,
            TransactionManager transactionManager,
            AccessControl accessControl,
            Tracer tracer)
    {
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.config = requireNonNull(config, "config is null");
        this.executor = requireNonNull(executor, "executor is null");
        this.dispatchManager = requireNonNull(dispatchManager, "dispatchManager is null");
        this.sessionPropertyManager = requireNonNull(sessionPropertyManager, "sessionPropertyManager is null");
        this.queryIdGenerator = requireNonNull(queryIdGenerator, "queryIdGenerator is null");
        this.transactionManager = requireNonNull(transactionManager, "transactionManager is null");
        this.accessControl = requireNonNull(accessControl, "accessControl is null");
        this.tracer = requireNonNull(tracer, "tracer is null");
        this.refreshSemaphore = new Semaphore(config.getMaxConcurrentRefreshes());
    }

    @PostConstruct
    public void start()
    {
        if (!config.isEnabled()) {
            log.info("Materialized view refresh scheduler is disabled");
            return;
        }
        long intervalSeconds = (long) config.getScanInterval().getValue(TimeUnit.SECONDS);
        executor.scheduleAtFixedRate(
                this::scanAndRefresh,
                intervalSeconds,
                intervalSeconds,
                TimeUnit.SECONDS);
        log.info("Materialized view refresh scheduler started (coordinator=%s)", config.getCoordinatorId());
    }

    private void scanAndRefresh()
    {
        try {
            ZonedDateTime now = ZonedDateTime.now(ZoneOffset.UTC);
            log.info("Scanning for materialized views that need refreshing at %s", now);

            // Create a read-only transaction for the metadata scan
            TransactionId transactionId = transactionManager.beginTransaction(READ_COMMITTED, true, true);
            try {
                Session baseSession = Session.builder(sessionPropertyManager)
                        .setQueryId(queryIdGenerator.createNextQueryId())
                        .setIdentity(SYSTEM_IDENTITY)
                        .setOriginalIdentity(SYSTEM_IDENTITY)
                        .setSource("materialized-view-refresh")
                        .build();

                Session systemSession = baseSession.beginTransactionId(transactionId, transactionManager, accessControl);

                int scanned = 0;
                int dispatched = 0;

                List<String> catalogNames = transactionManager.getCatalogs(transactionId).stream()
                        .map(info -> info.catalogName())
                        .toList();

                for (String catalogName : catalogNames) {
                    List<String> schemas;
                    try {
                        schemas = metadata.listSchemaNames(systemSession, catalogName);
                    }
                    catch (Exception e) {
                        log.warn(e, "Failed to list schemas for catalog %s, skipping", catalogName);
                        continue;
                    }

                    for (String schemaName : schemas) {
                        if (SKIP_SCHEMAS.contains(schemaName)) {
                            continue;
                        }

                        List<QualifiedObjectName> mvNames;
                        try {
                            QualifiedTablePrefix prefix = new QualifiedTablePrefix(catalogName, schemaName);
                            mvNames = metadata.listMaterializedViews(systemSession, prefix);
                        }
                        catch (Exception e) {
                            log.warn(e, "Failed to list materialized views in %s.%s, skipping", catalogName, schemaName);
                            continue;
                        }

                        for (QualifiedObjectName mvName : mvNames) {
                            scanned++;
                            try {
                                if (shouldRefresh(systemSession, mvName, now)) {
                                    if (refreshSemaphore.tryAcquire()) {
                                        dispatched++;
                                        dispatchRefresh(mvName);
                                    }
                                    else {
                                        log.info("Skipping refresh for %s — max concurrent refreshes reached", mvName);
                                    }
                                }
                            }
                            catch (Exception e) {
                                log.warn(e, "Error checking refresh for %s", mvName);
                            }
                        }
                    }
                }

                log.info("Refresh scan complete: scanned %d materialized views, dispatched %d refreshes", scanned, dispatched);
            }
            finally {
                transactionManager.asyncAbort(transactionId);
            }
        }
        catch (Exception e) {
            log.error(e, "Error in materialized view refresh scan");
        }
    }

    private boolean shouldRefresh(Session session, QualifiedObjectName mvName, ZonedDateTime now)
    {
        Optional<MaterializedViewDefinition> mvDef = metadata.getMaterializedView(session, mvName);
        if (mvDef.isEmpty()) {
            return false;
        }

        Optional<String> refreshSchedule = mvDef.get().getRefreshSchedule();
        if (refreshSchedule.isEmpty()) {
            return false;
        }

        CronExpression cron = CronExpressionParser.parse(refreshSchedule.get());

        // Determine reference time: last attempt or epoch if never attempted
        ZonedDateTime lastAttempt = lastRefreshAttempt.getOrDefault(
                mvName,
                ZonedDateTime.of(1970, 1, 1, 0, 0, 0, 0, ZoneOffset.UTC));

        ZonedDateTime nextFireTime = cron.nextFireTime(lastAttempt);

        if (!nextFireTime.isAfter(now)) {
            log.info("Materialized view %s is due for refresh (schedule: '%s', next fire: %s)",
                    mvName, refreshSchedule.get(), nextFireTime);
            return true;
        }

        log.debug("Materialized view %s next refresh at %s (schedule: '%s')",
                mvName, nextFireTime, refreshSchedule.get());
        return false;
    }

    private void dispatchRefresh(QualifiedObjectName mvName)
    {
        try {
            QueryId queryId = dispatchManager.createQueryId();

            Span querySpan = tracer.spanBuilder("mv-refresh")
                    .setAttribute("trino.mv.name", mvName.toString())
                    .startSpan();

            SessionContext sessionContext = new SessionContext(
                    TRINO_HEADERS,
                    Optional.of(mvName.catalogName()),
                    Optional.of(mvName.schemaName()),
                    Optional.empty(),
                    Optional.empty(),
                    SYSTEM_IDENTITY,
                    SYSTEM_IDENTITY,
                    new SelectedRole(SelectedRole.Type.NONE, Optional.empty()),
                    Optional.of("materialized-view-refresh"),
                    Optional.empty(),
                    Optional.empty(),
                    Optional.empty(),
                    Optional.of("UTC"),
                    Optional.empty(),
                    ImmutableSet.of(),
                    ImmutableSet.of(),
                    new ResourceEstimates(Optional.empty(), Optional.empty(), Optional.empty()),
                    ImmutableMap.of(),
                    ImmutableMap.of(),
                    ImmutableMap.of(),
                    Optional.empty(),
                    false,
                    Optional.empty(),
                    Optional.empty());

            String qualifiedName = mvName.catalogName() + "." + mvName.schemaName() + "." + mvName.objectName();
            String refreshQuery = "REFRESH MATERIALIZED VIEW " + qualifiedName;

            // Record attempt time before dispatching
            lastRefreshAttempt.put(mvName, ZonedDateTime.now(ZoneOffset.UTC));

            log.info("Dispatching refresh for %s (queryId: %s)", mvName, queryId);

            ListenableFuture<Void> future = dispatchManager.createQuery(
                    queryId,
                    querySpan,
                    Slug.createNew(),
                    sessionContext,
                    refreshQuery);

            addCallback(future, new FutureCallback<>()
            {
                @Override
                public void onSuccess(Void result)
                {
                    refreshSemaphore.release();
                    log.info("Refresh dispatched successfully for %s (queryId: %s)", mvName, queryId);
                }

                @Override
                public void onFailure(Throwable t)
                {
                    refreshSemaphore.release();
                    log.error(t, "Failed to dispatch refresh for %s (queryId: %s)", mvName, queryId);
                }
            }, directExecutor());
        }
        catch (Exception e) {
            refreshSemaphore.release();
            log.error(e, "Error dispatching refresh for %s", mvName);
        }
    }

    @PreDestroy
    public void stop()
    {
        executor.shutdownNow();
    }
}
