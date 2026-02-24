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

import com.google.common.collect.ImmutableList;
import io.airlift.units.Duration;
import io.trino.Session;
import io.trino.execution.QueryIdGenerator;
import io.trino.metadata.AbstractMockMetadata;
import io.trino.metadata.MaterializedViewDefinition;
import io.trino.metadata.QualifiedObjectName;
import io.trino.metadata.SessionPropertyManager;
import io.trino.metadata.ViewColumn;
import io.trino.security.AllowAllAccessControl;
import io.trino.spi.connector.CatalogSchemaTableName;
import io.trino.spi.connector.MaterializedViewFreshness;
import io.trino.spi.connector.MaterializedViewFreshness.Freshness;
import io.trino.spi.security.Identity;
import io.trino.transaction.TestingTransactionManager;
import io.trino.transaction.TransactionId;
import io.trino.transaction.TransactionManager;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static io.trino.spi.connector.ConnectorMaterializedViewDefinition.WhenStaleBehavior.INLINE;
import static io.trino.spi.type.BigintType.BIGINT;
import static org.assertj.core.api.Assertions.assertThat;

public class TestMaterializedViewRefreshDeduplication
{
    private static final QualifiedObjectName MV_NAME = new QualifiedObjectName("catalog", "schema", "test_mv");

    private static final MaterializedViewDefinition MV_WITH_SCHEDULE = new MaterializedViewDefinition(
            "SELECT * FROM base_table",
            Optional.of("catalog"),
            Optional.of("schema"),
            ImmutableList.of(new ViewColumn("col1", BIGINT.getTypeId(), Optional.empty())),
            Optional.empty(),
            INLINE,
            Optional.of("* * * * *"),
            Optional.empty(),
            Identity.ofUser("owner"),
            ImmutableList.of(),
            Optional.of(new CatalogSchemaTableName("catalog", "schema", "test_mv_storage")));

    private static final MaterializedViewDefinition MV_WITHOUT_SCHEDULE = new MaterializedViewDefinition(
            "SELECT * FROM base_table",
            Optional.of("catalog"),
            Optional.of("schema"),
            ImmutableList.of(new ViewColumn("col1", BIGINT.getTypeId(), Optional.empty())),
            Optional.empty(),
            INLINE,
            Optional.empty(),
            Identity.ofUser("owner"),
            ImmutableList.of(),
            Optional.of(new CatalogSchemaTableName("catalog", "schema", "test_mv_storage")));

    // Far future time ensures cron "* * * * *" is always due
    private static final ZonedDateTime FAR_FUTURE = ZonedDateTime.of(2099, 1, 1, 12, 0, 0, 0, ZoneOffset.UTC);

    @Test
    public void testStaleViewTriggersRefresh()
    {
        TestMetadata metadata = new TestMetadata();
        metadata.addMaterializedView(MV_NAME, MV_WITH_SCHEDULE);
        metadata.setFreshness(MV_NAME, new MaterializedViewFreshness(Freshness.STALE, Optional.empty()));

        MaterializedViewRefreshScheduler scheduler = createScheduler(metadata, "coordinator-1");
        Session session = createSession();

        assertThat(scheduler.shouldRefresh(session, MV_NAME, FAR_FUTURE))
                .as("Stale MV should trigger refresh")
                .isTrue();
        assertThat(metadata.getFreshnessCheckCount())
                .as("Freshness should have been checked")
                .isEqualTo(1);
    }

    @Test
    public void testFreshViewSkipsRefresh()
    {
        TestMetadata metadata = new TestMetadata();
        metadata.addMaterializedView(MV_NAME, MV_WITH_SCHEDULE);
        metadata.setFreshness(MV_NAME, new MaterializedViewFreshness(Freshness.FRESH, Optional.of(Instant.now())));

        MaterializedViewRefreshScheduler scheduler = createScheduler(metadata, "coordinator-1");
        Session session = createSession();

        assertThat(scheduler.shouldRefresh(session, MV_NAME, FAR_FUTURE))
                .as("Fresh MV should skip refresh")
                .isFalse();
        assertThat(metadata.getFreshnessCheckCount())
                .as("Freshness should have been checked")
                .isEqualTo(1);
    }

    @Test
    public void testMultiCoordinatorDeduplication()
    {
        // Shared metadata — simulates shared HMS + S3
        TestMetadata sharedMetadata = new TestMetadata();
        sharedMetadata.addMaterializedView(MV_NAME, MV_WITH_SCHEDULE);
        sharedMetadata.setFreshness(MV_NAME, new MaterializedViewFreshness(Freshness.STALE, Optional.empty()));

        // Two coordinators, both pointing at the same metadata
        MaterializedViewRefreshScheduler coordinatorA = createScheduler(sharedMetadata, "coordinator-A");
        MaterializedViewRefreshScheduler coordinatorB = createScheduler(sharedMetadata, "coordinator-B");
        Session session = createSession();

        // Coordinator A checks — MV is stale, should refresh
        assertThat(coordinatorA.shouldRefresh(session, MV_NAME, FAR_FUTURE))
                .as("Coordinator A should see stale MV and trigger refresh")
                .isTrue();

        // Simulate: Coordinator A's refresh completes, MV is now FRESH
        sharedMetadata.setFreshness(MV_NAME, new MaterializedViewFreshness(Freshness.FRESH, Optional.of(Instant.now())));

        // Coordinator B checks — MV is now fresh, should skip
        assertThat(coordinatorB.shouldRefresh(session, MV_NAME, FAR_FUTURE))
                .as("Coordinator B should see fresh MV and skip refresh")
                .isFalse();

        // Both coordinators checked freshness
        assertThat(sharedMetadata.getFreshnessCheckCount())
                .as("Both coordinators should have checked freshness")
                .isEqualTo(2);
    }

    @Test
    public void testFreshnessCheckFailureProceeds()
    {
        TestMetadata metadata = new TestMetadata();
        metadata.addMaterializedView(MV_NAME, MV_WITH_SCHEDULE);
        metadata.setThrowOnFreshnessCheck(true);

        MaterializedViewRefreshScheduler scheduler = createScheduler(metadata, "coordinator-1");
        Session session = createSession();

        // When freshness check throws, refresh should still proceed (fail-open)
        assertThat(scheduler.shouldRefresh(session, MV_NAME, FAR_FUTURE))
                .as("Should proceed with refresh when freshness check fails")
                .isTrue();
    }

    @Test
    public void testNoScheduleSkipsRefresh()
    {
        TestMetadata metadata = new TestMetadata();
        metadata.addMaterializedView(MV_NAME, MV_WITHOUT_SCHEDULE);

        MaterializedViewRefreshScheduler scheduler = createScheduler(metadata, "coordinator-1");
        Session session = createSession();

        assertThat(scheduler.shouldRefresh(session, MV_NAME, FAR_FUTURE))
                .as("MV without schedule should not refresh")
                .isFalse();
        assertThat(metadata.getFreshnessCheckCount())
                .as("Freshness should not be checked for MV without schedule")
                .isEqualTo(0);
    }

    @Test
    public void testCronNotDueSkipsRefresh()
    {
        // MV with hourly schedule: fires at minute 0 of each hour
        MaterializedViewDefinition hourlyMv = new MaterializedViewDefinition(
                "SELECT * FROM base_table",
                Optional.of("catalog"),
                Optional.of("schema"),
                ImmutableList.of(new ViewColumn("col1", BIGINT.getTypeId(), Optional.empty())),
                Optional.empty(),
                INLINE,
                Optional.of("0 * * * *"),
                Optional.empty(),
                Identity.ofUser("owner"),
                ImmutableList.of(),
                Optional.of(new CatalogSchemaTableName("catalog", "schema", "test_mv_storage")));

        TestMetadata metadata = new TestMetadata();
        metadata.addMaterializedView(MV_NAME, hourlyMv);
        metadata.setFreshness(MV_NAME, new MaterializedViewFreshness(Freshness.STALE, Optional.empty()));

        MaterializedViewRefreshScheduler scheduler = createScheduler(metadata, "coordinator-1");
        Session session = createSession();

        // Check at minute 30 — the next fire after epoch is hour 1 minute 0,
        // but we use a time that's before that first fire time.
        // Actually, with epoch as reference, nextFireTime = 1970-01-01T01:00Z.
        // A time before that means cron is not due yet.
        ZonedDateTime beforeFirstFire = ZonedDateTime.of(1970, 1, 1, 0, 30, 0, 0, ZoneOffset.UTC);

        assertThat(scheduler.shouldRefresh(session, MV_NAME, beforeFirstFire))
                .as("Cron not yet due should skip without checking freshness")
                .isFalse();
        assertThat(metadata.getFreshnessCheckCount())
                .as("Freshness should not be checked when cron is not due")
                .isEqualTo(0);
    }

    private MaterializedViewRefreshScheduler createScheduler(TestMetadata metadata, String coordinatorId)
    {
        MaterializedViewRefreshConfig config = new MaterializedViewRefreshConfig()
                .setEnabled(true)
                .setCoordinatorId(coordinatorId)
                .setScanInterval(new Duration(60, TimeUnit.SECONDS))
                .setMaxConcurrentRefreshes(4);

        return new MaterializedViewRefreshScheduler(metadata, config);
    }

    private Session createSession()
    {
        TransactionManager transactionManager = new TestingTransactionManager();
        AllowAllAccessControl accessControl = new AllowAllAccessControl();
        SessionPropertyManager sessionPropertyManager = new SessionPropertyManager();

        Session baseSession = Session.builder(sessionPropertyManager)
                .setQueryId(new QueryIdGenerator().createNextQueryId())
                .setIdentity(Identity.ofUser("test-user"))
                .setOriginalIdentity(Identity.ofUser("test-user"))
                .setSource("test")
                .build();

        TransactionId transactionId = transactionManager.beginTransaction(false);
        return baseSession.beginTransactionId(transactionId, transactionManager, accessControl);
    }

    private static class TestMetadata
            extends AbstractMockMetadata
    {
        private final ConcurrentHashMap<QualifiedObjectName, MaterializedViewDefinition> mvDefinitions = new ConcurrentHashMap<>();
        private final ConcurrentHashMap<QualifiedObjectName, MaterializedViewFreshness> freshnessMap = new ConcurrentHashMap<>();
        private final AtomicInteger freshnessCheckCount = new AtomicInteger();
        private volatile boolean throwOnFreshnessCheck;

        public void addMaterializedView(QualifiedObjectName name, MaterializedViewDefinition definition)
        {
            mvDefinitions.put(name, definition);
        }

        public void setFreshness(QualifiedObjectName name, MaterializedViewFreshness freshness)
        {
            freshnessMap.put(name, freshness);
        }

        public void setThrowOnFreshnessCheck(boolean throwOnFreshnessCheck)
        {
            this.throwOnFreshnessCheck = throwOnFreshnessCheck;
        }

        public int getFreshnessCheckCount()
        {
            return freshnessCheckCount.get();
        }

        public void resetFreshnessCheckCount()
        {
            freshnessCheckCount.set(0);
        }

        @Override
        public Optional<MaterializedViewDefinition> getMaterializedView(Session session, QualifiedObjectName viewName)
        {
            return Optional.ofNullable(mvDefinitions.get(viewName));
        }

        @Override
        public MaterializedViewFreshness getMaterializedViewFreshness(Session session, QualifiedObjectName name, boolean considerGracePeriod)
        {
            freshnessCheckCount.incrementAndGet();
            if (throwOnFreshnessCheck) {
                throw new RuntimeException("Simulated freshness check failure");
            }
            return freshnessMap.getOrDefault(name, new MaterializedViewFreshness(Freshness.STALE, Optional.empty()));
        }
    }
}
