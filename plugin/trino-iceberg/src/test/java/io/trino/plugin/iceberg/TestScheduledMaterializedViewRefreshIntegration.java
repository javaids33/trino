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
package io.trino.plugin.iceberg;

import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;
import io.trino.tpch.TpchTable;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration test that validates the REFRESH SCHEDULE feature end-to-end.
 * Boots a real Trino coordinator with the internal refresh scheduler enabled.
 */
public class TestScheduledMaterializedViewRefreshIntegration
        extends AbstractTestQueryFramework
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        DistributedQueryRunner queryRunner = IcebergQueryRunner.builder()
                .setCoordinatorProperties(Map.of(
                        "materialized-view-refresh.enabled", "true",
                        "materialized-view-refresh.scan-interval", "10s",
                        "materialized-view-refresh.coordinator-id", "test-coord-1",
                        "materialized-view-refresh.max-concurrent-refreshes", "2",
                        "materialized-view-refresh.lock-timeout", "5m"))
                .setInitialTables(TpchTable.NATION)
                .build();
        return queryRunner;
    }

    @Test
    public void testCreateWithRefreshSchedule()
    {
        assertUpdate(
                "CREATE MATERIALIZED VIEW test_create_schedule " +
                "GRACE PERIOD INTERVAL '2' HOUR " +
                "REFRESH SCHEDULE '0 * * * *' " +
                "AS SELECT count(*) cnt FROM nation");

        // Verify SHOW CREATE
        String showCreate = (String) computeScalar(
                "SHOW CREATE MATERIALIZED VIEW test_create_schedule");
        assertThat(showCreate).contains("REFRESH SCHEDULE '0 * * * *'");
        assertThat(showCreate).contains("GRACE PERIOD");

        assertUpdate("DROP MATERIALIZED VIEW test_create_schedule");
    }

    @Test
    public void testInvalidCronFails()
    {
        assertQueryFails(
                "CREATE MATERIALIZED VIEW bad_cron " +
                "REFRESH SCHEDULE 'garbage' " +
                "AS SELECT 1",
                ".*Invalid REFRESH SCHEDULE cron expression.*");
    }

    @Test
    public void testManualRefreshOfScheduledView()
    {
        assertUpdate(
                "CREATE MATERIALIZED VIEW manual_refresh_test " +
                "REFRESH SCHEDULE '0 0 * * *' " +
                "AS SELECT count(*) cnt FROM nation");

        // Manual refresh works (external scheduler path)
        computeActual("REFRESH MATERIALIZED VIEW manual_refresh_test");
        assertThat(computeScalar("SELECT cnt FROM manual_refresh_test")).isEqualTo(25L);

        assertUpdate("DROP MATERIALIZED VIEW manual_refresh_test");
    }

    @Test
    public void testOrReplaceUpdatesSchedule()
    {
        assertUpdate(
                "CREATE MATERIALIZED VIEW replace_schedule_test " +
                "REFRESH SCHEDULE '0 * * * *' " +
                "AS SELECT 1 AS x");

        String show1 = (String) computeScalar(
                "SHOW CREATE MATERIALIZED VIEW replace_schedule_test");
        assertThat(show1).contains("REFRESH SCHEDULE '0 * * * *'");

        assertUpdate(
                "CREATE OR REPLACE MATERIALIZED VIEW replace_schedule_test " +
                "REFRESH SCHEDULE '*/30 * * * *' " +
                "AS SELECT 2 AS x");

        String show2 = (String) computeScalar(
                "SHOW CREATE MATERIALIZED VIEW replace_schedule_test");
        assertThat(show2).contains("REFRESH SCHEDULE '*/30 * * * *'");

        assertUpdate("DROP MATERIALIZED VIEW replace_schedule_test");
    }

    @Test
    public void testNoScheduleStillWorks()
    {
        // Ensure we didn't break regular MVs
        assertUpdate(
                "CREATE MATERIALIZED VIEW no_schedule " +
                "AS SELECT name FROM nation WHERE nationkey < 3");

        computeActual("REFRESH MATERIALIZED VIEW no_schedule");
        assertThat(computeScalar("SELECT count(*) FROM no_schedule")).isEqualTo(3L);

        String show = (String) computeScalar(
                "SHOW CREATE MATERIALIZED VIEW no_schedule");
        assertThat(show).doesNotContain("REFRESH SCHEDULE");

        assertUpdate("DROP MATERIALIZED VIEW no_schedule");
    }

    @Test
    public void testMultipleCronFormats()
    {
        // Every 15 minutes
        assertUpdate("CREATE MATERIALIZED VIEW cron_15m REFRESH SCHEDULE '*/15 * * * *' AS SELECT 1 x");
        assertUpdate("DROP MATERIALIZED VIEW cron_15m");

        // Weekdays at 6am
        assertUpdate("CREATE MATERIALIZED VIEW cron_weekday REFRESH SCHEDULE '0 6 * * 1-5' AS SELECT 1 x");
        assertUpdate("DROP MATERIALIZED VIEW cron_weekday");

        // 1st and 15th of month at midnight
        assertUpdate("CREATE MATERIALIZED VIEW cron_bimonth REFRESH SCHEDULE '0 0 1,15 * *' AS SELECT 1 x");
        assertUpdate("DROP MATERIALIZED VIEW cron_bimonth");

        // Every 6 hours
        assertUpdate("CREATE MATERIALIZED VIEW cron_6h REFRESH SCHEDULE '0 */6 * * *' AS SELECT 1 x");
        assertUpdate("DROP MATERIALIZED VIEW cron_6h");
    }
}
