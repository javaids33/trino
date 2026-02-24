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

import io.airlift.log.Logger;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.MaterializedResult;
import io.trino.testing.QueryRunner;
import io.trino.tpch.TpchTable;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Demo test that exercises the REFRESH SCHEDULE feature end-to-end
 * and prints results to log output, simulating interactive CLI use.
 * This is Approach C from the validation plan.
 */
public class TestScheduledRefreshDemo
        extends AbstractTestQueryFramework
{
    private static final Logger log = Logger.get(TestScheduledRefreshDemo.class);

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        DistributedQueryRunner queryRunner = IcebergQueryRunner.builder()
                .setCoordinatorProperties(Map.of(
                        "materialized-view-refresh.enabled", "true",
                        "materialized-view-refresh.scan-interval", "10s",
                        "materialized-view-refresh.coordinator-id", "demo-coordinator",
                        "materialized-view-refresh.max-concurrent-refreshes", "2",
                        "materialized-view-refresh.lock-timeout", "5m"))
                .setInitialTables(TpchTable.NATION)
                .build();
        return queryRunner;
    }

    @Test
    public void runDemo()
    {
        log.info("============================================");
        log.info("  REFRESH SCHEDULE Feature Demo (Approach C)");
        log.info("============================================");

        // -- TEST 1: Create MV with REFRESH SCHEDULE --
        log.info("");
        log.info("-- TEST 1: Create materialized view with REFRESH SCHEDULE --");
        log.info(">>> CREATE MATERIALIZED VIEW test_scheduled_mv GRACE PERIOD INTERVAL '2' HOUR REFRESH SCHEDULE '*/2 * * * *' COMMENT 'Test scheduled refresh every 2 minutes' AS SELECT count(*) AS nation_count FROM nation");
        assertUpdate(
                "CREATE MATERIALIZED VIEW test_scheduled_mv " +
                "GRACE PERIOD INTERVAL '2' HOUR " +
                "REFRESH SCHEDULE '*/2 * * * *' " +
                "COMMENT 'Test scheduled refresh every 2 minutes' " +
                "AS SELECT count(*) AS nation_count FROM nation");
        log.info("CREATE MATERIALIZED VIEW - OK");

        // -- TEST 2: SHOW CREATE --
        log.info("");
        log.info("-- TEST 2: Verify SHOW CREATE includes REFRESH SCHEDULE --");
        log.info(">>> SHOW CREATE MATERIALIZED VIEW test_scheduled_mv");
        String showCreate = (String) computeScalar("SHOW CREATE MATERIALIZED VIEW test_scheduled_mv");
        log.info("Result:\n%s", showCreate);
        assertThat(showCreate).contains("REFRESH SCHEDULE '*/2 * * * *'");
        log.info("SHOW CREATE - OK (contains REFRESH SCHEDULE)");

        // -- TEST 3: System table metadata --
        log.info("");
        log.info("-- TEST 3: Check system.metadata.materialized_views --");
        log.info(">>> SELECT name, comment FROM system.metadata.materialized_views WHERE name = 'test_scheduled_mv'");
        MaterializedResult sysMeta = computeActual(
                "SELECT name, comment FROM system.metadata.materialized_views WHERE name = 'test_scheduled_mv'");
        log.info("Result: %s", sysMeta);
        assertThat(sysMeta.getRowCount()).isEqualTo(1);
        log.info("System table query - OK");

        // -- TEST 4: Manual refresh (external scheduler path) --
        log.info("");
        log.info("-- TEST 4: External scheduler simulation -- manual REFRESH --");
        log.info(">>> REFRESH MATERIALIZED VIEW test_scheduled_mv");
        computeActual("REFRESH MATERIALIZED VIEW test_scheduled_mv");
        log.info("REFRESH MATERIALIZED VIEW - OK");

        log.info(">>> SELECT * FROM test_scheduled_mv");
        MaterializedResult data = computeActual("SELECT * FROM test_scheduled_mv");
        log.info("Result: %s", data);
        assertThat(data.getRowCount()).isEqualTo(1);
        assertThat(computeScalar("SELECT nation_count FROM test_scheduled_mv")).isEqualTo(25L);
        log.info("Query result - OK (nation_count = 25)");

        // -- TEST 5: Invalid cron expression --
        log.info("");
        log.info("-- TEST 5: Invalid cron expression should fail --");
        log.info(">>> CREATE MATERIALIZED VIEW bad_cron_mv REFRESH SCHEDULE 'not-a-cron' AS SELECT 1");
        assertQueryFails(
                "CREATE MATERIALIZED VIEW bad_cron_mv REFRESH SCHEDULE 'not-a-cron' AS SELECT 1",
                ".*Invalid REFRESH SCHEDULE cron expression.*");
        log.info("Invalid cron rejected - OK");

        // -- TEST 6: OR REPLACE updates schedule --
        log.info("");
        log.info("-- TEST 6: OR REPLACE updates the schedule --");
        log.info(">>> CREATE OR REPLACE MATERIALIZED VIEW test_scheduled_mv REFRESH SCHEDULE '*/5 * * * *' AS SELECT count(*) AS nation_count FROM nation");
        assertUpdate(
                "CREATE OR REPLACE MATERIALIZED VIEW test_scheduled_mv " +
                "GRACE PERIOD INTERVAL '1' HOUR " +
                "REFRESH SCHEDULE '*/5 * * * *' " +
                "AS SELECT count(*) AS nation_count FROM nation");
        String show2 = (String) computeScalar("SHOW CREATE MATERIALIZED VIEW test_scheduled_mv");
        log.info("Updated SHOW CREATE:\n%s", show2);
        assertThat(show2).contains("REFRESH SCHEDULE '*/5 * * * *'");
        log.info("OR REPLACE - OK (schedule updated to '*/5 * * * *')");

        // -- TEST 7: Regular MV without schedule still works --
        log.info("");
        log.info("-- TEST 7: Regular MV without schedule still works --");
        log.info(">>> CREATE MATERIALIZED VIEW unscheduled_mv AS SELECT name FROM nation WHERE nationkey < 5");
        assertUpdate("CREATE MATERIALIZED VIEW unscheduled_mv AS SELECT name FROM nation WHERE nationkey < 5");
        computeActual("REFRESH MATERIALIZED VIEW unscheduled_mv");
        MaterializedResult unscheduledData = computeActual("SELECT * FROM unscheduled_mv");
        log.info("Unscheduled MV data: %s", unscheduledData);
        assertThat(computeScalar("SELECT count(*) FROM unscheduled_mv")).isEqualTo(5L);
        String showUnscheduled = (String) computeScalar("SHOW CREATE MATERIALIZED VIEW unscheduled_mv");
        assertThat(showUnscheduled).doesNotContain("REFRESH SCHEDULE");
        log.info("Regular MV - OK (no REFRESH SCHEDULE in SHOW CREATE)");

        // -- TEST 8: Multiple cron formats --
        log.info("");
        log.info("-- TEST 8: Multiple cron format validation --");
        String[] crons = {"*/15 * * * *", "0 6 * * 1-5", "0 0 1,15 * *", "0 */6 * * *"};
        String[] names = {"cron_15m", "cron_weekday", "cron_bimonth", "cron_6h"};
        for (int i = 0; i < crons.length; i++) {
            log.info(">>> CREATE MV %s REFRESH SCHEDULE '%s'", names[i], crons[i]);
            assertUpdate("CREATE MATERIALIZED VIEW " + names[i] + " REFRESH SCHEDULE '" + crons[i] + "' AS SELECT 1 x");
            assertUpdate("DROP MATERIALIZED VIEW " + names[i]);
            log.info("  %s - OK", crons[i]);
        }

        // -- Cleanup --
        log.info("");
        log.info("-- CLEANUP --");
        assertUpdate("DROP MATERIALIZED VIEW IF EXISTS test_scheduled_mv");
        assertUpdate("DROP MATERIALIZED VIEW IF EXISTS unscheduled_mv");
        log.info("Cleanup - OK");

        log.info("");
        log.info("============================================");
        log.info("  ALL DEMO TESTS PASSED SUCCESSFULLY!");
        log.info("============================================");
    }
}
