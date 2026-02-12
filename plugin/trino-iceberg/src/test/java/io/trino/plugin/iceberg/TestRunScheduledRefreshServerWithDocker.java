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
import io.trino.plugin.hive.containers.Hive3MinioDataLake;
import io.trino.testing.DistributedQueryRunner;
import io.trino.tpch.TpchTable;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.Map;
import java.util.concurrent.TimeUnit;

import static io.trino.testing.containers.Minio.MINIO_ACCESS_KEY;
import static io.trino.testing.containers.Minio.MINIO_REGION;
import static io.trino.testing.containers.Minio.MINIO_SECRET_KEY;

/**
 * Starts a Trino server backed by Docker Minio + Hive Metastore with REFRESH SCHEDULE enabled.
 * The server stays running for manual validation via the Trino Web UI at http://localhost:8080.
 *
 * <p>Run with: mvn test -pl plugin/trino-iceberg \
 *     -Dtest=TestRunScheduledRefreshServerWithDocker \
 *     -Dair.check.skip-all=true -nsu
 */
public class TestRunScheduledRefreshServerWithDocker
{
    private static final Logger log = Logger.get(TestRunScheduledRefreshServerWithDocker.class);

    @Test
    @Timeout(value = 120, unit = TimeUnit.MINUTES)
    public void runServer()
            throws Exception
    {
        String bucketName = "test-bucket";

        log.info("Starting Minio + Hive Metastore Docker containers...");
        Hive3MinioDataLake hiveMinioDataLake = new Hive3MinioDataLake(bucketName);
        hiveMinioDataLake.start();
        log.info("Minio endpoint: %s", hiveMinioDataLake.getMinio().getMinioAddress());
        log.info("Hive Metastore endpoint: %s", hiveMinioDataLake.getHiveHadoop().getHiveMetastoreEndpoint());

        DistributedQueryRunner queryRunner = IcebergQueryRunner.builder()
                .setCoordinatorProperties(Map.of(
                        "http-server.http.port", "8080",
                        "materialized-view-refresh.enabled", "true",
                        "materialized-view-refresh.scan-interval", "30s",
                        "materialized-view-refresh.coordinator-id", "local-test-coordinator-1",
                        "materialized-view-refresh.max-concurrent-refreshes", "2",
                        "materialized-view-refresh.lock-timeout", "10m"))
                .setIcebergProperties(Map.of(
                        "iceberg.catalog.type", "HIVE_METASTORE",
                        "hive.metastore.uri", hiveMinioDataLake.getHiveHadoop().getHiveMetastoreEndpoint().toString(),
                        "fs.native-s3.enabled", "true",
                        "s3.aws-access-key", MINIO_ACCESS_KEY,
                        "s3.aws-secret-key", MINIO_SECRET_KEY,
                        "s3.region", MINIO_REGION,
                        "s3.endpoint", hiveMinioDataLake.getMinio().getMinioAddress(),
                        "s3.path-style-access", "true",
                        "s3.streaming.part-size", "5MB"))
                .setSchemaInitializer(
                        SchemaInitializer.builder()
                                .withSchemaName("tpch")
                                .withClonedTpchTables(TpchTable.getTables())
                                .withSchemaProperties(Map.of("location", "'s3://" + bucketName + "/tpch'"))
                                .build())
                .build();

        String baseUrl = queryRunner.getCoordinator().getBaseUrl().toString();
        log.info("======== SERVER STARTED ========");
        log.info("Web UI:  %s", baseUrl);
        log.info("CLI:     trino --server %s --catalog iceberg --schema tpch", baseUrl);
        log.info("Scheduler: ENABLED (scan-interval=30s)");
        log.info("");
        log.info("Try these SQL commands:");
        log.info("  CREATE MATERIALIZED VIEW mv_nation_count");
        log.info("    GRACE PERIOD INTERVAL '1' HOUR");
        log.info("    REFRESH SCHEDULE '*/1 * * * *'");
        log.info("    AS SELECT count(*) AS cnt FROM nation;");
        log.info("");
        log.info("  SHOW CREATE MATERIALIZED VIEW mv_nation_count;");
        log.info("  REFRESH MATERIALIZED VIEW mv_nation_count;");
        log.info("  SELECT * FROM mv_nation_count;");
        log.info("");
        log.info("Server will stay running. Press Ctrl+C to stop.");

        // Keep the server alive until killed
        Thread.sleep(TimeUnit.MINUTES.toMillis(120));

        queryRunner.close();
        hiveMinioDataLake.close();
    }
}
