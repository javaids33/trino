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

import java.util.Map;

import static io.trino.testing.containers.Minio.MINIO_ACCESS_KEY;
import static io.trino.testing.containers.Minio.MINIO_REGION;
import static io.trino.testing.containers.Minio.MINIO_SECRET_KEY;

/**
 * Boots a Trino server with Iceberg connector backed by Docker Minio + Hive Metastore,
 * with the REFRESH SCHEDULE cron scheduler enabled.
 *
 * <p>Requires Docker Desktop running.
 *
 * <p>Connect via CLI: trino --server http://localhost:8080 --catalog iceberg --schema tpch
 * <p>Trino Web UI: http://localhost:8080
 */
public final class RunScheduledRefreshServer
{
    private RunScheduledRefreshServer() {}

    public static void main(String[] args)
            throws Exception
    {
        Logger log = Logger.get(RunScheduledRefreshServer.class);

        String bucketName = "test-bucket";

        log.info("Starting Minio + Hive Metastore Docker containers...");
        @SuppressWarnings("resource")
        Hive3MinioDataLake hiveMinioDataLake = new Hive3MinioDataLake(bucketName);
        hiveMinioDataLake.start();
        log.info("Minio endpoint: %s", hiveMinioDataLake.getMinio().getMinioAddress());
        log.info("Hive Metastore endpoint: %s", hiveMinioDataLake.getHiveHadoop().getHiveMetastoreEndpoint());

        @SuppressWarnings("resource")
        DistributedQueryRunner queryRunner = IcebergQueryRunner.builder()
                .addCoordinatorProperty("http-server.http.port", "8080")
                .setCoordinatorProperties(Map.of(
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

        log.info("======== SERVER STARTED ========");
        log.info("\n====\n%s\n====", queryRunner.getCoordinator().getBaseUrl());
        log.info("Materialized view refresh scheduler is ENABLED (scan-interval=30s)");
        log.info("Iceberg catalog backed by Docker Minio + Hive Metastore");
        log.info("Web UI: http://localhost:8080");
        log.info("CLI:    trino --server http://localhost:8080 --catalog iceberg --schema tpch");
    }
}
