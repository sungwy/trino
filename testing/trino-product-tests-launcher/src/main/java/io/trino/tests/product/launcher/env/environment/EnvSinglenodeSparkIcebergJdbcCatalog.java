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
package io.trino.tests.product.launcher.env.environment;

import com.google.common.collect.ImmutableList;
import io.trino.tests.product.launcher.docker.DockerFiles;
import io.trino.tests.product.launcher.env.DockerContainer;
import io.trino.tests.product.launcher.env.Environment;
import io.trino.tests.product.launcher.env.EnvironmentConfig;
import io.trino.tests.product.launcher.env.EnvironmentProvider;
import io.trino.tests.product.launcher.env.common.Hadoop;
import io.trino.tests.product.launcher.env.common.Standard;
import io.trino.tests.product.launcher.env.common.TestsEnvironment;
import io.trino.tests.product.launcher.testcontainers.PortBinder;
import org.testcontainers.containers.startupcheck.IsRunningStartupCheckStrategy;

import javax.inject.Inject;

import static io.trino.tests.product.launcher.docker.ContainerUtil.forSelectedPorts;
import static io.trino.tests.product.launcher.env.EnvironmentContainers.HADOOP;
import static io.trino.tests.product.launcher.env.EnvironmentContainers.TESTS;
import static io.trino.tests.product.launcher.env.common.Hadoop.CONTAINER_HADOOP_INIT_D;
import static io.trino.tests.product.launcher.env.common.Standard.CONTAINER_TEMPTO_PROFILE_CONFIG;
import static java.util.Objects.requireNonNull;
import static org.testcontainers.utility.MountableFile.forHostPath;

@TestsEnvironment
public class EnvSinglenodeSparkIcebergJdbcCatalog
        extends EnvironmentProvider
{
    private static final int SPARK_THRIFT_PORT = 10213;
    // Use non-default PostgreSQL port to avoid conflicts with locally installed PostgreSQL if any.
    public static final int POSTGRESQL_PORT = 25432;

    private final DockerFiles dockerFiles;
    private final PortBinder portBinder;
    private final String imagesVersion;
    private final String hadoopImagesVersion;

    @Inject
    public EnvSinglenodeSparkIcebergJdbcCatalog(Standard standard, Hadoop hadoop, DockerFiles dockerFiles, EnvironmentConfig config, PortBinder portBinder)
    {
        super(ImmutableList.of(standard, hadoop));
        this.dockerFiles = requireNonNull(dockerFiles, "dockerFiles is null");
        this.portBinder = requireNonNull(portBinder, "portBinder is null");
        this.imagesVersion = requireNonNull(config, "config is null").getImagesVersion();
        this.hadoopImagesVersion = requireNonNull(config, "config is null").getHadoopImagesVersion();
    }

    @Override
    public void extendEnvironment(Environment.Builder builder)
    {
        String dockerImageName = "ghcr.io/trinodb/testing/hdp3.1-hive:" + hadoopImagesVersion;

        builder.configureContainer(HADOOP, container -> {
            container.setDockerImageName(dockerImageName);
            container.withCopyFileToContainer(
                    forHostPath(dockerFiles.getDockerFilesHostPath("conf/environment/singlenode-spark-iceberg/apply-hive-config-for-iceberg.sh")),
                    CONTAINER_HADOOP_INIT_D + "/apply-hive-config-for-iceberg.sh");
        });

        builder.configureContainer(TESTS, dockerContainer -> {
            dockerContainer.withCopyFileToContainer(
                    forHostPath(dockerFiles.getDockerFilesHostPath("conf/tempto/tempto-configuration-for-hive3.yaml")),
                    CONTAINER_TEMPTO_PROFILE_CONFIG);
        });

        builder.addConnector("iceberg", forHostPath(dockerFiles.getDockerFilesHostPath("conf/environment/singlenode-spark-iceberg-jdbc-catalog/iceberg.properties")));

        builder.addContainer(createPostgreSql());

        builder.addContainer(createSpark())
                .containerDependsOn("spark", HADOOP);
    }

    @SuppressWarnings("resource")
    public DockerContainer createPostgreSql()
    {
        DockerContainer container = new DockerContainer("postgres:14.2", "postgresql")
                .withEnv("POSTGRES_PASSWORD", "test")
                .withEnv("POSTGRES_USER", "test")
                .withEnv("POSTGRES_DB", "test")
                .withEnv("PGPORT", Integer.toString(POSTGRESQL_PORT))
                .withCopyFileToContainer(
                        forHostPath(dockerFiles.getDockerFilesHostPath("conf/environment/singlenode-spark-iceberg-jdbc-catalog/create-table.sql")),
                        "/docker-entrypoint-initdb.d/create-table.sql")
                .withStartupCheckStrategy(new IsRunningStartupCheckStrategy())
                .waitingFor(forSelectedPorts(POSTGRESQL_PORT));

        portBinder.exposePort(container, POSTGRESQL_PORT);

        return container;
    }

    @SuppressWarnings("resource")
    private DockerContainer createSpark()
    {
        try {
            DockerContainer container = new DockerContainer("ghcr.io/trinodb/testing/spark3.0-iceberg:" + imagesVersion, "spark")
                    .withEnv("HADOOP_USER_NAME", "hive")
                    .withCopyFileToContainer(
                            forHostPath(dockerFiles.getDockerFilesHostPath("conf/environment/singlenode-spark-iceberg-jdbc-catalog/spark-defaults.conf")),
                            "/spark/conf/spark-defaults.conf")
                    .withCommand(
                            "spark-submit",
                            "--master", "local[*]",
                            "--class", "org.apache.spark.sql.hive.thriftserver.HiveThriftServer2",
                            "--name", "Thrift JDBC/ODBC Server",
                            "--conf", "spark.hive.server2.thrift.port=" + SPARK_THRIFT_PORT,
                            "spark-internal")
                    .withStartupCheckStrategy(new IsRunningStartupCheckStrategy())
                    .waitingFor(forSelectedPorts(SPARK_THRIFT_PORT));

            portBinder.exposePort(container, SPARK_THRIFT_PORT);

            return container;
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
