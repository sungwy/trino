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
package io.trino.plugin.iceberg.catalog.jdbc;

import org.intellij.lang.annotations.Language;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.utility.MountableFile;

import java.io.Closeable;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

import static java.lang.String.format;
import static org.testcontainers.containers.PostgreSQLContainer.POSTGRESQL_PORT;

public class TestingIcebergJdbcServer
        implements Closeable
{
    private static final String USER = "test";
    private static final String PASSWORD = "test";
    private static final String DATABASE = "tpch";

    private final PostgreSQLContainer<?> dockerContainer;

    public TestingIcebergJdbcServer()
    {
        // TODO: Use Iceberg docker image once the community provides it
        dockerContainer = new PostgreSQLContainer<>("postgres:12.10")
                .withDatabaseName(DATABASE)
                .withUsername(USER)
                .withPassword(PASSWORD)
                .withCopyFileToContainer(MountableFile.forClasspathResource("jdbc/create-table.sql"), "/docker-entrypoint-initdb.d/create-table.sql");
        dockerContainer.start();

        execute("CREATE SCHEMA tpch");
    }

    public void execute(@Language("SQL") String sql)
    {
        execute(getJdbcUrl(), getProperties(), sql);
    }

    private static void execute(String url, Properties properties, String sql)
    {
        try (Connection connection = DriverManager.getConnection(url, properties);
                Statement statement = connection.createStatement()) {
            statement.execute(sql);
        }
        catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public String getUser()
    {
        return USER;
    }

    public String getPassword()
    {
        return PASSWORD;
    }

    public Properties getProperties()
    {
        Properties properties = new Properties();
        properties.setProperty("user", USER);
        properties.setProperty("password", PASSWORD);
        properties.setProperty("currentSchema", "tpch,public");
        return properties;
    }

    public String getJdbcUrl()
    {
        return format("jdbc:postgresql://%s:%s/%s", dockerContainer.getContainerIpAddress(), dockerContainer.getMappedPort(POSTGRESQL_PORT), DATABASE);
    }

    @Override
    public void close()
    {
        dockerContainer.close();
    }
}
