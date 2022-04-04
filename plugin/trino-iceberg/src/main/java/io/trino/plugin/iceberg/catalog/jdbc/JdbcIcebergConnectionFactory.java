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

import org.jdbi.v3.core.ConnectionFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Optional;
import java.util.Properties;

import static com.google.common.base.Preconditions.checkState;

public class JdbcIcebergConnectionFactory
        implements ConnectionFactory
{
    private final String connectionUrl;
    private final Optional<String> connectionUser;
    private final Optional<String> connectionPassword;

    public JdbcIcebergConnectionFactory(String connectionUrl, Optional<String> connectionUser, Optional<String> connectionPassword)
    {
        this.connectionUrl = connectionUrl;
        this.connectionUser = connectionUser;
        this.connectionPassword = connectionPassword;
    }

    @Override
    public Connection openConnection()
            throws SQLException
    {
        Properties properties = new Properties();
        connectionUser.ifPresent(user -> properties.setProperty("user", user));
        connectionPassword.ifPresent(password -> properties.setProperty("password", password));
        Connection connection = DriverManager.getConnection(connectionUrl, properties);
        checkState(connection != null, "Driver returned null connection, make sure the connection URL '%s' is valid", connectionUrl);
        return connection;
    }
}
