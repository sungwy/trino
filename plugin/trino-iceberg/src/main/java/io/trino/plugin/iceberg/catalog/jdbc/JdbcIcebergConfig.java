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

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.configuration.ConfigSecuritySensitive;

import javax.validation.constraints.NotEmpty;

import java.util.Optional;

public class JdbcIcebergConfig
{
    private String connectionUrl;
    private String connectionUser;
    private String connectionPassword;
    private String catalogId;
    private Optional<String> defaultWarehouseDir = Optional.empty();
    private boolean caseInsensitiveNameMatching;

    public String getConnectionUrl()
    {
        return connectionUrl;
    }

    @Config("iceberg.metastore.jdbc.connection-url")
    @ConfigDescription("The URI to connect to the JDBC server")
    public JdbcIcebergConfig setConnectionUrl(String connectionUrl)
    {
        this.connectionUrl = connectionUrl;
        return this;
    }

    public Optional<String> getConnectionUser()
    {
        return Optional.ofNullable(connectionUser);
    }

    @Config("iceberg.metastore.jdbc.connection-user")
    @ConfigDescription("User name for JDBC client")
    public JdbcIcebergConfig setConnectionUser(String connectionUser)
    {
        this.connectionUser = connectionUser;
        return this;
    }

    public Optional<String> getConnectionPassword()
    {
        return Optional.ofNullable(connectionPassword);
    }

    @Config("iceberg.metastore.jdbc.connection-password")
    @ConfigSecuritySensitive
    @ConfigDescription("Password for JDBC client")
    public JdbcIcebergConfig setConnectionPassword(String connectionPassword)
    {
        this.connectionPassword = connectionPassword;
        return this;
    }

    @NotEmpty
    public String getCatalogId()
    {
        return catalogId;
    }

    @Config("iceberg.metastore.jdbc.catalogid")
    @ConfigDescription("Iceberg JDBC metastore catalog id")
    public JdbcIcebergConfig setCatalogId(String catalogId)
    {
        this.catalogId = catalogId;
        return this;
    }

    public Optional<String> getDefaultWarehouseDir()
    {
        return defaultWarehouseDir;
    }

    @Config("iceberg.metastore.jdbc.default-warehouse-dir")
    @ConfigDescription("The default warehouse directory to use for JDBC")
    public JdbcIcebergConfig setDefaultWarehouseDir(String defaultWarehouseDir)
    {
        this.defaultWarehouseDir = Optional.ofNullable(defaultWarehouseDir);
        return this;
    }

    public boolean isCaseInsensitiveNameMatching()
    {
        return caseInsensitiveNameMatching;
    }

    @Config("iceberg.metastore.jdbc.case-insensitive-name-matching")
    @ConfigDescription("Match schema and table names case-insensitively")
    public JdbcIcebergConfig setCaseInsensitiveNameMatching(boolean caseInsensitiveNameMatching)
    {
        this.caseInsensitiveNameMatching = caseInsensitiveNameMatching;
        return this;
    }
}
