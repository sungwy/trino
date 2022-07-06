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

import io.trino.plugin.iceberg.catalog.AbstractIcebergTableOperations;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.TableNotFoundException;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.io.FileIO;

import java.util.Optional;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static java.util.Objects.requireNonNull;

public class JdbcIcebergTableOperations
        extends AbstractIcebergTableOperations
{
    private final JdbcIcebergClient jdbcClient;

    public JdbcIcebergTableOperations(
            FileIO fileIo,
            JdbcIcebergClient jdbcClient,
            ConnectorSession session,
            String database,
            String table,
            Optional<String> owner,
            Optional<String> location)
    {
        super(fileIo, session, database, table, owner, location);
        this.jdbcClient = requireNonNull(jdbcClient, "jdbcClient is null");
    }

    @Override
    protected String getRefreshedLocation(boolean invalidateCaches)
    {
        return jdbcClient.getMetadataLocation(getSchemaTableName().getSchemaName(), getSchemaTableName().getTableName())
                .orElseThrow(() -> new TableNotFoundException(getSchemaTableName()));
    }

    @Override
    protected void commitNewTable(TableMetadata metadata)
    {
        verify(version == -1, "commitNewTable called on a table which already exists");
        jdbcClient.createTable(database, tableName, writeNewMetadata(metadata, 0));
        shouldRefresh = true;
    }

    @Override
    protected void commitToExistingTable(TableMetadata base, TableMetadata metadata)
    {
        checkState(currentMetadataLocation != null, "No current metadata location for existing table");
        jdbcClient.alterTable(database, tableName, writeNewMetadata(metadata, version + 1), currentMetadataLocation);
        shouldRefresh = true;
    }
}
