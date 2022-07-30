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

import com.google.common.collect.ImmutableList;
import io.trino.spi.connector.SchemaTableName;
import org.jdbi.v3.core.Handle;
import org.jdbi.v3.core.Jdbi;

import java.util.AbstractMap.SimpleEntry;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;

import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static java.util.Objects.requireNonNull;

public class JdbcIcebergClient
{
    private final JdbcIcebergConnectionFactory connectionFactory;
    private final String catalogId;

    public JdbcIcebergClient(JdbcIcebergConnectionFactory connectionFactory, String catalogId)
    {
        this.connectionFactory = requireNonNull(connectionFactory, "connectionFactory is null");
        this.catalogId = requireNonNull(catalogId, "catalogId is null");
    }

    public List<String> getNamespaces()
    {
        ImmutableList.Builder<String> namespaces = ImmutableList.builder();
        try (Handle handle = Jdbi.open(connectionFactory)) {
            namespaces.addAll(handle.createQuery("" +
                            "SELECT DISTINCT table_namespace " +
                            "FROM iceberg_tables " +
                            "WHERE catalog_name = :catalog")
                    .bind("catalog", catalogId)
                    .mapTo(String.class)
                    .list());
            namespaces.addAll(handle.createQuery("" +
                            "SELECT DISTINCT namespace " +
                            "FROM iceberg_namespace_properties " +
                            "WHERE catalog_name = :catalog")
                    .bind("catalog", catalogId)
                    .mapTo(String.class)
                    .list());
        }
        return namespaces.build();
    }

    public Map<String, Object> getNamespaceProperties(String namespace)
    {
        try (Handle handle = Jdbi.open(connectionFactory)) {
            return handle.createQuery("" +
                            "SELECT property_key, property_value " +
                            "FROM iceberg_namespace_properties " +
                            "WHERE catalog_name = :catalog AND namespace = :schema")
                    .bind("catalog", catalogId)
                    .bind("schema", namespace)
                    .map((rs, ctx) -> new SimpleEntry<>(rs.getString("property_key"), rs.getString("property_value")))
                    .collect(toImmutableMap(Map.Entry::getKey, Map.Entry::getValue));
        }
    }

    public Optional<String> getNamespaceLocation(String namespace)
    {
        try (Handle handle = Jdbi.open(connectionFactory)) {
            return handle.createQuery("" +
                            "SELECT property_value " +
                            "FROM iceberg_namespace_properties " +
                            "WHERE catalog_name = :catalog AND namespace = :schema AND property_key = 'location'")
                    .bind("catalog", catalogId)
                    .bind("schema", namespace)
                    .mapTo(String.class)
                    .findOne();
        }
    }

    public void createNamespace(String namespace, Map<String, Object> properties)
    {
        ImmutableList.Builder<NamespaceProperties> namespaceProperties = ImmutableList.builderWithExpectedSize(properties.size() + 1);
        namespaceProperties.add(new NamespaceProperties(catalogId, namespace, "exists", "true"));
        for (Entry<String, Object> property : properties.entrySet()) {
            namespaceProperties.add(new NamespaceProperties(catalogId, namespace, property.getKey(), property.getValue().toString()));
        }

        try (Handle handle = Jdbi.open(connectionFactory)) {
            handle.createUpdate("" +
                            "INSERT INTO iceberg_namespace_properties " +
                            "(catalog_name, namespace, property_key, property_value) " +
                            "VALUES <values>")
                    .bindBeanList("values", namespaceProperties.build(), ImmutableList.of("catalogName", "namespace", "propertyKey", "propertyValue"))
                    .execute();
        }
    }

    public void dropNamespace(String namespace)
    {
        try (Handle handle = Jdbi.open(connectionFactory)) {
            handle.createUpdate("" +
                            "DELETE FROM iceberg_namespace_properties " +
                            "WHERE catalog_name = :catalog AND namespace = :schema")
                    .bind("catalog", catalogId)
                    .bind("schema", namespace)
                    .execute();
        }
    }

    public List<SchemaTableName> getTables(List<String> namespaces)
    {
        try (Handle handle = Jdbi.open(connectionFactory)) {
            return handle.createQuery("" +
                    "SELECT table_namespace, table_name " +
                    "FROM iceberg_tables " +
                    "WHERE catalog_name = :catalog AND table_namespace IN (<namespaces>)")
                    .bind("catalog", catalogId)
                    .bindList("namespaces", namespaces)
                    .map((rs, ctx) -> new SchemaTableName(rs.getString("table_namespace"), rs.getString("table_name")))
                    .list();
        }
    }

    public void createTable(String schemaName, String tableName, String metadataLocation)
    {
        try (Handle handle = Jdbi.open(connectionFactory)) {
            handle.createUpdate("" +
                            "INSERT INTO iceberg_tables " +
                            "(catalog_name, table_namespace, table_name, metadata_location, previous_metadata_location) " +
                            "VALUES (:catalog, :schema, :table, :metadata_location, null)")
                    .bind("catalog", catalogId)
                    .bind("schema", schemaName)
                    .bind("table", tableName)
                    .bind("metadata_location", metadataLocation)
                    .execute();
        }
    }

    public void alterTable(String schemaName, String tableName, String newMetadataLocation, String previousMetadataLocation)
    {
        try (Handle handle = Jdbi.open(connectionFactory)) {
            handle.createUpdate("" +
                            "UPDATE iceberg_tables " +
                            "SET metadata_location = :metadata_location, previous_metadata_location = :previous_metadata_location " +
                            "WHERE catalog_name = :catalog AND table_namespace = :schema AND table_name = :table")
                    .bind("metadata_location", newMetadataLocation)
                    .bind("previous_metadata_location", previousMetadataLocation)
                    .bind("catalog", catalogId)
                    .bind("schema", schemaName)
                    .bind("table", tableName)
                    .execute();
        }
    }

    public Optional<String> getMetadataLocation(String schemaName, String tableName)
    {
        try (Handle handle = Jdbi.open(connectionFactory)) {
            return handle.createQuery("" +
                            "SELECT metadata_location " +
                            "FROM iceberg_tables " +
                            "WHERE catalog_name = :catalog AND table_namespace = :schema AND table_name = :table")
                    .bind("catalog", catalogId)
                    .bind("schema", schemaName)
                    .bind("table", tableName)
                    .mapTo(String.class)
                    .findOne();
        }
    }

    public void dropTable(String schemaName, String tableName)
    {
        try (Handle handle = Jdbi.open(connectionFactory)) {
            handle.createUpdate("" +
                            "DELETE FROM iceberg_tables " +
                            "WHERE catalog_name = :catalog AND table_namespace = :schema AND table_name = :table")
                    .bind("catalog", catalogId)
                    .bind("schema", schemaName)
                    .bind("table", tableName)
                    .execute();
        }
    }

    public void renameTable(SchemaTableName from, SchemaTableName to)
    {
        try (Handle handle = Jdbi.open(connectionFactory)) {
            handle.createUpdate("" +
                            "UPDATE iceberg_tables " +
                            "SET table_namespace = :new_schema, table_name = :new_table " +
                            "WHERE catalog_name = :catalog AND table_namespace = :schema AND table_name = :table")
                    .bind("catalog", catalogId)
                    .bind("new_schema", to.getSchemaName())
                    .bind("new_table", to.getTableName())
                    .bind("schema", from.getSchemaName())
                    .bind("table", from.getTableName())
                    .execute();
        }
    }

    // Requires public access modifier for JDBI library
    public static class NamespaceProperties
    {
        private final String catalogName;
        private final String namespace;
        private final String propertyKey;
        private final String propertyValue;

        public NamespaceProperties(String catalogName, String namespace, String propertyKey, String propertyValue)
        {
            this.catalogName = catalogName;
            this.namespace = namespace;
            this.propertyKey = propertyKey;
            this.propertyValue = propertyValue;
        }

        public String getCatalogName()
        {
            return catalogName;
        }

        public String getNamespace()
        {
            return namespace;
        }

        public String getPropertyKey()
        {
            return propertyKey;
        }

        public String getPropertyValue()
        {
            return propertyValue;
        }
    }
}
