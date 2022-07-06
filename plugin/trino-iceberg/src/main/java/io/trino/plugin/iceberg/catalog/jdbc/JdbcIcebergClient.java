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

import com.google.common.cache.Cache;
import com.google.common.cache.Weigher;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.trino.collect.cache.EvictableCacheBuilder;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.SchemaNotFoundException;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.TableNotFoundException;
import org.jdbi.v3.core.Handle;
import org.jdbi.v3.core.Jdbi;

import java.util.AbstractMap.SimpleEntry;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.Iterables.getOnlyElement;
import static io.trino.plugin.iceberg.IcebergErrorCode.ICEBERG_AMBIGUOUS_OBJECT_NAME;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MINUTES;

public class JdbcIcebergClient
{
    private final JdbcIcebergConnectionFactory connectionFactory;
    private final String catalogId;
    private final boolean caseInsensitiveNameMatching;
    private final Cache<String, Map<String, Optional<RemoteDatabaseObject>>> remoteSchemaToRemoteTableNamesCache;

    public JdbcIcebergClient(JdbcIcebergConnectionFactory connectionFactory, JdbcIcebergConfig config)
    {
        this.connectionFactory = requireNonNull(connectionFactory, "connectionFactory is null");
        this.catalogId = requireNonNull(config.getCatalogId(), "catalogId is null");
        this.caseInsensitiveNameMatching = config.isCaseInsensitiveNameMatching();
        this.remoteSchemaToRemoteTableNamesCache = EvictableCacheBuilder.newBuilder()
                .maximumWeight(100000)
                .weigher((Weigher<String, Map<String, Optional<RemoteDatabaseObject>>>) (key, value) -> value.size())
                .expireAfterWrite(10, MINUTES)
                .build();
    }

    public List<String> getRemoteNamespaces()
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

    public List<String> getNamespaces()
    {
        return getRemoteNamespaces().stream().map((namespace) -> namespace.toLowerCase(ENGLISH)).collect(Collectors.toList());
    }

    public Map<String, Object> getNamespaceProperties(String namespace)
    {
        String remoteSchemaName = toRemoteSchema(namespace).map(RemoteDatabaseObject::getOnlyRemoteName).orElseThrow(() -> new SchemaNotFoundException(namespace));
        try (Handle handle = Jdbi.open(connectionFactory)) {
            return handle.createQuery("" +
                            "SELECT property_key, property_value " +
                            "FROM iceberg_namespace_properties " +
                            "WHERE catalog_name = :catalog AND namespace = :schema")
                    .bind("catalog", catalogId)
                    .bind("schema", remoteSchemaName)
                    .map((rs, ctx) -> new SimpleEntry<>(rs.getString("property_key"), rs.getString("property_value")))
                    .collect(toImmutableMap(Map.Entry::getKey, Map.Entry::getValue));
        }
    }

    public Optional<String> getNamespaceLocation(String namespace)
    {
        String remoteSchemaName = toRemoteSchema(namespace).map(RemoteDatabaseObject::getOnlyRemoteName).orElseThrow(() -> new SchemaNotFoundException(namespace));
        try (Handle handle = Jdbi.open(connectionFactory)) {
            return handle.createQuery("" +
                            "SELECT property_value " +
                            "FROM iceberg_namespace_properties " +
                            "WHERE catalog_name = :catalog AND namespace = :schema AND property_key = 'location'")
                    .bind("catalog", catalogId)
                    .bind("schema", remoteSchemaName)
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
        String remoteSchemaName = toRemoteSchema(namespace).map(RemoteDatabaseObject::getOnlyRemoteName).orElseThrow(() -> new SchemaNotFoundException(namespace));
        try (Handle handle = Jdbi.open(connectionFactory)) {
            handle.createUpdate("" +
                            "DELETE FROM iceberg_namespace_properties " +
                            "WHERE catalog_name = :catalog AND namespace = :schema")
                    .bind("catalog", catalogId)
                    .bind("schema", remoteSchemaName)
                    .execute();
        }
    }

    public List<String> getRemoteTables(String namespace)
    {
        String remoteSchemaName = toRemoteSchema(namespace).map(RemoteDatabaseObject::getOnlyRemoteName).orElseThrow(() -> new SchemaNotFoundException(namespace));
        try (Handle handle = Jdbi.open(connectionFactory)) {
            return handle.createQuery("" +
                            "SELECT table_name " +
                            "FROM iceberg_tables " +
                            "WHERE catalog_name = :catalog AND table_namespace = :namespace")
                    .bind("catalog", catalogId)
                    .bind("namespace", remoteSchemaName)
                    .mapTo(String.class)
                    .list();
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
        String remoteSchemaName = toRemoteSchema(schemaName).map(RemoteDatabaseObject::getOnlyRemoteName).orElseThrow(() -> new SchemaNotFoundException(schemaName));
        String remoteTableName = toRemoteTable(schemaName, tableName).map(RemoteDatabaseObject::getOnlyRemoteName).orElseThrow(() -> new TableNotFoundException(new SchemaTableName(schemaName, tableName)));
        try (Handle handle = Jdbi.open(connectionFactory)) {
            handle.createUpdate("" +
                            "UPDATE iceberg_tables " +
                            "SET metadata_location = :metadata_location, previous_metadata_location = :previous_metadata_location " +
                            "WHERE catalog_name = :catalog AND table_namespace = :schema AND table_name = :table")
                    .bind("metadata_location", newMetadataLocation)
                    .bind("previous_metadata_location", previousMetadataLocation)
                    .bind("catalog", catalogId)
                    .bind("schema", remoteSchemaName)
                    .bind("table", remoteTableName)
                    .execute();
        }
    }

    public Optional<String> getMetadataLocation(String schemaName, String tableName)
    {
        String remoteSchemaName = toRemoteSchema(schemaName).map(RemoteDatabaseObject::getOnlyRemoteName).orElseThrow(() -> new SchemaNotFoundException(schemaName));
        String remoteTableName = toRemoteTable(schemaName, tableName).map(RemoteDatabaseObject::getOnlyRemoteName).orElseThrow(() -> new TableNotFoundException(new SchemaTableName(schemaName, tableName)));
        try (Handle handle = Jdbi.open(connectionFactory)) {
            return handle.createQuery("" +
                            "SELECT metadata_location " +
                            "FROM iceberg_tables " +
                            "WHERE catalog_name = :catalog AND table_namespace = :schema AND table_name = :table")
                    .bind("catalog", catalogId)
                    .bind("schema", remoteSchemaName)
                    .bind("table", remoteTableName)
                    .mapTo(String.class)
                    .findOne();
        }
    }

    public void dropTable(String schemaName, String tableName)
    {
        String remoteSchemaName = toRemoteSchema(schemaName).map(RemoteDatabaseObject::getOnlyRemoteName).orElseThrow(() -> new SchemaNotFoundException(schemaName));
        String remoteTableName = toRemoteTable(schemaName, tableName).map(RemoteDatabaseObject::getOnlyRemoteName).orElseThrow(() -> new TableNotFoundException(new SchemaTableName(schemaName, tableName)));
        try (Handle handle = Jdbi.open(connectionFactory)) {
            handle.createUpdate("" +
                            "DELETE FROM iceberg_tables " +
                            "WHERE catalog_name = :catalog AND table_namespace = :schema AND table_name = :table")
                    .bind("catalog", catalogId)
                    .bind("schema", remoteSchemaName)
                    .bind("table", remoteTableName)
                    .execute();
        }
        this.remoteSchemaToRemoteTableNamesCache.invalidate(remoteSchemaName);
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

    public Map<String, Optional<RemoteDatabaseObject>> createRemoteDatabaseObjectsMapping(List<String> remoteNames)
    {
        Map<String, Optional<RemoteDatabaseObject>> mapping = new HashMap<>();
        for (String remoteName : remoteNames) {
            mapping.merge(
                    remoteName.toLowerCase(ENGLISH),
                    Optional.of(RemoteDatabaseObject.of(remoteName)),
                    (currentValue, collision) -> currentValue.map(current -> current.registerCollision(collision.get().getOnlyRemoteName())));
        }

        return mapping;
    }

    public Optional<RemoteDatabaseObject> toRemoteSchema(String schemaName)
    {
        requireNonNull(schemaName, "schemaName is null");
        verify(schemaName.codePoints().noneMatch(Character::isUpperCase), "Expected schema name from internal metadata to be lowercase: %s", schemaName);
        if (!caseInsensitiveNameMatching) {
            return Optional.of(RemoteDatabaseObject.of(schemaName));
        }

        Map<String, Optional<RemoteDatabaseObject>> remoteSchemaMapping = createRemoteDatabaseObjectsMapping(getRemoteNamespaces());

        return remoteSchemaMapping.getOrDefault(schemaName, Optional.empty());
    }

    public Optional<RemoteDatabaseObject> toRemoteTable(String schemaName, String tableName)
    {
        requireNonNull(schemaName, "schemaName is null");
        requireNonNull(tableName, "tableName is null");
        verify(tableName.codePoints().noneMatch(Character::isUpperCase), "Expected table name from internal metadata to be lowercase: %s", tableName);
        verify(schemaName.codePoints().noneMatch(Character::isUpperCase), "Expected schema name from internal metadata to be lowercase: %s", schemaName);
        if (!caseInsensitiveNameMatching) {
            return Optional.of(RemoteDatabaseObject.of(tableName));
        }
        String remoteSchemaName = toRemoteSchema(schemaName).map(RemoteDatabaseObject::getOnlyRemoteName).orElseThrow(() -> new SchemaNotFoundException(schemaName));
        Map<String, Optional<RemoteDatabaseObject>> remoteTableMapping = new HashMap<>();
        try {
            remoteTableMapping = this.remoteSchemaToRemoteTableNamesCache.get(tableName, () -> createRemoteDatabaseObjectsMapping(getRemoteTables(remoteSchemaName)));
        }
        catch (ExecutionException e) {
            throw new RuntimeException(e);
        }
        return remoteTableMapping.getOrDefault(tableName, Optional.empty());
    }

    public static final class RemoteDatabaseObject
    {
        private final Set<String> remoteNames;

        private RemoteDatabaseObject(Set<String> remoteNames)
        {
            this.remoteNames = ImmutableSet.copyOf(remoteNames);
        }

        public static RemoteDatabaseObject of(String remoteName)
        {
            return new RemoteDatabaseObject(ImmutableSet.of(remoteName));
        }

        public RemoteDatabaseObject registerCollision(String ambiguousName)
        {
            return new RemoteDatabaseObject(ImmutableSet.<String>builderWithExpectedSize(remoteNames.size() + 1)
                    .addAll(remoteNames)
                    .add(ambiguousName)
                    .build());
        }

        public String getAnyRemoteName()
        {
            return Collections.min(remoteNames);
        }

        public String getOnlyRemoteName()
        {
            if (!isAmbiguous()) {
                return getOnlyElement(remoteNames);
            }

            throw new TrinoException(ICEBERG_AMBIGUOUS_OBJECT_NAME, "Found ambiguous names in Jdbc connected database when looking up '" + getAnyRemoteName().toLowerCase(ENGLISH) + "': " + remoteNames);
        }

        public boolean isAmbiguous()
        {
            return remoteNames.size() > 1;
        }
    }
}
