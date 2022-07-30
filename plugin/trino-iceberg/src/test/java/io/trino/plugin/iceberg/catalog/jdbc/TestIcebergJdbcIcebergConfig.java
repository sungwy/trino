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

import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.util.Map;

import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;

public class TestIcebergJdbcIcebergConfig
{
    @Test
    public void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(JdbcIcebergConfig.class)
                .setConnectionUrl(null)
                .setConnectionUser(null)
                .setConnectionPassword(null)
                .setCatalogId(null)
                .setDefaultWarehouseDir(null));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("iceberg.metastore.jdbc.connection-url", "jdbc:postgresql://localhost:5432/test")
                .put("iceberg.metastore.jdbc.connection-user", "alice")
                .put("iceberg.metastore.jdbc.connection-password", "password")
                .put("iceberg.metastore.jdbc.catalogid", "test")
                .put("iceberg.metastore.jdbc.default-warehouse-dir", "s3://bucket")
                .buildOrThrow();

        JdbcIcebergConfig expected = new JdbcIcebergConfig()
                .setConnectionUrl("jdbc:postgresql://localhost:5432/test")
                .setConnectionUser("alice")
                .setConnectionPassword("password")
                .setCatalogId("test")
                .setDefaultWarehouseDir("s3://bucket");

        assertFullMapping(properties, expected);
    }
}
