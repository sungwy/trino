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

import com.google.inject.Binder;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.google.inject.Singleton;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.trino.plugin.iceberg.catalog.IcebergTableOperationsProvider;
import io.trino.plugin.iceberg.catalog.TrinoCatalogFactory;

import static io.airlift.configuration.ConfigBinder.configBinder;
import static org.weakref.jmx.guice.ExportBinder.newExporter;

public class IcebergJdbcCatalogModule
        extends AbstractConfigurationAwareModule
{
    @Override
    protected void setup(Binder binder)
    {
        configBinder(binder).bindConfig(JdbcIcebergConfig.class);
        binder.bind(TrinoJdbcCatalogFactory.class);
        binder.bind(IcebergTableOperationsProvider.class).to(JdbcIcebergTableOperationsProvider.class).in(Scopes.SINGLETON);
        newExporter(binder).export(IcebergTableOperationsProvider.class).withGeneratedName();
        binder.bind(TrinoCatalogFactory.class).to(TrinoJdbcCatalogFactory.class).in(Scopes.SINGLETON);
        newExporter(binder).export(TrinoJdbcCatalogFactory.class).withGeneratedName();
    }

    @Provides
    @Singleton
    public static JdbcIcebergClient createJdbcIcebergClient(JdbcIcebergConfig config)
    {
        return new JdbcIcebergClient(
                new JdbcIcebergConnectionFactory(
                        config.getConnectionUrl(),
                        config.getConnectionUser(),
                        config.getConnectionPassword()),
                config.getCatalogId());
    }
}
