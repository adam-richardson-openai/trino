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
package io.trino.plugin.deltalake;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.inject.AbstractModule;
import com.google.inject.Injector;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import io.airlift.bootstrap.Bootstrap;
import io.airlift.json.JsonModule;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.filesystem.cache.CachingHostAddressProvider;
import io.trino.filesystem.cache.DefaultCachingHostAddressProvider;
import io.trino.filesystem.hdfs.HdfsFileSystemFactory;
import io.trino.hdfs.HdfsEnvironment;
import io.trino.hdfs.TrinoHdfsFileSystemStats;
import io.trino.metastore.Database;
import io.trino.metastore.HiveMetastoreFactory;
import io.trino.metastore.RawHiveMetastoreFactory;
import io.trino.plugin.base.ConnectorContextModule;
import io.trino.plugin.deltalake.metastore.DeltaLakeMetastore;
import io.trino.plugin.deltalake.metastore.DeltaLakeMetastoreModule;
import io.trino.plugin.deltalake.metastore.HiveMetastoreBackedDeltaLakeMetastore;
import io.trino.plugin.deltalake.transactionlog.MetadataEntry;
import io.trino.plugin.deltalake.transactionlog.ProtocolEntry;
import io.trino.plugin.deltalake.transactionlog.TransactionLogAccess;
import io.trino.plugin.hive.parquet.ParquetReaderConfig;
import io.trino.plugin.hive.parquet.ParquetWriterConfig;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.Assignment;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTableLayout;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.ProjectionApplicationResult;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.expression.ConnectorExpression;
import io.trino.spi.expression.Constant;
import io.trino.spi.expression.FieldDereference;
import io.trino.spi.expression.Variable;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.BooleanType;
import io.trino.spi.type.DateType;
import io.trino.spi.type.DoubleType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;
import io.trino.testing.TestingConnectorContext;
import io.trino.testing.TestingConnectorSession;
import io.trino.tests.BogusType;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;
import java.util.UUID;

import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.io.MoreFiles.deleteRecursively;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;
import static io.airlift.testing.Closeables.closeAll;
import static io.trino.plugin.deltalake.DeltaLakeColumnType.REGULAR;
import static io.trino.plugin.deltalake.DeltaLakeErrorCode.DELTA_LAKE_INVALID_SCHEMA;
import static io.trino.plugin.deltalake.DeltaLakeTableProperties.CHANGE_DATA_FEED_ENABLED_PROPERTY;
import static io.trino.plugin.deltalake.DeltaLakeTableProperties.COLUMN_MAPPING_MODE_PROPERTY;
import static io.trino.plugin.deltalake.DeltaLakeTableProperties.LOCATION_PROPERTY;
import static io.trino.plugin.deltalake.DeltaLakeTableProperties.PARTITIONED_BY_PROPERTY;
import static io.trino.plugin.deltalake.DeltaTestingConnectorSession.SESSION;
import static io.trino.plugin.deltalake.transactionlog.MetadataEntry.DELTA_CHANGE_DATA_FEED_ENABLED_PROPERTY;
import static io.trino.plugin.hive.HiveTestUtils.HDFS_ENVIRONMENT;
import static io.trino.plugin.hive.HiveTestUtils.HDFS_FILE_SYSTEM_STATS;
import static io.trino.spi.connector.SaveMode.FAIL;
import static io.trino.spi.security.PrincipalType.USER;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.TimestampType.TIMESTAMP_MILLIS;
import static java.nio.file.Files.createTempDirectory;
import static java.util.Locale.ENGLISH;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT;

@TestInstance(PER_CLASS)
@Execution(CONCURRENT)
public class TestDeltaLakeMetadata
{
    private static final String DATABASE_NAME = "mock_database";

    private static final ColumnMetadata BIGINT_COLUMN_1 = new ColumnMetadata("bigint_column1", BIGINT);
    private static final ColumnMetadata BIGINT_COLUMN_2 = new ColumnMetadata("bigint_column2", BIGINT);
    private static final ColumnMetadata TIMESTAMP_COLUMN = new ColumnMetadata("timestamp_column", TIMESTAMP_MILLIS);
    private static final ColumnMetadata MISSING_COLUMN = new ColumnMetadata("missing_column", BIGINT);

    private static final RowType BOGUS_ROW_FIELD = RowType.from(ImmutableList.of(
            new RowType.Field(Optional.of("test_field"), BogusType.BOGUS)));
    private static final RowType NESTED_ROW_FIELD = RowType.from(ImmutableList.of(
            new RowType.Field(Optional.of("child1"), INTEGER),
            new RowType.Field(Optional.of("child2"), INTEGER)));

    private static final DeltaLakeColumnHandle BOOLEAN_COLUMN_HANDLE =
            new DeltaLakeColumnHandle("boolean_column_name", BooleanType.BOOLEAN, OptionalInt.empty(), "boolean_column_name", BooleanType.BOOLEAN, REGULAR, Optional.empty());
    private static final DeltaLakeColumnHandle DOUBLE_COLUMN_HANDLE =
            new DeltaLakeColumnHandle("double_column_name", DoubleType.DOUBLE, OptionalInt.empty(), "double_column_name", DoubleType.DOUBLE, REGULAR, Optional.empty());
    private static final DeltaLakeColumnHandle BOGUS_COLUMN_HANDLE =
            new DeltaLakeColumnHandle("bogus_column_name", BogusType.BOGUS, OptionalInt.empty(), "bogus_column_name", BogusType.BOGUS, REGULAR, Optional.empty());
    private static final DeltaLakeColumnHandle VARCHAR_COLUMN_HANDLE =
            new DeltaLakeColumnHandle("varchar_column_name", VarcharType.VARCHAR, OptionalInt.empty(), "varchar_column_name", VarcharType.VARCHAR, REGULAR, Optional.empty());
    private static final DeltaLakeColumnHandle DATE_COLUMN_HANDLE =
            new DeltaLakeColumnHandle("date_column_name", DateType.DATE, OptionalInt.empty(), "date_column_name", DateType.DATE, REGULAR, Optional.empty());
    private static final DeltaLakeColumnHandle NESTED_COLUMN_HANDLE =
            new DeltaLakeColumnHandle("nested_column_name", NESTED_ROW_FIELD, OptionalInt.empty(), "nested_column_name", NESTED_ROW_FIELD, REGULAR, Optional.empty());
    private static final DeltaLakeColumnHandle EXPECTED_NESTED_COLUMN_HANDLE =
            new DeltaLakeColumnHandle(
                    "nested_column_name",
                    NESTED_ROW_FIELD,
                    OptionalInt.empty(),
                    "nested_column_name",
                    NESTED_ROW_FIELD,
                    REGULAR,
                    Optional.of(new DeltaLakeColumnProjectionInfo(INTEGER, ImmutableList.of(1), ImmutableList.of("child2"))));

    private static final Map<String, ColumnHandle> SYNTHETIC_COLUMN_ASSIGNMENTS = ImmutableMap.of(
            "test_synthetic_column_name_1", BOGUS_COLUMN_HANDLE,
            "test_synthetic_column_name_2", VARCHAR_COLUMN_HANDLE);
    private static final Map<String, ColumnHandle> NESTED_COLUMN_ASSIGNMENTS = ImmutableMap.of("nested_column_name", NESTED_COLUMN_HANDLE);
    private static final Map<String, ColumnHandle> EXPECTED_NESTED_COLUMN_ASSIGNMENTS = ImmutableMap.of("nested_column_name#child2", EXPECTED_NESTED_COLUMN_HANDLE);

    private static final ConnectorExpression DOUBLE_PROJECTION = new Variable("double_projection", DoubleType.DOUBLE);
    private static final ConnectorExpression BOOLEAN_PROJECTION = new Variable("boolean_projection", BooleanType.BOOLEAN);
    private static final ConnectorExpression DEREFERENCE_PROJECTION = new FieldDereference(
            BOGUS_ROW_FIELD,
            new Constant(1, BOGUS_ROW_FIELD),
            0);
    private static final ConnectorExpression NESTED_DEREFERENCE_PROJECTION = new FieldDereference(
            INTEGER,
            new Variable("nested_column_name", NESTED_ROW_FIELD),
            1);
    private static final ConnectorExpression EXPECTED_NESTED_DEREFERENCE_PROJECTION = new Variable(
            "nested_column_name#child2",
            INTEGER);

    private static final List<ConnectorExpression> SIMPLE_COLUMN_PROJECTIONS =
            ImmutableList.of(DOUBLE_PROJECTION, BOOLEAN_PROJECTION);
    private static final List<ConnectorExpression> DEREFERENCE_COLUMN_PROJECTIONS =
            ImmutableList.of(DOUBLE_PROJECTION, DEREFERENCE_PROJECTION, BOOLEAN_PROJECTION);
    private static final List<ConnectorExpression> NESTED_DEREFERENCE_COLUMN_PROJECTIONS =
            ImmutableList.of(NESTED_DEREFERENCE_PROJECTION);
    private static final List<ConnectorExpression> EXPECTED_NESTED_DEREFERENCE_COLUMN_PROJECTIONS =
            ImmutableList.of(EXPECTED_NESTED_DEREFERENCE_PROJECTION);

    private static final Set<DeltaLakeColumnHandle> PREDICATE_COLUMNS =
            ImmutableSet.of(BOOLEAN_COLUMN_HANDLE, DOUBLE_COLUMN_HANDLE);

    private File temporaryCatalogDirectory;
    private DeltaLakeMetadataFactory deltaLakeMetadataFactory;
    private TransactionLogAccess transactionLogAccess;

    @BeforeAll
    public void setUp()
            throws IOException
    {
        temporaryCatalogDirectory = createTempDirectory("HiveCatalog").toFile();
        Map<String, String> config = ImmutableMap.<String, String>builder()
                .put("hive.metastore", "file")
                .put("hive.metastore.catalog.dir", temporaryCatalogDirectory.getPath())
                .buildOrThrow();

        TestingConnectorContext context = new TestingConnectorContext();
        Bootstrap app = new Bootstrap(
                // connector dependencies
                new JsonModule(),
                new ConnectorContextModule("test", context),
                // connector modules
                new DeltaLakeSecurityModule(),
                new DeltaLakeMetastoreModule(),
                new DeltaLakeModule(),
                new TestingDeltaLakeExtensionsModule(),
                // test setup
                binder -> {
                    binder.bind(HdfsEnvironment.class).toInstance(HDFS_ENVIRONMENT);
                    binder.bind(TrinoHdfsFileSystemStats.class).toInstance(HDFS_FILE_SYSTEM_STATS);
                    binder.bind(TrinoFileSystemFactory.class).to(HdfsFileSystemFactory.class).in(Scopes.SINGLETON);
                    binder.bind(CachingHostAddressProvider.class).to(DefaultCachingHostAddressProvider.class).in(Scopes.SINGLETON);
                },
                new AbstractModule()
                {
                    @Provides
                    public DeltaLakeMetastore getDeltaLakeMetastore(@RawHiveMetastoreFactory HiveMetastoreFactory hiveMetastoreFactory)
                    {
                        return new HiveMetastoreBackedDeltaLakeMetastore(hiveMetastoreFactory.createMetastore(Optional.empty()));
                    }
                });

        Injector injector = app
                .doNotInitializeLogging()
                .setRequiredConfigurationProperties(config)
                .initialize();

        deltaLakeMetadataFactory = injector.getInstance(DeltaLakeMetadataFactory.class);
        transactionLogAccess = injector.getInstance(TransactionLogAccess.class);

        injector.getInstance(DeltaLakeMetastore.class)
                .createDatabase(Database.builder()
                        .setDatabaseName(DATABASE_NAME)
                        .setOwnerName(Optional.of("test"))
                        .setOwnerType(Optional.of(USER))
                        .setLocation(Optional.empty())
                        .build());
    }

    @AfterAll
    public void tearDown()
            throws Exception
    {
        closeAll(() -> deleteRecursively(temporaryCatalogDirectory.toPath(), ALLOW_INSECURE));
        temporaryCatalogDirectory = null;
    }

    @Test
    public void testGetNewTableLayout()
    {
        DeltaLakeMetadata deltaLakeMetadata = deltaLakeMetadataFactory.create(SESSION.getIdentity());
        Optional<ConnectorTableLayout> newTableLayout = deltaLakeMetadata.getNewTableLayout(
                SESSION,
                newTableMetadata(
                        ImmutableList.of(BIGINT_COLUMN_1, BIGINT_COLUMN_2),
                        ImmutableList.of(BIGINT_COLUMN_2)));

        assertThat(newTableLayout).isPresent();

        // should not have ConnectorPartitioningHandle since DeltaLake does not support bucketing
        assertThat(newTableLayout.get().getPartitioning()).isNotPresent();

        assertThat(newTableLayout.get().getPartitionColumns())
                .isEqualTo(ImmutableList.of(BIGINT_COLUMN_2.getName()));

        deltaLakeMetadata.cleanupQuery(SESSION);
    }

    @Test
    public void testGetNewTableLayoutNoPartitionColumns()
    {
        DeltaLakeMetadata deltaLakeMetadata = deltaLakeMetadataFactory.create(SESSION.getIdentity());
        assertThat(deltaLakeMetadata.getNewTableLayout(
                SESSION,
                newTableMetadata(
                        ImmutableList.of(BIGINT_COLUMN_1, BIGINT_COLUMN_2),
                        ImmutableList.of())))
                .isNotPresent();

        deltaLakeMetadata.cleanupQuery(SESSION);
    }

    @Test
    public void testGetNewTableLayoutInvalidPartitionColumns()
    {
        DeltaLakeMetadata deltaLakeMetadata = deltaLakeMetadataFactory.create(SESSION.getIdentity());
        assertThatThrownBy(() -> deltaLakeMetadata.getNewTableLayout(
                SESSION,
                newTableMetadata(
                        ImmutableList.of(BIGINT_COLUMN_1, BIGINT_COLUMN_2),
                        ImmutableList.of(BIGINT_COLUMN_2, MISSING_COLUMN))))
                .isInstanceOf(TrinoException.class)
                .hasMessage("Table property 'partitioned_by' contained column names which do not exist: [missing_column]");

        assertThatThrownBy(() -> deltaLakeMetadata.getNewTableLayout(
                SESSION,
                newTableMetadata(
                        ImmutableList.of(TIMESTAMP_COLUMN, BIGINT_COLUMN_2),
                        ImmutableList.of(BIGINT_COLUMN_2))))
                .isInstanceOf(TrinoException.class)
                .hasMessage("Unsupported type: timestamp(3)");

        deltaLakeMetadata.cleanupQuery(SESSION);
    }

    @Test
    public void testGetInsertLayout()
    {
        DeltaLakeMetadata deltaLakeMetadata = deltaLakeMetadataFactory.create(SESSION.getIdentity());

        ConnectorTableMetadata tableMetadata = newTableMetadata(
                ImmutableList.of(BIGINT_COLUMN_1, BIGINT_COLUMN_2),
                ImmutableList.of(BIGINT_COLUMN_1));

        deltaLakeMetadata.createTable(SESSION, tableMetadata, FAIL);

        Optional<ConnectorTableLayout> insertLayout = deltaLakeMetadata
                .getInsertLayout(
                        SESSION,
                        deltaLakeMetadata.getTableHandle(SESSION, tableMetadata.getTable(), Optional.empty(), Optional.empty()));

        assertThat(insertLayout).isPresent();

        assertThat(insertLayout.get().getPartitioning()).isNotPresent();

        assertThat(insertLayout.get().getPartitionColumns())
                .isEqualTo(getPartitionColumnNames(ImmutableList.of(BIGINT_COLUMN_1)));

        deltaLakeMetadata.cleanupQuery(SESSION);
    }

    private ConnectorTableMetadata newTableMetadata(List<ColumnMetadata> tableColumns, List<ColumnMetadata> partitionTableColumns)
    {
        return new ConnectorTableMetadata(
                newMockSchemaTableName(),
                tableColumns,
                ImmutableMap.of(
                        PARTITIONED_BY_PROPERTY,
                        getPartitionColumnNames(partitionTableColumns),
                        COLUMN_MAPPING_MODE_PROPERTY,
                        "none"));
    }

    @Test
    public void testGetInsertLayoutTableUnpartitioned()
    {
        DeltaLakeMetadata deltaLakeMetadata = deltaLakeMetadataFactory.create(SESSION.getIdentity());

        ConnectorTableMetadata tableMetadata = newTableMetadata(
                ImmutableList.of(BIGINT_COLUMN_1),
                ImmutableList.of());

        deltaLakeMetadata.createTable(SESSION, tableMetadata, FAIL);

        // should return empty insert layout since table exists but is unpartitioned
        assertThat(deltaLakeMetadata.getInsertLayout(
                SESSION,
                deltaLakeMetadata.getTableHandle(SESSION, tableMetadata.getTable(), Optional.empty(), Optional.empty())))
                .isNotPresent();

        deltaLakeMetadata.cleanupQuery(SESSION);
    }

    @Test
    public void testApplyProjection()
    {
        testApplyProjection(
                ImmutableSet.of(),
                SYNTHETIC_COLUMN_ASSIGNMENTS,
                SIMPLE_COLUMN_PROJECTIONS,
                SIMPLE_COLUMN_PROJECTIONS,
                ImmutableSet.of(BOGUS_COLUMN_HANDLE, VARCHAR_COLUMN_HANDLE),
                SYNTHETIC_COLUMN_ASSIGNMENTS);
        testApplyProjection(
                // table handle already contains subset of expected projected columns
                ImmutableSet.of(BOGUS_COLUMN_HANDLE),
                SYNTHETIC_COLUMN_ASSIGNMENTS,
                SIMPLE_COLUMN_PROJECTIONS,
                SIMPLE_COLUMN_PROJECTIONS,
                ImmutableSet.of(BOGUS_COLUMN_HANDLE, VARCHAR_COLUMN_HANDLE),
                SYNTHETIC_COLUMN_ASSIGNMENTS);
        testApplyProjection(
                // table handle already contains superset of expected projected columns
                ImmutableSet.of(DOUBLE_COLUMN_HANDLE, BOOLEAN_COLUMN_HANDLE, DATE_COLUMN_HANDLE, BOGUS_COLUMN_HANDLE, VARCHAR_COLUMN_HANDLE),
                SYNTHETIC_COLUMN_ASSIGNMENTS,
                SIMPLE_COLUMN_PROJECTIONS,
                SIMPLE_COLUMN_PROJECTIONS,
                ImmutableSet.of(BOGUS_COLUMN_HANDLE, VARCHAR_COLUMN_HANDLE),
                SYNTHETIC_COLUMN_ASSIGNMENTS);
        testApplyProjection(
                // table handle has empty assignments
                ImmutableSet.of(DOUBLE_COLUMN_HANDLE, BOOLEAN_COLUMN_HANDLE, DATE_COLUMN_HANDLE, BOGUS_COLUMN_HANDLE, VARCHAR_COLUMN_HANDLE),
                ImmutableMap.of(),
                SIMPLE_COLUMN_PROJECTIONS,
                SIMPLE_COLUMN_PROJECTIONS,
                ImmutableSet.of(),
                ImmutableMap.of());
        testApplyProjection(
                ImmutableSet.of(DOUBLE_COLUMN_HANDLE, BOOLEAN_COLUMN_HANDLE, DATE_COLUMN_HANDLE, BOGUS_COLUMN_HANDLE, VARCHAR_COLUMN_HANDLE),
                ImmutableMap.of(),
                DEREFERENCE_COLUMN_PROJECTIONS,
                DEREFERENCE_COLUMN_PROJECTIONS,
                ImmutableSet.of(),
                ImmutableMap.of());
        testApplyProjection(
                ImmutableSet.of(NESTED_COLUMN_HANDLE),
                NESTED_COLUMN_ASSIGNMENTS,
                NESTED_DEREFERENCE_COLUMN_PROJECTIONS,
                EXPECTED_NESTED_DEREFERENCE_COLUMN_PROJECTIONS,
                ImmutableSet.of(EXPECTED_NESTED_COLUMN_HANDLE),
                EXPECTED_NESTED_COLUMN_ASSIGNMENTS);
    }

    private void testApplyProjection(
            Set<DeltaLakeColumnHandle> inputProjectedColumns,
            Map<String, ColumnHandle> inputAssignments,
            List<ConnectorExpression> inputProjections,
            List<ConnectorExpression> expectedProjections,
            Set<DeltaLakeColumnHandle> expectedProjectedColumns,
            Map<String, ColumnHandle> expectedAssignments)
    {
        DeltaLakeMetadata deltaLakeMetadata = deltaLakeMetadataFactory.create(SESSION.getIdentity());

        ProjectionApplicationResult<ConnectorTableHandle> projection = deltaLakeMetadata
                .applyProjection(
                        SESSION,
                        createDeltaLakeTableHandle(inputProjectedColumns, PREDICATE_COLUMNS),
                        inputProjections,
                        inputAssignments)
                .get();

        assertThat(((DeltaLakeTableHandle) projection.getHandle()).getProjectedColumns())
                .isEqualTo(Optional.of(expectedProjectedColumns));

        assertThat(projection.getProjections())
                .usingRecursiveComparison()
                .isEqualTo(expectedProjections);

        assertThat(projection.getAssignments())
                .usingRecursiveComparison()
                .isEqualTo(createNewColumnAssignments(expectedAssignments));

        assertThat(projection.isPrecalculateStatistics())
                .isFalse();

        deltaLakeMetadata.cleanupQuery(SESSION);
    }

    @Test
    public void testApplyProjectionWithEmptyResult()
    {
        DeltaLakeMetadata deltaLakeMetadata = deltaLakeMetadataFactory.create(SESSION.getIdentity());

        assertThat(deltaLakeMetadata
                .applyProjection(
                        SESSION,
                        createDeltaLakeTableHandle(
                                ImmutableSet.of(BOGUS_COLUMN_HANDLE, VARCHAR_COLUMN_HANDLE),
                                PREDICATE_COLUMNS),
                        SIMPLE_COLUMN_PROJECTIONS,
                        SYNTHETIC_COLUMN_ASSIGNMENTS))
                .isEmpty();

        assertThat(deltaLakeMetadata
                .applyProjection(
                        SESSION,
                        createDeltaLakeTableHandle(ImmutableSet.of(), ImmutableSet.of()),
                        ImmutableList.of(),
                        ImmutableMap.of()))
                .isEmpty();

        deltaLakeMetadata.cleanupQuery(SESSION);
    }

    @Test
    public void testGetInputInfoForPartitionedTable()
    {
        DeltaLakeMetadata deltaLakeMetadata = deltaLakeMetadataFactory.create(SESSION.getIdentity());
        ConnectorTableMetadata tableMetadata = newTableMetadata(
                ImmutableList.of(BIGINT_COLUMN_1, BIGINT_COLUMN_2),
                ImmutableList.of(BIGINT_COLUMN_1));
        deltaLakeMetadata.createTable(SESSION, tableMetadata, FAIL);
        DeltaLakeTableHandle tableHandle = (DeltaLakeTableHandle) deltaLakeMetadata.getTableHandle(SESSION, tableMetadata.getTable(), Optional.empty(), Optional.empty());
        assertThat(deltaLakeMetadata.getInfo(SESSION, tableHandle)).isEqualTo(Optional.of(new DeltaLakeInputInfo(true, 0)));
        deltaLakeMetadata.cleanupQuery(SESSION);
    }

    @Test
    public void testGetInputInfoForUnPartitionedTable()
    {
        DeltaLakeMetadata deltaLakeMetadata = deltaLakeMetadataFactory.create(SESSION.getIdentity());
        ConnectorTableMetadata tableMetadata = newTableMetadata(
                ImmutableList.of(BIGINT_COLUMN_1, BIGINT_COLUMN_2),
                ImmutableList.of());
        deltaLakeMetadata.createTable(SESSION, tableMetadata, FAIL);
        DeltaLakeTableHandle tableHandle = (DeltaLakeTableHandle) deltaLakeMetadata.getTableHandle(SESSION, tableMetadata.getTable(), Optional.empty(), Optional.empty());
        assertThat(deltaLakeMetadata.getInfo(SESSION, tableHandle)).isEqualTo(Optional.of(new DeltaLakeInputInfo(false, 0)));
        deltaLakeMetadata.cleanupQuery(SESSION);
    }

    @Test
    public void testGetTableHandleUsesSyntheticChecksumMetadataWhenPresent()
            throws Exception
    {
        ConnectorSession loadMetadataFromChecksumFileEnabledSession = loadMetadataFromChecksumFileSession(true);
        DeltaLakeMetadata deltaLakeMetadata = deltaLakeMetadataFactory.create(loadMetadataFromChecksumFileEnabledSession.getIdentity());
        ConnectorTableMetadata tableMetadata = newTableMetadata(
                ImmutableList.of(BIGINT_COLUMN_1, BIGINT_COLUMN_2),
                ImmutableList.of());
        deltaLakeMetadata.createTable(loadMetadataFromChecksumFileEnabledSession, tableMetadata, FAIL);
        DeltaLakeTableHandle initialHandle = (DeltaLakeTableHandle) deltaLakeMetadata.getTableHandle(loadMetadataFromChecksumFileEnabledSession, tableMetadata.getTable(), Optional.empty(), Optional.empty());
        String tableLocation = initialHandle.getLocation();
        deltaLakeMetadata.cleanupQuery(loadMetadataFromChecksumFileEnabledSession);

        String syntheticSchemaString = String.join("",
                "{\"type\":\"struct\",\"fields\":[",
                "{\"name\":\"bigint_column1\",\"type\":\"long\",\"nullable\":true,\"metadata\":{}},",
                "{\"name\":\"bigint_column2\",\"type\":\"long\",\"nullable\":true,\"metadata\":{}},",
                "{\"name\":\"checksum_only_column\",\"type\":\"long\",\"nullable\":true,\"metadata\":{}}",
                "]}");

        // At this time, Trino doens't actually support writing checksum files for Delta tables. Write a synethetic/dummy
        // checksum file to ensure that Delta checksum loading and parsing are exercised in tests. This can be removed once
        // Trino supports writing checksum files
        //
        // Additionally, include a dummy checksum_only_column in the checksum metadata. This is invalid per the Delta spec,
        // but it makes it easy to validate that we are loading the metadata from the checksum rather than the commit log
        writeChecksumFile(tableLocation, 0, """
                {
                  "metadata": {
                    "id": "synthetic-checksum-id",
                    "name": null,
                    "description": null,
                    "format": {
                      "provider": "parquet",
                      "options": {}
                    },
                    "schemaString": "%s",
                    "partitionColumns": [],
                    "configuration": {},
                    "createdTime": 0
                  },
                  "protocol": {
                    "minReaderVersion": 1,
                    "minWriterVersion": 2
                  }
                }
                """.formatted(syntheticSchemaString.replace("\"", "\\\"")));

        DeltaLakeMetadata checksumMetadata = deltaLakeMetadataFactory.create(loadMetadataFromChecksumFileEnabledSession.getIdentity());
        LocatedTableHandle checksumHandle = checksumMetadata.getTableHandle(loadMetadataFromChecksumFileEnabledSession, tableMetadata.getTable(), Optional.empty(), Optional.empty());
        assertThat(checksumHandle).isInstanceOf(DeltaLakeTableHandle.class);

        // Validate the presence of checksum_only_column
        assertThat(((DeltaLakeTableHandle) checksumHandle).getMetadataEntry().getSchemaString()).contains("checksum_only_column");
        checksumMetadata.cleanupQuery(loadMetadataFromChecksumFileEnabledSession);
    }

    @Test
    public void testGetTableHandleFallsBackWhenChecksumFileIsMissing()
            throws Exception
    {
        DeltaLakeMetadata deltaLakeMetadata = deltaLakeMetadataFactory.create(SESSION.getIdentity());
        ConnectorTableMetadata tableMetadata = newTableMetadata(
                ImmutableList.of(BIGINT_COLUMN_1, BIGINT_COLUMN_2),
                ImmutableList.of());
        deltaLakeMetadata.createTable(SESSION, tableMetadata, FAIL);
        DeltaLakeTableHandle initialHandle = (DeltaLakeTableHandle) deltaLakeMetadata.getTableHandle(SESSION, tableMetadata.getTable(), Optional.empty(), Optional.empty());
        String tableLocation = initialHandle.getLocation();
        deltaLakeMetadata.cleanupQuery(SESSION);

        Files.deleteIfExists(checksumFilePath(tableLocation, 0));

        DeltaLakeMetadata fallbackMetadata = deltaLakeMetadataFactory.create(SESSION.getIdentity());
        assertThat(fallbackMetadata.getTableHandle(SESSION, tableMetadata.getTable(), Optional.empty(), Optional.empty()))
                .isInstanceOf(DeltaLakeTableHandle.class);
        fallbackMetadata.cleanupQuery(SESSION);

        // Sanity check: corrupt the actual commit file and verify that loading now fails -- proving that we did load from
        // the Delta log rather than from a checksum
        Path commitFilePath = Path.of(tableLocation).resolve("_delta_log").resolve("%020d.json".formatted(0));
        Files.writeString(commitFilePath, "}");
        transactionLogAccess.invalidateCache(tableMetadata.getTable(), Optional.of(tableLocation));
        DeltaLakeMetadata corruptedLogMetadata = deltaLakeMetadataFactory.create(SESSION.getIdentity());
        LocatedTableHandle corruptedLogHandle = corruptedLogMetadata.getTableHandle(SESSION, tableMetadata.getTable(), Optional.empty(), Optional.empty());
        assertThat(corruptedLogHandle).isInstanceOf(CorruptedDeltaLakeTableHandle.class);
        assertThat(((CorruptedDeltaLakeTableHandle) corruptedLogHandle).originalException())
                .isInstanceOfSatisfying(TrinoException.class, exception ->
                        assertThat(exception.getErrorCode()).isEqualTo(DELTA_LAKE_INVALID_SCHEMA.toErrorCode()));
        corruptedLogMetadata.cleanupQuery(SESSION);
    }

    @Test
    public void testGetTableHandleDoesNotUseOldCheckpointFile()
            throws Exception
    {
        ConnectorSession loadMetadataFromChecksumFileEnabledSession = loadMetadataFromChecksumFileSession(true);
        DeltaLakeMetadata deltaLakeMetadata = deltaLakeMetadataFactory.create(loadMetadataFromChecksumFileEnabledSession.getIdentity());
        ConnectorTableMetadata tableMetadata = new ConnectorTableMetadata(
                newMockSchemaTableName(),
                ImmutableList.of(BIGINT_COLUMN_1, BIGINT_COLUMN_2),
                ImmutableMap.of(
                        LOCATION_PROPERTY,
                        // Note: need a file:// URI for setTableTableProperties
                        temporaryCatalogDirectory.toPath().resolve("table-" + UUID.randomUUID()).toUri().toString(),
                        PARTITIONED_BY_PROPERTY,
                        ImmutableList.of(),
                        COLUMN_MAPPING_MODE_PROPERTY,
                        "none"));
        deltaLakeMetadata.createTable(loadMetadataFromChecksumFileEnabledSession, tableMetadata, FAIL);
        DeltaLakeTableHandle initialHandle = (DeltaLakeTableHandle) deltaLakeMetadata.getTableHandle(loadMetadataFromChecksumFileEnabledSession, tableMetadata.getTable(), Optional.empty(), Optional.empty());
        String tableLocation = initialHandle.getLocation().replaceFirst("^file://", "");

        // At this time, Trino doens't actually support writing checksum files for Delta tables. Write a synethetic/dummy
        // checksum file
        writeChecksumFile(tableLocation, 0, """
                {
                  "metadata": {
                    "id": "synthetic-checksum-id",
                    "name": null,
                    "description": null,
                    "format": {
                      "provider": "parquet",
                      "options": {}
                    },
                    "schemaString": "%s",
                    "partitionColumns": [],
                    "configuration": {
                      "%s": "false"
                    },
                    "createdTime": 0
                  },
                  "protocol": {
                    "minReaderVersion": %d,
                    "minWriterVersion": %d
                  }
                }
                """.formatted(
                initialHandle.getMetadataEntry().getSchemaString().replace("\"", "\\\""),
                DELTA_CHANGE_DATA_FEED_ENABLED_PROPERTY,
                initialHandle.getProtocolEntry().minReaderVersion(),
                initialHandle.getProtocolEntry().minWriterVersion()));

        // Evolve metadata to version 1 using a supported write-path operation.
        deltaLakeMetadata.setTableProperties(
                loadMetadataFromChecksumFileEnabledSession,
                initialHandle,
                ImmutableMap.of(CHANGE_DATA_FEED_ENABLED_PROPERTY, Optional.of((Object) true)));
        deltaLakeMetadata.cleanupQuery(loadMetadataFromChecksumFileEnabledSession);

        // At this time, Trino does not write checksum files for Delta tables -- but for future-proofing, delete the
        // checksum file for version 1 if it exists.
        Files.deleteIfExists(checksumFilePath(tableLocation, 1));

        transactionLogAccess.invalidateCache(tableMetadata.getTable(), Optional.of(initialHandle.getLocation()));
        DeltaLakeMetadata reloadedMetadata = deltaLakeMetadataFactory.create(loadMetadataFromChecksumFileEnabledSession.getIdentity());
        LocatedTableHandle reloadedHandle = reloadedMetadata.getTableHandle(loadMetadataFromChecksumFileEnabledSession, tableMetadata.getTable(), Optional.empty(), Optional.empty());
        assertThat(reloadedHandle).isInstanceOf(DeltaLakeTableHandle.class);

        // The new property is visible despite the lack of a checksum file for version 1, proving that we loaded metadata
        // from the Delta log rather than from the checksum
        DeltaLakeTableHandle tableHandle = (DeltaLakeTableHandle) reloadedHandle;
        assertThat(tableHandle.getReadVersion()).isEqualTo(1);
        assertThat(tableHandle.getMetadataEntry().getConfiguration())
                .containsEntry(DELTA_CHANGE_DATA_FEED_ENABLED_PROPERTY, "true");
        reloadedMetadata.cleanupQuery(loadMetadataFromChecksumFileEnabledSession);
    }

    @Test
    public void testGetTableHandleReturnsCorruptedHandleForMalformedChecksumWhenEnabled()
            throws Exception
    {
        ConnectorSession loadMetadataFromChecksumFileEnabledSession = loadMetadataFromChecksumFileSession(true);
        DeltaLakeMetadata deltaLakeMetadata = deltaLakeMetadataFactory.create(loadMetadataFromChecksumFileEnabledSession.getIdentity());
        ConnectorTableMetadata tableMetadata = newTableMetadata(
                ImmutableList.of(BIGINT_COLUMN_1, BIGINT_COLUMN_2),
                ImmutableList.of());
        deltaLakeMetadata.createTable(loadMetadataFromChecksumFileEnabledSession, tableMetadata, FAIL);
        DeltaLakeTableHandle initialHandle = (DeltaLakeTableHandle) deltaLakeMetadata.getTableHandle(loadMetadataFromChecksumFileEnabledSession, tableMetadata.getTable(), Optional.empty(), Optional.empty());
        String tableLocation = initialHandle.getLocation();
        deltaLakeMetadata.cleanupQuery(loadMetadataFromChecksumFileEnabledSession);

        // Malformed checksum file
        writeChecksumFile(tableLocation, 0, "{");

        // With checksum metadata enabled, an invalid checksum file is treated as a hard error
        DeltaLakeMetadata checksumMetadata = deltaLakeMetadataFactory.create(loadMetadataFromChecksumFileEnabledSession.getIdentity());
        LocatedTableHandle checksumHandle = checksumMetadata.getTableHandle(loadMetadataFromChecksumFileEnabledSession, tableMetadata.getTable(), Optional.empty(), Optional.empty());
        assertThat(checksumHandle).isInstanceOf(CorruptedDeltaLakeTableHandle.class);
        assertThat(((CorruptedDeltaLakeTableHandle) checksumHandle).originalException())
                .isInstanceOfSatisfying(TrinoException.class, exception ->
                        assertThat(exception.getErrorCode()).isEqualTo(DELTA_LAKE_INVALID_SCHEMA.toErrorCode()));
        checksumMetadata.cleanupQuery(loadMetadataFromChecksumFileEnabledSession);

        // With checksum metadata disabled, the invalid checksum file goes unnoticed, and the table is loaded successfully
        // from the Delta log alone
        ConnectorSession loadMetadataFromChecksumFileDisabledSession = loadMetadataFromChecksumFileSession(false);
        DeltaLakeMetadata transactionLogMetadata = deltaLakeMetadataFactory.create(loadMetadataFromChecksumFileDisabledSession.getIdentity());
        assertThat(transactionLogMetadata.getTableHandle(loadMetadataFromChecksumFileDisabledSession, tableMetadata.getTable(), Optional.empty(), Optional.empty()))
                .isInstanceOf(DeltaLakeTableHandle.class);
        transactionLogMetadata.cleanupQuery(loadMetadataFromChecksumFileDisabledSession);
    }

    private static ConnectorSession loadMetadataFromChecksumFileSession(boolean enabled)
    {
        return TestingConnectorSession.builder()
                .setPropertyMetadata(new DeltaLakeSessionProperties(new DeltaLakeConfig(), new ParquetReaderConfig(), new ParquetWriterConfig()).getSessionProperties())
                .setPropertyValues(ImmutableMap.of("load_metadata_from_checksum_file", enabled))
                .build();
    }

    private static void writeChecksumFile(String tableLocation, long version, String contents)
            throws IOException
    {
        Files.writeString(checksumFilePath(tableLocation, version), contents);
    }

    private static Path checksumFilePath(String tableLocation, long version)
    {
        return Path.of(tableLocation).resolve("_delta_log").resolve("%020d.crc".formatted(version));
    }

    private static DeltaLakeTableHandle createDeltaLakeTableHandle(Set<DeltaLakeColumnHandle> projectedColumns, Set<DeltaLakeColumnHandle> constrainedColumns)
    {
        return new DeltaLakeTableHandle(
                "test_schema_name",
                "test_table_name",
                true,
                "test_location",
                createMetadataEntry(),
                new ProtocolEntry(1, 2, Optional.empty(), Optional.empty()),
                createConstrainedColumnsTuple(constrainedColumns),
                TupleDomain.all(),
                Optional.of(DeltaLakeTableHandle.WriteType.UPDATE),
                Optional.of(projectedColumns),
                Optional.of(ImmutableList.of(BOOLEAN_COLUMN_HANDLE)),
                Optional.of(ImmutableList.of(DOUBLE_COLUMN_HANDLE)),
                Optional.empty(),
                0,
                false);
    }

    private static TupleDomain<DeltaLakeColumnHandle> createConstrainedColumnsTuple(
            Set<DeltaLakeColumnHandle> constrainedColumns)
    {
        ImmutableMap.Builder<DeltaLakeColumnHandle, Domain> tupleBuilder = ImmutableMap.builder();

        constrainedColumns.forEach(column -> {
            verify(column.isBaseColumn(), "Unexpected dereference: %s", column);
            tupleBuilder.put(column, Domain.notNull(column.baseType()));
        });

        return TupleDomain.withColumnDomains(tupleBuilder.buildOrThrow());
    }

    private static List<Assignment> createNewColumnAssignments(Map<String, ColumnHandle> assignments)
    {
        return assignments.entrySet().stream()
                .map(assignment -> {
                    DeltaLakeColumnHandle column = ((DeltaLakeColumnHandle) assignment.getValue());
                    Type type = column.projectionInfo().map(DeltaLakeColumnProjectionInfo::type).orElse(column.baseType());
                    return new Assignment(
                            assignment.getKey(),
                            assignment.getValue(),
                            type);
                })
                .collect(toImmutableList());
    }

    private static MetadataEntry createMetadataEntry()
    {
        return new MetadataEntry(
                "test_id",
                "test_name",
                "test_description",
                new MetadataEntry.Format("test_provider", ImmutableMap.of()),
                "test_schema",
                ImmutableList.of("test_partition_column"),
                ImmutableMap.of("test_configuration_key", "test_configuration_value"),
                1);
    }

    private static List<String> getPartitionColumnNames(List<ColumnMetadata> tableMetadataColumns)
    {
        return tableMetadataColumns.stream()
                .map(ColumnMetadata::getName)
                .collect(toImmutableList());
    }

    private static SchemaTableName newMockSchemaTableName()
    {
        String randomSuffix = UUID.randomUUID().toString().toLowerCase(ENGLISH).replace("-", "");
        return new SchemaTableName(DATABASE_NAME, "table_" + randomSuffix);
    }
}
