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

package io.trino.plugin.deltalake.transactionlog;

import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.hdfs.HdfsFileSystemFactory;
import io.trino.spi.TrinoException;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Optional;

import static io.trino.plugin.deltalake.DeltaLakeErrorCode.DELTA_LAKE_INVALID_SCHEMA;
import static io.trino.plugin.deltalake.DeltaTestingConnectorSession.SESSION;
import static io.trino.plugin.deltalake.transactionlog.TransactionLogParser.getLatestCommitVersion;
import static io.trino.plugin.deltalake.transactionlog.TransactionLogParser.getMandatoryCurrentVersion;
import static io.trino.plugin.deltalake.transactionlog.TransactionLogParser.readPartitionTimestampWithZone;
import static io.trino.plugin.deltalake.transactionlog.TransactionLogParser.readVersionChecksumFile;
import static io.trino.plugin.hive.HiveTestUtils.HDFS_ENVIRONMENT;
import static io.trino.plugin.hive.HiveTestUtils.HDFS_FILE_SYSTEM_STATS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestTransactionLogParser
{
    @Test
    public void testGetCurrentVersion()
            throws Exception
    {
        TrinoFileSystem fileSystem = createFileSystem();

        String basePath = getClass().getClassLoader().getResource("databricks73").toURI().toString();

        assertThat(getMandatoryCurrentVersion(fileSystem, basePath + "/simple_table_without_checkpoint", 8)).isEqualTo(9);
        assertThat(getMandatoryCurrentVersion(fileSystem, basePath + "/simple_table_without_checkpoint", 9)).isEqualTo(9);
        assertThat(getMandatoryCurrentVersion(fileSystem, basePath + "/simple_table_ending_on_checkpoint", 10)).isEqualTo(10);
        assertThat(getMandatoryCurrentVersion(fileSystem, basePath + "/simple_table_past_checkpoint", 10)).isEqualTo(11);
        assertThat(getMandatoryCurrentVersion(fileSystem, basePath + "/simple_table_past_checkpoint", 11)).isEqualTo(11);
    }

    @Test
    void testReadPartitionTimestampWithZone()
    {
        assertThat(readPartitionTimestampWithZone("1970-01-01 00:00:00")).isEqualTo(0L);
        assertThat(readPartitionTimestampWithZone("1970-01-01 00:00:00.1")).isEqualTo(409600L);
        assertThat(readPartitionTimestampWithZone("1970-01-01 00:00:00.01")).isEqualTo(40960L);
        assertThat(readPartitionTimestampWithZone("1970-01-01 00:00:00.001")).isEqualTo(4096L);

        // https://github.com/trinodb/trino/issues/20359 Increase timestamp precision to microseconds
        assertThat(readPartitionTimestampWithZone("1970-01-01 00:00:00.0001")).isEqualTo(0L);
        assertThat(readPartitionTimestampWithZone("1970-01-01 00:00:00.00001")).isEqualTo(0L);
        assertThat(readPartitionTimestampWithZone("1970-01-01 00:00:00.000001")).isEqualTo(0L);
    }

    @Test
    void testReadPartitionTimestampWithZoneIso8601()
    {
        assertThat(readPartitionTimestampWithZone("1970-01-01T00:00:00.000000Z")).isEqualTo(0L);
        assertThat(readPartitionTimestampWithZone("1970-01-01T01:00:00.000000+01:00")).isEqualTo(0L);
    }

    @Test
    public void testGetLatestCommitVersion()
            throws Exception
    {
        TrinoFileSystem fileSystem = createFileSystem();
        String tableLocation = getClass().getClassLoader().getResource("databricks73/person").toURI().toString();

        assertThat(getLatestCommitVersion(fileSystem, tableLocation, Optional.empty(), Optional.empty())).hasValue(13L);
        assertThat(getLatestCommitVersion(fileSystem, tableLocation, Optional.of(11L), Optional.of(12L))).hasValue(12L);
        assertThat(getLatestCommitVersion(fileSystem, tableLocation, Optional.of(14L), Optional.empty())).isEmpty();
    }

    @Test
    public void testReadVersionChecksum()
            throws Exception
    {
        TrinoFileSystem fileSystem = createFileSystem();
        Path tableLocation = createDummyDeltaTableWithChecksum("""
                {
                  "metadata": {
                    "id": "test-id",
                    "name": "test",
                    "description": "desc",
                    "format": {
                      "provider": "parquet",
                      "options": {}
                    },
                    "schemaString": "schema",
                    "partitionColumns": ["Part_Col"],
                    "configuration": {
                      "delta.appendOnly": "true"
                    },
                    "createdTime": 123
                  },
                  "protocol": {
                    "minReaderVersion": 1,
                    "minWriterVersion": 2
                  },
                  "numFiles": 7
                }
                """);

        DeltaLakeVersionChecksum checksum = readVersionChecksumFile(fileSystem, tableLocation.toUri().toString(), 0).orElseThrow();
        assertThat(checksum.getMetadata()).isNotNull();
        assertThat(checksum.getMetadata().getId()).isEqualTo("test-id");
        assertThat(checksum.getMetadata().getLowercasePartitionColumns()).containsExactly("part_col");
        assertThat(checksum.getMetadata().getConfiguration()).containsEntry("delta.appendOnly", "true");
        assertThat(checksum.getProtocol()).isEqualTo(new ProtocolEntry(1, 2, Optional.empty(), Optional.empty()));
    }

    @Test
    public void testReadVersionChecksumMissingFile()
            throws Exception
    {
        TrinoFileSystem fileSystem = createFileSystem();
        String tableLocation = getClass().getClassLoader().getResource("databricks73/person").toURI().toString();

        assertThat(readVersionChecksumFile(fileSystem, tableLocation, 99)).isEmpty();
    }

    @Test
    public void testReadVersionChecksumInvalidJson()
            throws Exception
    {
        TrinoFileSystem fileSystem = createFileSystem();
        Path tableLocation = createDummyDeltaTableWithChecksum("{");

        assertThatThrownBy(() -> readVersionChecksumFile(fileSystem, tableLocation.toUri().toString(), 0))
                .isInstanceOfSatisfying(TrinoException.class, exception ->
                        assertThat(exception.getErrorCode()).isEqualTo(DELTA_LAKE_INVALID_SCHEMA.toErrorCode()));
    }

    @Test
    public void testReadVersionChecksumInvalidJsonMapping()
            throws Exception
    {
        TrinoFileSystem fileSystem = createFileSystem();
        Path tableLocation = createDummyDeltaTableWithChecksum("""
                {
                  "protocol": {
                    "minReaderVersion": "abc",
                    "minWriterVersion": 2
                  }
                }
                """);

        assertThatThrownBy(() -> readVersionChecksumFile(fileSystem, tableLocation.toUri().toString(), 0))
                .isInstanceOfSatisfying(TrinoException.class, exception ->
                        assertThat(exception.getErrorCode()).isEqualTo(DELTA_LAKE_INVALID_SCHEMA.toErrorCode()));
    }

    @Test
    public void testReadVersionChecksumJsonWithTrailingContent()
            throws Exception
    {
        TrinoFileSystem fileSystem = createFileSystem();
        Path tableLocation = createDummyDeltaTableWithChecksum("""
                {
                  "protocol": {
                    "minReaderVersion": 1,
                    "minWriterVersion": 2
                  }
                } garbage
                """);

        assertThatThrownBy(() -> readVersionChecksumFile(fileSystem, tableLocation.toUri().toString(), 0))
                .isInstanceOfSatisfying(TrinoException.class, exception ->
                        assertThat(exception.getErrorCode()).isEqualTo(DELTA_LAKE_INVALID_SCHEMA.toErrorCode()));
    }

    private static TrinoFileSystem createFileSystem()
    {
        return new HdfsFileSystemFactory(HDFS_ENVIRONMENT, HDFS_FILE_SYSTEM_STATS).create(SESSION);
    }

    private static Path createDummyDeltaTableWithChecksum(String checksumContents)
            throws IOException
    {
        Path tableLocation = Files.createTempDirectory("delta-checksum-test");
        Path transactionLogDir = tableLocation.resolve("_delta_log");
        Files.createDirectories(transactionLogDir);
        Files.writeString(transactionLogDir.resolve("00000000000000000000.crc"), checksumContents);
        Files.createFile(transactionLogDir.resolve("00000000000000000000.json"));
        return tableLocation;
    }
}
