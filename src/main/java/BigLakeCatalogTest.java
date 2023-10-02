import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.time.Instant;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.Transaction;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.gcp.GCPProperties;
import org.apache.iceberg.gcp.biglake.BigLakeCatalog;
import org.apache.iceberg.gcp.gcs.GCSFileIO;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.types.Types;
import org.junit.Assert;

public class BigLakeCatalogTest
{
    public static void main(String... args) throws IOException
    {
        String accessTokenJson = "___ACCESS_TOKEN_JSON___";

        Map token = new ObjectMapper().readValue(accessTokenJson, Map.class);

        Instant expireTime = Instant.parse((String) token.get("expireTime"));

        Assert.assertFalse("token expired", expireTime.isBefore(Instant.now()));
        Map<String, String> props = new HashMap<>();
        props.put(CatalogProperties.WAREHOUSE_LOCATION, "gs://gstest-bucket-1/iceberg_catalog");
        props.put(GCPProperties.PROJECT_ID, "sky-225649-proj-c89517cd");
        props.put(GCPProperties.REGION, "us");
        props.put(GCPProperties.GCS_OAUTH2_TOKEN, (String) token.get("accessToken"));
        props.put(GCPProperties.GCS_OAUTH2_TOKEN_EXPIRES_AT, Long.toString(expireTime.toEpochMilli()));
        props.put(CatalogProperties.FILE_IO_IMPL, GCSFileIO.class.getCanonicalName());

        BigLakeCatalog catalog = new BigLakeCatalog();
        catalog.initialize("iceberg_catalog", props);

        TableIdentifier tableIdentifier = getTable(catalog, "iceberg_warehouse.nyc_taxi_trips_iceberg");
        Table table = loadAndLogTableDetails(catalog, tableIdentifier);
        FileIO io = table.io();

        List<String> files = Arrays.asList(
                "gs://gstest-bucket-1/biglake1/data/b409209b-2554-4e3f-86ee-adda00f26796-296421583d0f44b0-f-00000-of-00001.parquet",
                "gs://gstest-bucket-1/biglake1/data/d53c5a4d-9cc2-431c-8958-5a6428b5fd12-6f08412c7a183267-f-00000-of-00001.parquet"
        );

        Types.NestedField field1 = Types.NestedField.required(1, "a", Types.IntegerType.get());
        Types.NestedField field2 = Types.NestedField.optional(2, "b", Types.StringType.get());
        Schema newSchema = new Schema(field1, field2);

        String testTable = "rafael_test3";

        catalog.dropTable(TableIdentifier.of("iceberg_warehouse", testTable), false);
        PartitionSpec partitionSpec = PartitionSpec.builderFor(newSchema).build();

        Catalog.TableBuilder tableBuilder = catalog.buildTable(TableIdentifier.of("iceberg_warehouse", testTable), newSchema);
        Transaction transaction = tableBuilder.createTransaction();

        AppendFiles appendFiles = transaction.newAppend();

        for (String file : files)
        {
            InputFile inputFile = io.newInputFile(file);
            long count = 0;
            try (CloseableIterable<Object> iterator = Parquet.read(inputFile).project(newSchema).build())
            {
                for (Object o : iterator)
                {
                    count++;
                }
            }

            DataFile dataFile = DataFiles.builder(partitionSpec)
                    .withRecordCount(count)
                    .withInputFile(inputFile)
                    .withFormat(FileFormat.PARQUET)
                    .build();

            appendFiles.appendFile(dataFile);
        }

        appendFiles.commit();
        transaction.commitTransaction();

        loadAndLogTableDetails(catalog, TableIdentifier.of("iceberg_warehouse", testTable)).refresh();

    }

    private static Table loadAndLogTableDetails(BigLakeCatalog catalog, TableIdentifier tableIdentifier)
    {
        Table table = catalog.loadTable(tableIdentifier);
        Snapshot snapshot = table.currentSnapshot();
        System.out.println(snapshot.toString());
        System.out.println(table.properties().toString());
        return table;
    }

    private static TableIdentifier getTable(BigLakeCatalog catalog, String tableName)
    {
        List<TableIdentifier> tableIdentifiers = catalog.listTables(Namespace.empty());
        System.out.println(tableIdentifiers.stream().map(TableIdentifier::toString).collect(Collectors.joining(", ")));
        Optional<TableIdentifier> any = tableIdentifiers.stream().filter(x -> x.toString().equals(tableName)).findAny();
        return any.orElseThrow(() -> new RuntimeException("Table not found"));
    }
}
