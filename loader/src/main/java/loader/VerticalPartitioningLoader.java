package loader;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.Vector;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

import loader.ProtobufStats.Graph;
import loader.ProtobufStats.TableStats;

public class VerticalPartitioningLoader extends Loader {
    private boolean computeStatistics;

    public VerticalPartitioningLoader(String hdfs_input_directory,
                                      String database_name, SparkSession spark, boolean computeStatistics) {
        super(hdfs_input_directory, database_name, spark);
        this.computeStatistics = computeStatistics;
    }

    @Override
    public void load() {

        logger.info("Beginning the creation of VP tables.");

        if (this.properties_names == null) {
            logger.error("Properties not calculated yet. Extracting them");
            this.properties_names = extractProperties();
        }

        Vector<TableStats> tables_stats = new Vector<TableStats>();

        for (int i = 0; i < this.properties_names.length; i++) {
            String property = this.properties_names[i];
            Dataset<Row> table_VP = spark.sql("SELECT s AS s, o AS o FROM tripletable WHERE p='" + property + "'");
            String table_name_VP = "vp_" + this.getValidHiveName(property);
            // save the table
            table_VP.write().mode(SaveMode.Overwrite).saveAsTable(table_name_VP);
            // calculate stats
            if (computeStatistics)
                tables_stats.add(calculate_stats_table(table_VP, this.getValidHiveName(property)));

            logger.info("Created VP table for the property: " + property);
        }

        // save the stats in a file with the same name as the output database
        if (computeStatistics)
            save_stats(this.database_name, tables_stats);

        logger.info("Vertical Partitioning completed. Loaded " + String.valueOf(this.properties_names.length) + " tables.");

    }

    /*
     * calculate the statistics for a single table:
     * size, number of distinct subjects and isComplex.
     * It returns a protobuf object defined in ProtobufStats.proto
     */
    private TableStats calculate_stats_table(Dataset<Row> table, String tableName) {
        TableStats.Builder table_stats_builder = TableStats.newBuilder();

        // calculate the stats
        int table_size = (int) table.count();
        int distinct_subjects = (int) table.select(this.column_name_subject).distinct().count();
        boolean is_complex = table_size != distinct_subjects;

        // put them in the protobuf object
        table_stats_builder.setSize(table_size)
                .setDistinctSubjects(distinct_subjects)
                .setIsComplex(is_complex)
                .setName(tableName);

        return table_stats_builder.build();
    }


    /*
     * save the statistics in a serialized file
     */
    private void save_stats(String name, List<TableStats> table_stats) {
        Graph.Builder graph_stats_builder = Graph.newBuilder();

        graph_stats_builder.addAllTables(table_stats);
        graph_stats_builder.setArePrefixesActive(arePrefixesUsed());

        Graph serialized_stats = graph_stats_builder.build();


        FileOutputStream f_stream; //s
        File file;
        try {
            file = new File(name + this.stats_file_suffix);
            f_stream = new FileOutputStream(file);
            serialized_stats.writeTo(f_stream);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    private String[] extractProperties() {
        List<Row> props = spark.sql(String.format("SELECT DISTINCT(%1$s) AS %1$s FROM %2$s",
                column_name_predicate, name_tripletable)).collectAsList();
        String[] result_properties = new String[props.size()];

        for (int i = 0; i < props.size(); i++) {
            result_properties[i] = props.get(i).getString(0);
        }
        return result_properties;
    }

    /**
     * Checks if there is at least one property that uses prefixes.
     */
    private boolean arePrefixesUsed(){
       for (String property : properties_names){
          if (property.contains(":")){
              return true;
          }
       }
       return false;
    }

}
