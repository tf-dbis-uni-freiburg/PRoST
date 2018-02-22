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
  public final int max_parallelism = 5;
  private boolean computeStatistics;

	public VerticalPartitioningLoader(String hdfs_input_directory,
			String database_name, SparkSession spark, boolean computeStatistics) {
		super(hdfs_input_directory, database_name, spark);
		this.computeStatistics = computeStatistics;
	}

	@Override
	public void load() {
		long startTime;
		long executionTime;
	
		startTime = System.currentTimeMillis();

		LOGGER.info("Beginning the creation of VP tables.");
		
		if (this.propertiesNames == null){
			LOGGER.error("Properties not calculated yet. Extracting them");
			this.propertiesNames = extractProperties();
		}
		
		Vector<TableStats> tables_stats =  new Vector<TableStats>();
		
		for(int i = 0; i < this.propertiesNames.length; i++){
		    String property = this.propertiesNames[i];
			Dataset<Row> table_VP = spark.sql("SELECT s AS s, o AS o FROM tripletable WHERE p='" + property + "'");
			String table_name_VP = "vp_" + this.getValidHiveName(property);
			// save the table
			table_VP.write().mode(SaveMode.Overwrite).saveAsTable(table_name_VP);
			// calculate stats
			if(computeStatistics)
			  tables_stats.add(calculate_stats_table(table_VP, this.getValidHiveName(property)));
			
			executionTime = System.currentTimeMillis() - startTime;
			
			LOGGER.info("Created VP table for the property: " + property);
			LOGGER.info("Vertical partitions created in: " + String.valueOf(executionTime));
		}
		
		// save the stats in a file with the same name as the output database
		if(computeStatistics)
		  save_stats(this.databaseName, tables_stats);
		
		LOGGER.info("Vertical Partitioning completed. Loaded " + String.valueOf(this.propertiesNames.length) + " tables.");
		
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
		int distinct_subjects = (int) table.select(this.subjectColumnName).distinct().count();
		boolean is_complex = table_size != distinct_subjects;
		
		String query = new String("select is_complex from reverse_properties where p='" + tableName + "'" );
		
		boolean is_revese_complex = spark.sql(query.toString()).head().getInt(0)==1;

		// put them in the protobuf object
		table_stats_builder.setSize(table_size)
			.setDistinctSubjects(distinct_subjects)
			.setIsComplex(is_complex)
			.setName(tableName)
			.setIsReverseComplex(is_revese_complex);
		
		return table_stats_builder.build();
	}
	
	
	/*
	 * save the statistics in a serialized file
	 */
	private void save_stats(String name, List<TableStats> table_stats) {
		Graph.Builder graph_stats_builder = Graph.newBuilder();
		
		graph_stats_builder.addAllTables(table_stats);
		
		Graph serialized_stats = graph_stats_builder.build();
		
		FileOutputStream f_stream;
		File file;
		try {
			file = new File(name + this.statsFileSufix);
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
				predicateColumnName, tripleTableName)).collectAsList();
		String[] result_properties = new String[props.size()];
		
		for (int i = 0; i < props.size(); i++) {
			result_properties[i] = props.get(i).getString(0);
		}
		return result_properties;
	}
	
}
