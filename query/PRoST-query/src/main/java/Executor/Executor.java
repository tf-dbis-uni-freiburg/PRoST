package Executor;

import java.io.BufferedWriter;

import java.io.IOException;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;

import JoinTree.JoinTree;

/**
 * This class reads the JoinTree,
 * and executes it on top of Spark .
 *
 * @author Matteo Cossu
 */
public class Executor {

	private String outputFile;
	private String databaseName;
	private JoinTree queryTree;
	private List<String[]> query_time_results;
	
	SparkSession spark;
	SQLContext sqlContext;


	private static final Logger logger = Logger.getLogger(run.Main.class);
	
	public Executor(JoinTree queryTree, String databaseName){
		
		this.databaseName = databaseName;
		this.queryTree = queryTree;
		this.query_time_results = new ArrayList<String[]>();
		
		// initialize the Spark environment 
		spark = SparkSession
				  .builder()
				  .appName("PRoST-Executor")
				  .getOrCreate();
	}
	
	public void setOutputFile(String outputFile){
		this.outputFile = outputFile;
	}
	
	public void setQueryTree(JoinTree queryTree) {
		this.queryTree = queryTree;
	}

		
	
	/*
	 * execute performs the Spark computation and measure the time required
	 */
	public void execute() {
		// use the selected database
		sqlContext.sql("USE "+ this.databaseName);
		logger.info("USE "+ this.databaseName);
		
		long totalStartTime = System.currentTimeMillis();
		
		// compute the singular nodes data
		queryTree.computeSingularNodeData(sqlContext);
		logger.info("COMPUTED nodes data");
		
		
		long startTime;
		long executionTime;
		
		// compute the joins
		
		Dataset<Row> results = queryTree.computeJoins(sqlContext);
		startTime = System.currentTimeMillis();
		long number_results = -1;
		// if specified, save the results in HDFS, just count otherwise
		if (this.outputFile != null) {
			results.write().parquet(this.outputFile);
		} else {
			number_results = results.count();
			logger.info("Number of Results: " + String.valueOf(number_results));
		}
		executionTime = System.currentTimeMillis() - startTime;
		logger.info("Execution time JOINS: " + String.valueOf(executionTime));
		
		// save the results in the list
		this.query_time_results.add(new String[]{queryTree.query_name, 
				String.valueOf(executionTime), 
				String.valueOf(number_results)});
		
		
		long totalExecutionTime = System.currentTimeMillis() - totalStartTime;
		logger.info("Total execution time: " + String.valueOf(totalExecutionTime));
	}
	
	/*
	 * When called, it loads tables into memory before execution.
	 * This method is suggested only for batch execution of queries
	 * and in general it doesn't produce benefit (only overhead) 
	 * for queries with large intermediate results.
	 */
	public void cacheTables() {
		sqlContext.sql("USE "+ this.databaseName);
		List<Row> tablesNamesRows = sqlContext.sql("SHOW TABLES").collectAsList();
		for(Row row : tablesNamesRows){
			String name = row.getString(1);
			// skip the property table
			if (name.equals("property_table"))
				continue;
			spark.catalog().cacheTable(name);
		}
		
	}
	
	/*
	 * Save the results <query name, execution time, number of results> in a csv file.
	 */
	public void saveResultsCsv(String fileName) {
		try (
	            BufferedWriter writer = Files.newBufferedWriter(Paths.get(fileName));

	            CSVPrinter csvPrinter = new CSVPrinter(writer, CSVFormat.DEFAULT
	                    .withHeader("Query", "Time (ms)", "Number of results"));
	        ) {
				for(String[] res :  this.query_time_results)
					csvPrinter.printRecord(res[0], res[1], res[2]);
	            csvPrinter.flush();            
	            
	        } catch (IOException e) {
				e.printStackTrace();
			}
	}
	
	public void clearQueryTimes() {
		this.query_time_results.clear();
	}
}
