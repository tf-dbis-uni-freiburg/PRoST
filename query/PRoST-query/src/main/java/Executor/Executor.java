package Executor;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.List;

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

	String outputDB;
	String databaseName;
	JoinTree queryTree;
	SparkSession spark;
	SQLContext sqlContext;


	private static final Logger logger = Logger.getLogger(run.Main.class);
	
	public Executor(JoinTree queryTree, String outputFile, String databaseName){
		this.outputDB = outputFile;
		this.databaseName = databaseName;
		this.queryTree = queryTree;
		
		// initialize the Spark environment 
		spark = SparkSession
				  .builder()
				  .appName("SparkVP-Executor")
				  .getOrCreate();
		sqlContext = spark.sqlContext();
	}
		
	
	/*
	 * execute performs the Spark computation and measure the time required
	 */
	public void execute() {
		// use the selected Database
		sqlContext.sql("USE "+ this.databaseName);
		logger.info("USE "+ this.databaseName);
		
		PrintWriter out;
		try {
			@SuppressWarnings("resource")
			FileWriter fw = new FileWriter("RESULTS.txt", true);
		    BufferedWriter bw = new BufferedWriter(fw);
		    out = new PrintWriter(bw);
		} catch (IOException e) {
			logger.error("Cannot write results file");
			return;
		}
		long totalStartTime = System.currentTimeMillis();
		
		// compute the singular nodes data
		queryTree.computeSingularNodeData(sqlContext);
		logger.info("COMPUTED singular nodes data");
		
		
		long startTime;
		long executionTime;
		
		// compute the full joins
		startTime = System.currentTimeMillis();
		Dataset<Row> results = queryTree.computeJoins(sqlContext);
		logger.info("Joins Computed succesfully");
		
		System.out.println(results.count());
		executionTime = System.currentTimeMillis() - startTime;
		logger.info("Execution time JOINS: " + String.valueOf(executionTime));
		
		
		out.write(String.valueOf(executionTime) + "\n");
		out.close();
		
		long totalExecutionTime = System.currentTimeMillis() - totalStartTime;
		logger.info("Total execution time: " + String.valueOf(totalExecutionTime));
	}
	
	public void cacheTables() {
		sqlContext.sql("USE "+ this.databaseName);
		List<Row> tablesNamesRows = sqlContext.sql("SHOW TABLES").collectAsList();
		for(Row row : tablesNamesRows){
			String name = row.getString(1);
			if (name.equals("property_table"))
				continue;
			spark.catalog().cacheTable(name);
		}
		
	}
}
