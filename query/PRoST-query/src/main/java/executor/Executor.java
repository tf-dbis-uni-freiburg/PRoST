package executor;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;

import joinTree.JoinTree;

/**
 * This class reads the JoinTree, and executes it on top of Spark .
 *
 * @author Matteo Cossu
 */
public class Executor {

	private String outputFile;
	private final String databaseName;
	//private JoinTree queryTree;
	private final List<String[]> query_time_results;

	SparkSession spark;
	SQLContext sqlContext;

	private static final Logger logger = Logger.getLogger("PRoST");

	public Executor(/*final JoinTree queryTree,*/ final String databaseName) {

		this.databaseName = databaseName;
//		this.queryTree = queryTree;
		query_time_results = new ArrayList<>();

		// initialize the Spark environment
		spark = SparkSession.builder().appName("PRoST-Executor").getOrCreate();
		sqlContext = spark.sqlContext();
		
		// use the selected database
		sqlContext.sql("USE " + databaseName);
		logger.info("USE " + databaseName);
	}

	public void setOutputFile(final String outputFile) {
		this.outputFile = outputFile;
	}

	public void setQueryTree(final JoinTree queryTree) {
//		this.queryTree = queryTree;

		// refresh session
		spark = SparkSession.builder().appName("PRoST-Executor").getOrCreate();
		sqlContext = spark.sqlContext();
	}

	/*
	 * execute performs the Spark computation and measure the time required
	 */
	public void execute(JoinTree queryTree) {
//		// use the selected database
//		sqlContext.sql("USE " + databaseName);
//		logger.info("USE " + databaseName);
//
//		final long totalStartTime = System.currentTimeMillis();
//		// logger.info("Start computing the query tree!");
//		// compute the singular nodes data
//		queryTree.computeSingularNodeData(sqlContext);
//		// logger.info("COMPUTED nodes data");
//
//		long startTime;
//		long executionTime;
//
//		// compute the joins
//		// logger.info("Compute joins");
//		final Dataset<Row> results = queryTree.computeJoins(sqlContext);
//		//logger.info("Query plan:");
//		//logger.info(results.queryExecution().analyzed().toString());
//		//logger.info(results.queryExecution().executedPlan().toString());
//		startTime = System.currentTimeMillis();
//		long number_results = -1;
//		// if specified, save the results in HDFS, just count otherwise
//		if (outputFile != null) {
//			results.write().parquet(outputFile);
//		} else {
//			number_results = results.count();
//		}
//		executionTime = System.currentTimeMillis() - startTime;
//		logger.info("Execution time JOINS: " + String.valueOf(executionTime));
//
//		// save the results in the list
//		query_time_results.add(
//				new String[] { queryTree.query_name, String.valueOf(executionTime), String.valueOf(number_results) });
//
//		final long totalExecutionTime = System.currentTimeMillis() - totalStartTime;
//		logger.info("Total execution time: " + String.valueOf(totalExecutionTime));
	}

	/*
	 * execute performs the Spark computation and measure the time required
	 */
	public void executeBottomUp(JoinTree queryTree) {
		// use the selected database
//		sqlContext.sql("USE " + databaseName);
//		logger.info("USE " + databaseName);

		final long totalStartTime = System.currentTimeMillis();
		// logger.info("Start computing the query tree!");
		// compute the singular nodes data
		// queryTree.computeSingularNodeData(sqlContext);
		// logger.info("COMPUTED nodes data");

		long startTime;
		long executionTime;

		// compute the joins
		// logger.info("Compute joins");
		final Dataset<Row> results = queryTree.compute(sqlContext);
		//logger.info("Query plan:");
		//logger.info(results.queryExecution().analyzed().toString());
		//logger.info(results.queryExecution().executedPlan().toString());
		startTime = System.currentTimeMillis();
		long number_results = -1;
		// if specified, save the results in HDFS, just count otherwise
		if (outputFile != null) {
			results.write().parquet(outputFile);
		} else {
			number_results = results.count();
		}
		executionTime = System.currentTimeMillis() - startTime;
		logger.info("Execution time JOINS: " + String.valueOf(executionTime));

		// save the results in the list
		query_time_results.add(
				new String[] { queryTree.query_name, String.valueOf(executionTime), String.valueOf(number_results) });

		final long totalExecutionTime = System.currentTimeMillis() - totalStartTime;
		logger.info("Total execution time: " + String.valueOf(totalExecutionTime));
	}
	
	/*
	 * When called, it loads tables into memory before execution. This method is suggested only for batch execution of
	 * queries and in general it doesn't produce benefit (only overhead) for queries with large intermediate results.
	 */
	public void cacheTables() {
		sqlContext.sql("USE " + databaseName);
		final List<Row> tablesNamesRows = sqlContext.sql("SHOW TABLES").collectAsList();
		for (final Row row : tablesNamesRows) {
			final String name = row.getString(1);
			// skip the property table
			if (name.equals("property_table")) {
				continue;
			}
			spark.catalog().cacheTable(name);
		}
	}

	/*
	 * Save the results <query name, execution time, number of results> in a csv file.
	 */
	public void saveResultsCsv(final String fileName) {
		try (BufferedWriter writer = Files.newBufferedWriter(Paths.get(fileName), StandardCharsets.UTF_8, 
                StandardOpenOption.APPEND);
				CSVPrinter csvPrinter = new CSVPrinter(writer,
						CSVFormat.DEFAULT.withHeader("Query", "Time (ms)", "Number of results"));) {
			for (final String[] res : query_time_results) {
				csvPrinter.printRecord(res[0], res[1], res[2]);
			}
			csvPrinter.flush();

		} catch (final IOException e) {
			e.printStackTrace();
		}
	}

	public void clearQueryTimes() {
		query_time_results.clear();
	}
}
