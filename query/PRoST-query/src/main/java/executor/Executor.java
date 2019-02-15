package executor;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import com.google.protobuf.StringValue;
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
	private static final Logger logger = Logger.getLogger("PRoST");
	private String outputFile;
	private final String databaseName;
	private JoinTree queryTree;
	private final List<String[]> queryTimeTesults;

	private SparkSession spark;
	private SQLContext sqlContext;

	public Executor(final JoinTree queryTree, final String databaseName) {

		this.databaseName = databaseName;
		this.queryTree = queryTree;
		queryTimeTesults = new ArrayList<>();

		// initialize the Spark environment
		spark = SparkSession.builder().appName("PRoST-Executor").getOrCreate();
		sqlContext = spark.sqlContext();
	}

	public void setOutputFile(final String outputFile) {
		this.outputFile = outputFile;
	}

	public void setQueryTree(final JoinTree queryTree) {
		this.queryTree = queryTree;

		// refresh session
		spark = SparkSession.builder().appName("PRoST-Executor").getOrCreate();
		sqlContext = spark.sqlContext();
	}

	/*
	 * execute performs the Spark computation and measure the time required
	 */
	public void execute() {
		// use the selected database
		sqlContext.sql("USE " + databaseName);
		logger.info("USE " + databaseName);

		final long totalStartTime = System.currentTimeMillis();

		// compute the singular nodes data
		queryTree.computeSingularNodeData(sqlContext);
		logger.info("COMPUTED nodes data");

		long startTime;
		long executionTime;

		// compute the joins
		final Dataset<Row> results = queryTree.computeJoins(sqlContext);
		startTime = System.currentTimeMillis();
		long numberResults = -1;
		// if specified, save the results in HDFS, just count otherwise
		if (outputFile != null) {
			results.write().parquet(outputFile);
		} else {
			numberResults = results.count();
			logger.info("Number of Results: " + String.valueOf(numberResults));
		}
		executionTime = System.currentTimeMillis() - startTime;
		logger.info("Execution time JOINS: " + String.valueOf(executionTime));

		// save the results in the list
		queryTimeTesults.add(
				new String[] { queryTree.queryName, String.valueOf(executionTime), String.valueOf(numberResults) });

		final long totalExecutionTime = System.currentTimeMillis() - totalStartTime;
		logger.info("Total execution time: " + String.valueOf(totalExecutionTime));
	}

	public void execute(int k) {
		// use the selected database
		sqlContext.sql("USE " + databaseName);
		logger.info("USE " + databaseName);

		long bestTime = Long.MAX_VALUE;
		long worseTime = 0;
		long totalTime = 0;
		long numberResults = -1;

		final long totalStartTime = System.currentTimeMillis();
		// compute the singular nodes data
		queryTree.computeSingularNodeData(sqlContext);
		logger.info("COMPUTED nodes data");

		long[] timeArray = new long[k];

		for (int i=0; i<k;i++) {
			long startTime;
			long executionTime;

			// compute the joins
			final Dataset<Row> results = queryTree.computeJoins(sqlContext);
			startTime = System.currentTimeMillis();

			// if specified, save the results in HDFS, just count otherwise
			if (outputFile != null) {
				results.write().parquet(outputFile);
			} else {
				numberResults = results.count();
				logger.info("Number of Results: " + String.valueOf(numberResults));
			}
			executionTime = System.currentTimeMillis() - startTime;
			logger.info("Execution time JOINS: " + String.valueOf(executionTime));

			final long totalExecutionTime = System.currentTimeMillis() - totalStartTime;
			logger.info("Total execution time: " + String.valueOf(totalExecutionTime));

			totalTime = totalTime + executionTime;
			if (executionTime>worseTime){
				worseTime = executionTime;
			}
			if(executionTime<bestTime){
				bestTime=executionTime;
			}

			timeArray[i] = executionTime;
		}
		final long averageTime = totalTime/k;
		long standardDeviation = 0;
		for(double t: timeArray) {
			standardDeviation += Math.pow(t - averageTime, 2);
		}
		standardDeviation = (long)Math.sqrt(standardDeviation/k);

		// save the results in the list
		queryTimeTesults.add(
				new String[]{
						queryTree.queryName,
						String.valueOf(averageTime),
						String.valueOf(numberResults),
						String.valueOf(bestTime),
						String.valueOf(worseTime),
						String.valueOf(totalTime),
						String.valueOf(standardDeviation)
				});
	}

	/*
	 * When called, it loads tables into memory before execution. This method is suggested
	 * only for batch execution of queries and in general it doesn't produce benefit (only
	 * overhead) for queries with large intermediate results.
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
		try (BufferedWriter writer = Files.newBufferedWriter(Paths.get(fileName));

				CSVPrinter csvPrinter = new CSVPrinter(writer,
						CSVFormat.DEFAULT.withHeader("Query", "Time (ms)", "Number of results"));) {
			for (final String[] res : queryTimeTesults) {
				csvPrinter.printRecord(res[0], res[1], res[2]);
			}
			csvPrinter.flush();

		} catch (final IOException e) {
			e.printStackTrace();
		}
	}

	public void saveResultsCsv(final String fileName, int k) {
		try (BufferedWriter writer = Files.newBufferedWriter(Paths.get(fileName));

			 CSVPrinter csvPrinter = new CSVPrinter(writer,
					 CSVFormat.DEFAULT.withHeader("Query", "Average (ms)", "Number of results", "Best", "Worse", "Total", "Standard Deviation"))) {
			for (final String[] res : queryTimeTesults) {
				csvPrinter.printRecord(res[0], res[1], res[2], res[3], res[4], res[5], res[6]);
			}
			csvPrinter.flush();

		} catch (final IOException e) {
			e.printStackTrace();
		}
	}

	public void clearQueryTimes() {
		queryTimeTesults.clear();
	}
}
