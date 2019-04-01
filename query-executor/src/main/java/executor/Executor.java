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
 * Class that reads and executes join trees.
 *
 * @author Matteo Cossu
 * @author Polina Koleva
 */
public class Executor {

	private static final Logger logger = Logger.getLogger("PRoST");

	private String outputFile;
	private final String databaseName;
	private final List<String[]> queryTimeResults;
	private final List<String[]> queryStatistics;

	SparkSession spark;
	SQLContext sqlContext;

	public Executor(final String databaseName) {

		this.databaseName = databaseName;
		this.queryTimeResults = new ArrayList<>();
		this.queryStatistics = new ArrayList<>();

		// initialize the Spark environment
		spark = SparkSession.builder().appName("PRoST-Executor").getOrCreate();
		this.sqlContext = spark.sqlContext();

		// use the selected database
		this.sqlContext.sql("USE " + databaseName);
		logger.info("USE " + databaseName);

		// only if partition by subject
		//partitionBySubject();
	}

	public void setOutputFile(final String outputFile) {
		this.outputFile = outputFile;
	}

	/*
	 * Reads and execute a join tree, starting from the root node.Moreover, it
	 * performs the Spark computation and measure the time required.
	 */
	public void execute(JoinTree queryTree) {
		final long totalStartTime = System.currentTimeMillis();

		long startTime;
		long executionTime;
		final Dataset<Row> results = queryTree.compute(this.sqlContext);

		startTime = System.currentTimeMillis();
		long resultCount = -1;
		// if specified, save the results in HDFS, just count otherwise
		if (outputFile != null) {
			results.write().parquet(outputFile);
		} else {
			resultCount = results.count();
		}
		executionTime = System.currentTimeMillis() - startTime;
		logger.info("Execution time JOINS: " + String.valueOf(executionTime));

		// save the results in the list
		queryTimeResults
				.add(new String[] { queryTree.query_name, String.valueOf(executionTime), String.valueOf(resultCount) });
		// get information from the query and add it to overall statistics
		computeStatistics(queryTree, results);

		final long totalExecutionTime = System.currentTimeMillis() - totalStartTime;
		logger.info("Total execution time: " + String.valueOf(totalExecutionTime));
	}

	// TODO add comment
	private void computeStatistics(JoinTree queryTree, Dataset<Row> queryResult) {
		// get the join type
		String queryPlan = queryResult.queryExecution().executedPlan().toString();
		// count number of joins overall
		int joinsCount = org.apache.commons.lang3.StringUtils.countMatches(queryPlan, "Join");
		// count number of broadcast joins for a query
		int broadcastJoinCount = org.apache.commons.lang3.StringUtils.countMatches(queryPlan, "BroadcastHashJoin");
		// count number of sort merge joins
		int sortMergeJoinCount = org.apache.commons.lang3.StringUtils.countMatches(queryPlan, "SortMergeJoin");
		// save the statistics
		queryStatistics.add(new String[] { queryTree.query_name, String.valueOf(joinsCount),
				String.valueOf(broadcastJoinCount), String.valueOf(sortMergeJoinCount),
				String.valueOf(queryTree.getVpLeavesCount()), String.valueOf(queryTree.getWptLeavesCount()) });
	}

	/*
	 * When called, it loads tables into memory before execution. This method is
	 * suggested only for batch execution of queries and in general it doesn't
	 * produce benefit (only overhead) for queries with large intermediate results.
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

	public void partitionBySubject() {
		final long startTime = System.currentTimeMillis();
		final List<Row> tablesNamesRows = sqlContext.sql("SHOW TABLES").collectAsList();
		for (final Row row : tablesNamesRows) {
			String tableName = row.getString(1);
			if (tableName.equals("properties") || tableName.equals("tripletable_ext")) {
				continue;
			}
			Dataset tableData = this.spark.sql("SELECT * FROM " + tableName + " DISTRIBUTE BY s");
			tableData.registerTempTable("par_" + tableName);
			// cache tables
			// sqlContext.sql("CACHE TABLE par_" + tableName);
			// force partitioning
			tableData.count();
		}
		final long  executionTime = System.currentTimeMillis() - startTime;
		logger.info("Execution time for hashing by subject: " + String.valueOf(executionTime));
	}

	/*
	 * Save the results <query name, execution time, number of results> in a csv
	 * file.
	 */
	public void saveResultsCsv(final String fileName) {
		try (BufferedWriter writer = Files.newBufferedWriter(Paths.get(fileName), StandardCharsets.UTF_8,
				StandardOpenOption.APPEND);
				CSVPrinter csvPrinter = new CSVPrinter(writer,
						CSVFormat.DEFAULT.withHeader("Query", "Time (ms)", "Number of results"));) {
			for (final String[] res : this.queryTimeResults) {
				csvPrinter.printRecord(res[0], res[1], res[2]);
			}
			csvPrinter.printRecord("Query", "Joins", "Broadcast Joins", "SortMerge Join", "VP Nodes", "WPT Nodes");
			for (final String[] res : this.queryStatistics) {
				csvPrinter.printRecord(res[0], res[1], res[2], res[3], res[4], res[5]);
			}
			csvPrinter.flush();

		} catch (final IOException e) {
			e.printStackTrace();
		}
}
//	/*
//	 * Save the results <query name, execution time, number of results> in a csv
//	 * file.
//	 */
//	public void saveResultsCsv(final String fileName, String[] timeResult, String[] joinResults) {
//		try (BufferedWriter writer = Files.newBufferedWriter(Paths.get(fileName), StandardCharsets.UTF_8,
//				StandardOpenOption.APPEND);
//				CSVPrinter csvPrinter = new CSVPrinter(writer,
//						CSVFormat.DEFAULT.withHeader("Query", "Time (ms)", "Number of results"));) {
//			csvPrinter.printRecord(timeResult[0], timeResult[1], timeResult[2]);
//			csvPrinter.printRecord("Query", "Joins", "Broadcast Joins", "SortMerge Join");
//			csvPrinter.printRecord(joinResults[0], joinResults[1], joinResults[2], joinResults[3]);
//			csvPrinter.flush();
//
//		} catch (final IOException e) {
//			e.printStackTrace();
//		}
//	}

	public void clearHistory() {
		queryTimeResults.clear();
	}
}