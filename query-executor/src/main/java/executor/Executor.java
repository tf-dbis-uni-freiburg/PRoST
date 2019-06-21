package executor;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;

import joinTree.JoinTree;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import utils.Settings;

/**
 * Class that reads and executes join trees.
 *
 * @author Matteo Cossu
 * @author Polina Koleva
 */
public class Executor {
	private static final Logger logger = Logger.getLogger("PRoST");
	private final Settings settings;
	private final SparkSession spark;
	private final SQLContext sqlContext;
	private final List<Statistics> executionStatistics;

	public Executor(final Settings settings) {
		this.settings = settings;
		this.executionStatistics = new ArrayList<>();

		// initialize the Spark environment
		spark = SparkSession.builder().appName("PRoST-Executor").enableHiveSupport().getOrCreate();
		this.sqlContext = spark.sqlContext();

		// use the selected database
		this.sqlContext.sql("USE " + settings.getDatabaseName());
		//logger.info("USE " + databaseName);

		// only if partition by subject
		//partitionBySubject();
	}

	/*
	 * Reads and execute a join tree, starting from the root node.Moreover, it
	 * performs the Spark computation and measure the time required.
	 */
	public void execute(final JoinTree queryTree) {
		final long executionTime;
		final long totalStartTime = System.currentTimeMillis();

		final long startTime;
		final Dataset<Row> results = queryTree.compute(this.sqlContext);

		startTime = System.currentTimeMillis();


		if (settings.getOutputFilePath() != null) {
			results.write().mode(SaveMode.Overwrite).parquet(settings.getOutputFilePath());
		}
		final long resultsCount = results.count();

		executionTime = System.currentTimeMillis() - startTime;
		logger.info("Execution time JOINS: " + executionTime);

		final long totalExecutionTime = System.currentTimeMillis() - totalStartTime;
		logger.info("Total execution time: " + totalExecutionTime);

		if (settings.isSavingBenchmarkFile()) {
			final Statistics.Builder statisticsBuilder = new Statistics.Builder(queryTree.getQueryName());
			statisticsBuilder.executionTime(executionTime);
			statisticsBuilder.resultsCount(resultsCount);

			final String queryPlan = results.queryExecution().executedPlan().toString();
			statisticsBuilder.joinsCount(org.apache.commons.lang3.StringUtils.countMatches(queryPlan, "Join"));
			statisticsBuilder.broadcastJoinsCount(org.apache.commons.lang3.StringUtils.countMatches(queryPlan,
					"BroadcastHashJoin"));
			statisticsBuilder.sortMergeJoinsCount(org.apache.commons.lang3.StringUtils.countMatches(queryPlan,
					"SortMergeJoin"));

			statisticsBuilder.withCountedNodes(queryTree);
			executionStatistics.add(statisticsBuilder.build());
		}
	}

	/*
	 * When called, it loads tables into memory before execution. This method is
	 * suggested only for batch execution of queries and in general it doesn't
	 * produce benefit (only overhead) for queries with large intermediate results.
	 */
	public void cacheTables() {
		sqlContext.sql("USE " + settings.getDatabaseName());
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
			final String tableName = row.getString(1);
			if (tableName.equals("properties") || tableName.equals("tripletable_ext")) {
				continue;
			}
			final Dataset tableData = this.spark.sql("SELECT * FROM " + tableName + " DISTRIBUTE BY s");
			tableData.registerTempTable("par_" + tableName);
			// cache tables
			// sqlContext.sql("CACHE TABLE par_" + tableName);
			// force partitioning
			tableData.count();
		}
		final long executionTime = System.currentTimeMillis() - startTime;
		logger.info("Execution time for hashing by subject: " + executionTime);
	}

	/*
	 * Save the results <query name, execution time, number of results> in a csv
	 * file.
	 */
	public void saveResultsCsv(final String fileName) {
		final File file = new File(Paths.get(fileName).toString());
		if (!file.exists()) {
			try (final BufferedWriter writer = Files.newBufferedWriter(Paths.get(fileName), StandardCharsets.UTF_8,
					StandardOpenOption.APPEND, StandardOpenOption.CREATE);

				 final CSVPrinter csvPrinter = new CSVPrinter(writer,
						 CSVFormat.DEFAULT.withHeader("Query", "Time (ms)", "Number of results", "Joins",
								 "Broadcast Joins", "SortMerge Join", "Join Nodes", "TT Nodes",
								 "VP Nodes", "WPT Nodes", "IWPT Nodes", "JWPT Nodes"))) {
				for (final Statistics statistics : this.executionStatistics) {
					csvPrinter.printRecord(statistics.getQueryName(), statistics.getExecutionTime(),
							statistics.getResultsCount(), statistics.getJoinsCount(),
							statistics.getBroadcastJoinsCount(), statistics.getSortMergeJoinsCount(),
							statistics.getJoinNodesCount(), statistics.getTtNodesCount(), statistics.getVpNodesCount(),
							statistics.getWptNodesCount(), statistics.getIwptNodesCount(), statistics.getJwptNodesCount());
				}
				csvPrinter.flush();
			} catch (final IOException e) {
				e.printStackTrace();
			}
		} else {
			try (final BufferedWriter writer = Files.newBufferedWriter(Paths.get(fileName), StandardCharsets.UTF_8,
					StandardOpenOption.APPEND, StandardOpenOption.CREATE);
				 final CSVPrinter csvPrinter = new CSVPrinter(writer,
						 CSVFormat.DEFAULT)) {
				for (final Statistics statistics : this.executionStatistics) {
					csvPrinter.printRecord(statistics.getQueryName(), statistics.getExecutionTime(),
							statistics.getResultsCount(), statistics.getJoinsCount(),
							statistics.getBroadcastJoinsCount(), statistics.getSortMergeJoinsCount(),
							statistics.getJoinNodesCount(), statistics.getTtNodesCount(), statistics.getVpNodesCount(),
							statistics.getWptNodesCount(), statistics.getIwptNodesCount(), statistics.getJwptNodesCount());
				}
				csvPrinter.flush();

			} catch (final IOException e) {
				e.printStackTrace();
			}
		}
	}
}
