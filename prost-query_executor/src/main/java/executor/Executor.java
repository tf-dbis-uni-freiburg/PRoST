package executor;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.io.FileWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.HashMap;
import java.util.Collections;

import joinTree.JoinTree;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.execution.QueryExecution;
import com.google.gson.Gson;
import utils.Settings;
import statistics.DatabaseStatistics;
import statistics.PropertyStatistics;


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
	private final DatabaseStatistics statistics;

	public Executor(final Settings settings, final DatabaseStatistics statistics) {
		this.settings = settings;
		this.executionStatistics = new ArrayList<>();
		this.statistics = statistics;

		// initialize the Spark environment
		spark = SparkSession.builder().appName("PRoST-Executor").enableHiveSupport().getOrCreate();
		this.sqlContext = spark.sqlContext();

		// use the selected database
		this.sqlContext.sql("USE " + settings.getDatabaseName());
		//logger.info("USE " + databaseName);

		// only if partition by subject
		//partitionBySubject();
	}

	public void createCharacteristicsFile() {
		List<String> columnNames = getColumnNames();
		Collections.sort(columnNames);
		int columnsNumber = columnNames.size();
	
		Gson gson = new Gson();
		HashMap<String, Long> occurencesMap = new HashMap<String, Long>();
		for (int i = 0; i < columnsNumber; i++) {
			for (int j = i; j < columnsNumber; j++) {
				String a = columnNames.get(i);
				String b = columnNames.get(j);
				occurencesMap.put(a + "-" + b + "-SS",  getPairSize(a, b, "SS"));
				occurencesMap.put(a + "-" + b + "-OO",   getPairSize(a, b, "OO"));
				occurencesMap.put(a + "-" + b + "-SO",   getPairSize(a, b, "SO"));
				occurencesMap.put(a + "-" + b + "-OS",   getPairSize(a, b, "OS"));
			}
		}
		String json = gson.toJson(occurencesMap);
		try (FileWriter file = new FileWriter("characteristics.json")) {
 
            file.write(json);
            file.flush();
 
        } catch (IOException e) {
            e.printStackTrace();
        }
		
	}

	private long getPairSize(String a, String b, String type) {
		
		final ArrayList<String> selectElements = new ArrayList<>();
		final ArrayList<String> whereElements = new ArrayList<>();
		final ArrayList<String> explodedElements = new ArrayList<>();

		String actualA = statistics.getProperties().get(a).getInternalName();
		String actualB = statistics.getProperties().get(b).getInternalName();

		if (type.equals("SS")) {

			selectElements.add("s AS c");

			if (statistics.getProperties().get(a).isComplex()) {
				selectElements.add("a");
				explodedElements.add("lateral view explode(" + actualA + ") explodedA"
							+ " AS a");
			}
			else {
				selectElements.add(actualA + " AS a");
				whereElements.add(actualA + " IS NOT NULL");
			}

			if (statistics.getProperties().get(b).isComplex()) {
				selectElements.add("b");
				explodedElements.add("lateral view explode(" + actualB + ") explodedB"
							+ " AS b");
			}
			else {
				selectElements.add(actualB + " AS b");
				whereElements.add(actualB + " IS NOT NULL");
			}

		}

		else if (type.equals("OO")) {
			selectElements.add("o AS c");

			if (statistics.getProperties().get(a).isInverseComplex()) {
				selectElements.add("a");
				explodedElements.add("lateral view explode(" + actualA + ") explodedA"
							+ " AS a");
			}
			else {
				selectElements.add(actualA + " AS a");
				whereElements.add(actualA + " IS NOT NULL");
			}

			if (statistics.getProperties().get(b).isInverseComplex()) {
				selectElements.add("b");
				explodedElements.add("lateral view explode(" + actualB + ") explodedB"
							+ " AS b");
			}
			else {
				selectElements.add(actualB + " AS b");
				whereElements.add(actualB + " IS NOT NULL");
			}	
		}

		else if (type.equals("SO")) {
			selectElements.add("r AS c");

			if (statistics.getProperties().get(a).isComplex()) {
				selectElements.add("a");
				explodedElements.add("lateral view explode(o_" + actualA + ") explodedA"
							+ " AS a");
			}
			else {
				selectElements.add("o_" +actualA + " AS a");
				whereElements.add("o_" + actualA + " IS NOT NULL");
			}

			if (statistics.getProperties().get(b).isInverseComplex()) {
				selectElements.add("b");
				explodedElements.add("lateral view explode(s_" + actualB + ") explodedB"
							+ " AS b");
			}
			else {
				selectElements.add("s_" + actualB + " AS b");
				whereElements.add("s_" + actualB + " IS NOT NULL");
			}	

		}

		else {
			selectElements.add("r AS c");

			if (statistics.getProperties().get(a).isInverseComplex()) {
				selectElements.add("a");
				explodedElements.add("lateral view explode(s_" + actualA + ") explodedA"
							+ " AS a");
			}
			else {
				selectElements.add("s_" +actualA + " AS a");
				whereElements.add("s_" + actualA + " IS NOT NULL");
			}

			if (statistics.getProperties().get(b).isComplex()) {
				selectElements.add("b");
				explodedElements.add("lateral view explode(o_" + actualB + ") explodedB"
							+ " AS b");
			}
			else {
				selectElements.add("o_" + actualB + " AS b");
				whereElements.add("o_" + actualB + " IS NOT NULL");
			}	
		}

		String tableName = "";
		if (type.equals("SS")) {
			tableName = "wide_property_table";
		}
		else if (type.equals("OO")) {
			tableName = "inverse_wide_property_table";
		}
		else {
			tableName = "joined_wide_property_table_outer";
		}
		String query = "SELECT " + String.join(", ", selectElements);
		query += " FROM " + tableName;
		if (!explodedElements.isEmpty()) {
			query += " " + String.join(" ", explodedElements);
		}
		if (!whereElements.isEmpty()) {
			query += " WHERE " + String.join(" AND ", whereElements);
		}

		long sz = sqlContext.sql(query).count();
		return sz;
	}

	private List<String> getColumnNames() {
		List<String> columnNames = new ArrayList<>();
		HashMap<String, PropertyStatistics> properties = statistics.getProperties();
		for (String key : properties.keySet()) {
			columnNames.add(key);
		}
		return columnNames;
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

			final QueryExecution queryExecution = results.queryExecution();
			final String queryPlan = queryExecution.executedPlan().toString();
			statisticsBuilder.joinsCount(org.apache.commons.lang3.StringUtils.countMatches(queryPlan, "Join"));
			statisticsBuilder.broadcastJoinsCount(org.apache.commons.lang3.StringUtils.countMatches(queryPlan,
					"BroadcastHashJoin"));
			statisticsBuilder.sortMergeJoinsCount(org.apache.commons.lang3.StringUtils.countMatches(queryPlan,
					"SortMergeJoin"));

			statisticsBuilder.withCountedNodes(queryTree);
			statisticsBuilder.withPlansAsStrings(queryExecution);
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
	public void saveResultsCsv(final String fileName) throws IOException {
		final File file = new File(Paths.get(fileName).toString());
		final CSVPrinter csvPrinter;
		final BufferedWriter writer = Files.newBufferedWriter(Paths.get(fileName), StandardCharsets.UTF_8,
				StandardOpenOption.APPEND, StandardOpenOption.CREATE);
		if (file.length() == 0) {
			csvPrinter = new CSVPrinter(writer,
					CSVFormat.DEFAULT.withHeader("Query", "Time (ms)", "Number of results", "Joins",
							"Broadcast Joins", "SortMerge Join", "Join Nodes", "TT Nodes",
							"VP Nodes", "WPT Nodes", "IWPT Nodes", "JWPT Nodes",
							"Logical Plan", "Analyzed Plan", "Optimized Plan", "Executed Plan"));
		} else {
			csvPrinter = new CSVPrinter(writer, CSVFormat.DEFAULT);
		}
		for (final Statistics statistics : this.executionStatistics) {
			if (settings.isSavingSparkPlans()) {
				csvPrinter.printRecord(statistics.getQueryName(), statistics.getExecutionTime(),
						statistics.getResultsCount(), statistics.getJoinsCount(),
						statistics.getBroadcastJoinsCount(), statistics.getSortMergeJoinsCount(),
						statistics.getJoinNodesCount(), statistics.getTtNodesCount(), statistics.getVpNodesCount(),
						statistics.getWptNodesCount(), statistics.getIwptNodesCount(),
						statistics.getJwptNodesCount(),
						statistics.getLogicalPlan(), statistics.getAnalyzedPlan(), statistics.getOptimizedPlan(),
						statistics.getExecutedPlan());
			} else {
				csvPrinter.printRecord(statistics.getQueryName(), statistics.getExecutionTime(),
						statistics.getResultsCount(), statistics.getJoinsCount(),
						statistics.getBroadcastJoinsCount(), statistics.getSortMergeJoinsCount(),
						statistics.getJoinNodesCount(), statistics.getTtNodesCount(), statistics.getVpNodesCount(),
						statistics.getWptNodesCount(), statistics.getIwptNodesCount(),
						statistics.getJwptNodesCount());
			}
		}
		csvPrinter.flush();
	}
	class Characteristic {
		private String key;
		private long occurences;

		public Characteristic(String key, long occurences) {
			this.key = key;
			this.occurences = occurences;
		}
	}
}
