package run;

import java.io.FileNotFoundException;
import java.io.InputStream;
import java.util.Properties;
import java.util.stream.IntStream;

import loader.InverseWidePropertyTableLoader;
import loader.JoinedWidePropertyTableLoader;
import loader.Settings;
import loader.TripleTableLoader;
import loader.VerticalPartitioningLoader;
import loader.WidePropertyTableLoader;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import stats.DatabaseStatistics;

/**
 * The Main class parses the CLI arguments and calls the executor.
 * <p>
 * Options: -h, --help prints the usage help message. -i, --input {@code <file>} HDFS
 * input path of the RDF graph. -o, --output {@code <DBName>} output database name. -s,
 * compute statistics
 *
 * @author Matteo Cossu
 * @author Victor Anthony Arrascue Ayala
 */
public class Main {
	private static final String loj4jFileName = "log4j.properties";
	private static final Logger logger = Logger.getLogger("PRoST");

	public static void main(final String[] args) throws Exception {
		logger.info("INITIALIZING LOADER");

		final InputStream inStream = Main.class.getClassLoader().getResourceAsStream(loj4jFileName);
		final Properties props = new Properties();
		props.load(inStream);
		PropertyConfigurator.configure(props);

		final Settings settings = new Settings(args);


		DatabaseStatistics statistics;
		try {
			statistics = DatabaseStatistics.loadFromFile(settings.getDatabaseName() + ".stats");
		} catch (FileNotFoundException e) {
			statistics = new DatabaseStatistics(settings.getDatabaseName());
		}

		// Set the loader from the inputFile to the outputDB
		final SparkSession spark = SparkSession.builder().appName("PRoST-Loader").enableHiveSupport().getOrCreate();

		long startTime;
		long executionTime;

		if (settings.isGeneratingTT()) {
			startTime = System.currentTimeMillis();
			final TripleTableLoader tt_loader =
					new TripleTableLoader(settings, spark, statistics);
			tt_loader.load();
			executionTime = System.currentTimeMillis() - startTime;
			logger.info("Time in ms to build the Tripletable: " + executionTime);
			statistics.setHasTT(true);
		}

		if (settings.isGeneratingVP()) {
			startTime = System.currentTimeMillis();
			final VerticalPartitioningLoader vp_loader;
			if (settings.isComputingStatistics()) {
				vp_loader = new VerticalPartitioningLoader(settings, spark, statistics);
			} else {
				vp_loader = new VerticalPartitioningLoader(settings, spark);
			}
			vp_loader.load();
			executionTime = System.currentTimeMillis() - startTime;
			logger.info("Time in ms to build the Vertical partitioning: " + executionTime);
			statistics.setHasVPTables(true);
		}

		if (settings.isGeneratingWPT()) {
			startTime = System.currentTimeMillis();
			final WidePropertyTableLoader pt_loader =
					new WidePropertyTableLoader(settings, spark);
			pt_loader.load();
			executionTime = System.currentTimeMillis() - startTime;
			logger.info("Time in ms to build the Property Table: " + executionTime);
			statistics.setHasWPT(true);
		}

		if (settings.isGeneratingIWPT()) {
			startTime = System.currentTimeMillis();
			final InverseWidePropertyTableLoader iwptLoader =
					new InverseWidePropertyTableLoader(settings, spark);
			iwptLoader.load();
			executionTime = System.currentTimeMillis() - startTime;
			logger.info("Time in ms to build the Inverse Property Table: " + executionTime);
			statistics.setHasIWPT(true);
		}

		if (settings.isGeneratingJWPTOuter()) {
			startTime = System.currentTimeMillis();
			final JoinedWidePropertyTableLoader jwptLoader =
					new JoinedWidePropertyTableLoader(settings, spark, JoinedWidePropertyTableLoader.JoinType.outer);
			jwptLoader.load();
			executionTime = System.currentTimeMillis() - startTime;
			logger.info("Time in ms to build the Joined Property Table (outer join): " + executionTime);
			statistics.setHasJWPT(true);
		}

		if (settings.isGeneratingJWPTInner()) {
			startTime = System.currentTimeMillis();
			final JoinedWidePropertyTableLoader jwptLoader =
					new JoinedWidePropertyTableLoader(settings, spark, JoinedWidePropertyTableLoader.JoinType.inner);
			jwptLoader.load();
			executionTime = System.currentTimeMillis() - startTime;
			logger.info("Time in ms to build the Joined Property Table (inner join): " + executionTime);
			statistics.setHasJWPT(true);
		}

		if (settings.isGeneratingJWPTLeftOuter()) {
			startTime = System.currentTimeMillis();
			final JoinedWidePropertyTableLoader jwptLoader =
					new JoinedWidePropertyTableLoader(settings, spark,
							JoinedWidePropertyTableLoader.JoinType.leftouter);
			jwptLoader.load();
			executionTime = System.currentTimeMillis() - startTime;
			logger.info("Time in ms to build the Joined Property Table (WPT left inner join IWPT): " + executionTime);
			statistics.setHasJWPT(true);
		}

		// save statistics if needed
		if (settings.isComputingStatistics()) {
			statistics.saveToFile(settings.getDatabaseName() + ".json");
		}

		// compute statistics for each table
		computeTableStatistics(spark);

		logger.info("Loader terminated successfully");
	}

	/**
	 * Calculate statistics for each table in the database except the external ones.
	 * Broadcast join is used only when statistics are already computed.
	 */
	private static void computeTableStatistics(final SparkSession spark) {
		@SuppressWarnings("RedundantCast") final Row[] tables =
				(Row[]) spark.sql("SHOW TABLES").select("tableName").collect();
		// skip the external table
		IntStream.range(0, tables.length).filter(i -> !tables[i].getString(0).equals("tripletable_ext"))
				.mapToObj(i -> "ANALYZE TABLE " + tables[i].get(0) + " COMPUTE STATISTICS").forEach(spark::sql);
	}
}
