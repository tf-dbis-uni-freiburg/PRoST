package run;

import java.io.InputStream;
import java.util.Properties;
import java.util.stream.IntStream;

import loader.InverseWidePropertyTable;
import loader.JoinedWidePropertyTable;
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


		// create a statistics writer with statistics enabled
		//StatisticsWriter.getInstance().setUseStatistics(settings.isComputingStatistics());
		DatabaseStatistics statistics = new DatabaseStatistics(settings.getDatabaseName());

		// Set the loader from the inputFile to the outputDB
		final SparkSession spark = SparkSession.builder().appName("PRoST-Loader").enableHiveSupport().getOrCreate();

		// Removing previous instances of the database in case a database with
		// the same name already exists.
		// In this case a new database with the same name will be created.
		// TODO keep database and only load missing tables
		spark.sql("DROP DATABASE IF EXISTS " + settings.getDatabaseName() + " CASCADE");

		long startTime;
		long executionTime;

		//TODO loader constructor should receive settings object
		if (settings.isGeneratingTT()) {
			startTime = System.currentTimeMillis();
			final TripleTableLoader tt_loader =
					new TripleTableLoader(settings.getInputPath(), settings.getDatabaseName(), spark,
							settings.isTtPartitionedBySubject(), settings.isTtPartitionedByPredicate(),
							settings.isDroppingDuplicateTriples(), settings.isComputingCharacteristicSets(),
							statistics);
			tt_loader.load();
			executionTime = System.currentTimeMillis() - startTime;
			logger.info("Time in ms to build the Tripletable: " + executionTime);
		}

		if (settings.isGeneratingWPT()) {
			startTime = System.currentTimeMillis();
			final WidePropertyTableLoader pt_loader =
					new WidePropertyTableLoader(settings.getDatabaseName(), spark,
							settings.isWptPartitionedBySubject());
			pt_loader.load();
			executionTime = System.currentTimeMillis() - startTime;
			logger.info("Time in ms to build the Property Table: " + executionTime);
		}

		if (settings.isGeneratingIWPT()) {
			startTime = System.currentTimeMillis();
			final InverseWidePropertyTable iwptLoader = new InverseWidePropertyTable(settings.getDatabaseName(), spark,
					settings.isWptPartitionedBySubject());
			iwptLoader.load();
			executionTime = System.currentTimeMillis() - startTime;
			logger.info("Time in ms to build the Inverse Property Table: " + executionTime);
		}

		if (settings.isGeneratingJWPT()) {
			startTime = System.currentTimeMillis();
			final JoinedWidePropertyTable jwptLoader = new JoinedWidePropertyTable(settings.getDatabaseName(), spark,
					settings.isWptPartitionedBySubject());
			jwptLoader.load();
			executionTime = System.currentTimeMillis() - startTime;
			logger.info("Time in ms to build the Joined Property Table: " + executionTime);
		}

		if (settings.isGeneratingVP()) {
			startTime = System.currentTimeMillis();
			final VerticalPartitioningLoader vp_loader;
			if (settings.isComputingStatistics()) {
				vp_loader = new VerticalPartitioningLoader(settings.getDatabaseName(), spark,
								settings.isVpPartitionedBySubject(), statistics);
			} else {
				vp_loader = new VerticalPartitioningLoader(settings.getDatabaseName(), spark,
								settings.isVpPartitionedBySubject());
			}
			vp_loader.load();
			executionTime = System.currentTimeMillis() - startTime;
			logger.info("Time in ms to build the Vertical partitioning: " + executionTime);
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
