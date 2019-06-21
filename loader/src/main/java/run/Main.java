package run;

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
		final SparkSession spark = SparkSession.builder().appName("PRoST-Loader").enableHiveSupport().getOrCreate();

		final InputStream inStream = Main.class.getClassLoader().getResourceAsStream(loj4jFileName);
		final Properties props = new Properties();
		props.load(inStream);
		PropertyConfigurator.configure(props);

		final Settings settings = new Settings(args);
		final DatabaseStatistics statistics;
		if (settings.isDroppingDB()) {
			spark.sql("DROP DATABASE " + settings.getDatabaseName() + " CASCADE");
			statistics = new DatabaseStatistics(settings.getDatabaseName());
		} else {
			statistics = DatabaseStatistics.loadFromFile(settings.getDatabaseName() + ".json");
		}

		long startTime;
		long executionTime;

		if (settings.isGeneratingTT()) {
			statistics.setHasTT(false);
			statistics.saveToFile(settings.getDatabaseName() + ".json");

			logger.info("LOADING TT...");
			startTime = System.currentTimeMillis();
			final TripleTableLoader tt_loader =
					new TripleTableLoader(settings, spark, statistics);
			tt_loader.load();
			executionTime = System.currentTimeMillis() - startTime;
			logger.info("TT LOADED!");
			logger.info("Time in ms to build the Tripletable: " + executionTime);

			statistics.setHasTT(true);
			statistics.setTtPartitionedBySubject(settings.isTtPartitionedBySubject());
			statistics.setTtPartitionedByPredicate(settings.isTtPartitionedByPredicate());
			statistics.saveToFile(settings.getDatabaseName() + ".json");
		}

		if (settings.isComputingCharacteristicSets()) {
			logger.info("COMPUTING CHARACTERISTIC SETS...");
			startTime = System.currentTimeMillis();
			statistics.computeCharacteristicSetsStatistics(spark);
			executionTime = System.currentTimeMillis() - startTime;
			logger.info("CHARACTERISTIC SETS COMPUTED!");
			logger.info("Time in ms to compute characteristic sets: " + executionTime);
			statistics.saveToFile(settings.getDatabaseName() + ".json");
		}

		if (settings.isGeneratingVP()) {
			statistics.setHasVPTables(false);
			statistics.saveToFile(settings.getDatabaseName() + ".json");

			logger.info("LOADING VP TABLES...");
			startTime = System.currentTimeMillis();
			final VerticalPartitioningLoader vp_loader = new VerticalPartitioningLoader(settings, spark, statistics);

			vp_loader.load();
			executionTime = System.currentTimeMillis() - startTime;
			logger.info("VP TABLES LOADED!");
			logger.info("Time in ms to build the Vertical partitioning: " + executionTime);

			statistics.setHasVPTables(true);
			statistics.setVpPartitionedBySubject(settings.isVpPartitionedBySubject());
			statistics.saveToFile(settings.getDatabaseName() + ".json");
		}

		if (settings.isComputingPropertyStatistics()) {
			logger.info("COMPUTING PROPERTY STATISTICS...");
			startTime = System.currentTimeMillis();
			statistics.computePropertyStatistics(spark);
			executionTime = System.currentTimeMillis() - startTime;
			logger.info("PROPERTY STATISTICS COMPUTED!");
			logger.info("Time in ms to compute property statistics: " + executionTime);
			statistics.saveToFile(settings.getDatabaseName() + ".json");
		}


		if (settings.isGeneratingWPT()) {
			statistics.setHasWPT(false);
			statistics.saveToFile(settings.getDatabaseName() + ".json");

			logger.info("LOADING WPT...");
			startTime = System.currentTimeMillis();
			final WidePropertyTableLoader wptLoader =
					new WidePropertyTableLoader(settings, spark, statistics);
			wptLoader.load();
			executionTime = System.currentTimeMillis() - startTime;
			logger.info("WPT LOADED!");
			logger.info("Time in ms to build the Property Table: " + executionTime);
			statistics.setHasWPT(true);
			statistics.setWptPartitionedBySubject(settings.isWptPartitionedBySubject());
			statistics.saveToFile(settings.getDatabaseName() + ".json");
		}

		if (settings.isGeneratingIWPT()) {
			statistics.setHasIWPT(false);
			statistics.saveToFile(settings.getDatabaseName() + ".json");

			logger.info("LOADING IWPT...");
			startTime = System.currentTimeMillis();
			final InverseWidePropertyTableLoader iwptLoader =
					new InverseWidePropertyTableLoader(settings, spark, statistics);
			iwptLoader.load();
			executionTime = System.currentTimeMillis() - startTime;
			logger.info("IWPT LOADED!");
			logger.info("Time in ms to build the Inverse Property Table: " + executionTime);

			statistics.setHasIWPT(true);
			statistics.setIwptPartitionedByObject(settings.isIwptPartitionedByObject());
			statistics.saveToFile(settings.getDatabaseName() + ".json");
		}

		if (settings.isGeneratingJWPTOuter()) {
			assert statistics.getProperties().size() > 0 : "Property statistics are needed to compute JWPT";

			statistics.setHasJWPTOuter(false);
			statistics.saveToFile(settings.getDatabaseName() + ".json");

			logger.info("LOADING JWPT_OUTER...");

			startTime = System.currentTimeMillis();
			final JoinedWidePropertyTableLoader jwptLoader =
					new JoinedWidePropertyTableLoader(settings, spark, JoinedWidePropertyTableLoader.JoinType.outer,
							statistics);
			jwptLoader.load();
			executionTime = System.currentTimeMillis() - startTime;
			logger.info("JWPT_OUTER LOADED!");
			logger.info("Time in ms to build the Joined Property Table (outer join): " + executionTime);

			statistics.setHasJWPTOuter(true);
			statistics.setJwptPartitionedByResource(settings.isJwptPartitionedByResource());
			statistics.saveToFile(settings.getDatabaseName() + ".json");
		}

		if (settings.isGeneratingJWPTInner()) {
			assert statistics.getProperties().size() > 0 : "Property statistics are needed to compute JWPT";

			statistics.setHasJWPTInner(false);
			statistics.saveToFile(settings.getDatabaseName() + ".json");

			logger.info("LOADING JWPT_INNER...");
			startTime = System.currentTimeMillis();
			final JoinedWidePropertyTableLoader jwptLoader =
					new JoinedWidePropertyTableLoader(settings, spark, JoinedWidePropertyTableLoader.JoinType.inner,
							statistics);
			jwptLoader.load();
			executionTime = System.currentTimeMillis() - startTime;
			logger.info("JWPT_INNER LOADED!");
			logger.info("Time in ms to build the Joined Property Table (inner join): " + executionTime);

			statistics.setHasJWPTInner(true);
			statistics.setJwptPartitionedByResource(settings.isJwptPartitionedByResource());
			statistics.saveToFile(settings.getDatabaseName() + ".json");
		}

		if (settings.isGeneratingJWPTLeftOuter()) {
			assert statistics.getProperties().size() > 0 : "Property statistics are needed to compute JWPT";

			statistics.setHasJWPTLeftOuter(false);
			statistics.saveToFile(settings.getDatabaseName() + ".json");

			logger.info("LOADING JWPT_LEFTOUTER...");
			startTime = System.currentTimeMillis();
			final JoinedWidePropertyTableLoader jwptLoader =
					new JoinedWidePropertyTableLoader(settings, spark,
							JoinedWidePropertyTableLoader.JoinType.leftouter, statistics);
			jwptLoader.load();
			executionTime = System.currentTimeMillis() - startTime;
			logger.info("JWPT_LEFTOUTER LOADED!");
			logger.info("Time in ms to build the Joined Property Table (WPT left inner join IWPT): " + executionTime);

			statistics.setHasJWPTLeftOuter(true);
			statistics.setJwptPartitionedByResource(settings.isJwptPartitionedByResource());
			statistics.saveToFile(settings.getDatabaseName() + ".json");
		}

		logger.info("Statistics file: : " + settings.getDatabaseName() + ".json");

		// compute spark statistics for each table
		computeTableStatistics(spark);

		logger.info("LOADER TERMINATED SUCCESSFULLY");
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
