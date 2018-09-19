package run;

import java.io.InputStream;
import java.util.Properties;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.MissingOptionException;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.apache.spark.sql.SparkSession;

import loader.TripleTableLoader;
import loader.VerticalPartitioningLoader;
import loader.WidePropertyTableLoader;

/**
 * The Main class parses the CLI arguments and calls the executor.
 * <p>
 * Options: -h, --help prints the usage help message. -i, --input <file> HDFS input path of the RDF graph. -o, --output
 * <DBname> output database name. -s, compute statistics
 *
 * @author Matteo Cossu
 */
public class Main {
	private static String input_location;
	private static String outputDB;
	private static String lpStrategies;
	private static String loj4jFileName = "log4j.properties";
	private static final Logger logger = Logger.getLogger("PRoST");
	private static boolean useStatistics = false;
	private static boolean generateTT = true;
	private static boolean generateWPT = false;
	private static boolean generateVP = false;
	private static boolean generateIWPT = false;
	private static boolean ttPartitionedByPred = false;
	private static boolean ttPartitionedBySub = false;
	private static boolean wptPartitionedBySub = false;

	public static void main(final String[] args) throws Exception {
		final InputStream inStream = Main.class.getClassLoader().getResourceAsStream(loj4jFileName);
		final Properties props = new Properties();
		props.load(inStream);
		PropertyConfigurator.configure(props);

		/*
		 * Manage the CLI options
		 */
		final CommandLineParser parser = new PosixParser();
		final Options options = new Options();

		final Option inputOpt = new Option("i", "input", true, "HDFS input path of the RDF graph.");
		inputOpt.setRequired(true);
		options.addOption(inputOpt);

		final Option outputOpt = new Option("o", "output", true, "Output database name.");
		outputOpt.setRequired(true);
		options.addOption(outputOpt);

		final Option lpOpt = new Option("lp", "logicalPartitionStrategies", true, "Logical Partition Strategy.");
		lpOpt.setRequired(false);
		options.addOption(lpOpt);

		final Option helpOpt = new Option("h", "help", false, "Print this help.");
		options.addOption(helpOpt);

		final Option ttpPartPredOpt = new Option("ttp", "ttPartitionedByPredicate", false,
				"To physically partition the Triple Table by predicate.");
		ttpPartPredOpt.setRequired(false);
		options.addOption(ttpPartPredOpt);

		final Option ttpPartSubOpt =
				new Option("tts", "ttPartitionedBySub", false, "To physically partition the Triple Table by subject.");
		ttpPartSubOpt.setRequired(false);
		options.addOption(ttpPartSubOpt);

		final Option wptPartSubOpt = new Option("wpts", "wptPartitionedBySub", false,
				"To physically partition the Wide Property Table by subject.");
		wptPartSubOpt.setRequired(false);
		options.addOption(wptPartSubOpt);

		final Option statsOpt = new Option("s", "stats", false, "Flag to produce the statistics");
		options.addOption(statsOpt);

		final HelpFormatter formatter = new HelpFormatter();
		CommandLine cmd = null;
		try {
			cmd = parser.parse(options, args);
		} catch (final MissingOptionException e) {
			formatter.printHelp("JAR", "Load an RDF graph", options, "", true);
			return;
		} catch (final ParseException e) {
			e.printStackTrace();
		}

		if (cmd.hasOption("help")) {
			formatter.printHelp("JAR", "Load an RDF graph as Property Table using SparkSQL", options, "", true);
			return;
		}
		if (cmd.hasOption("input")) {
			input_location = cmd.getOptionValue("input");
			logger.info("Input path set to: " + input_location);
		}
		if (cmd.hasOption("output")) {
			outputDB = cmd.getOptionValue("output");
			logger.info("Output database set to: " + outputDB);
		}
		// default if a logical partition is not specified only the TT is
		// generated.
		if (!cmd.hasOption("logicalPartitionStrategies")) {
			generateTT = true;
			logger.info("Logical strategy used: TT + WPT + VP");
		} else {
			lpStrategies = cmd.getOptionValue("logicalPartitionStrategies");
			if (lpStrategies.contains("TT")) {
				generateTT = true;
				logger.info("Logical strategy used: TT");
			}
			if (lpStrategies.contains("WPT")) {
				if (generateTT == false) {
					generateTT = true;
					logger.info(
							"Logical strategy activated: TT (mandatory for WPT) with default physical partitioning");
				}
				logger.info("Logical strategy used: WPT");
				generateWPT = true;
			}
			if (lpStrategies.contains("VP")) {
				if (generateTT == false) {
					generateTT = true;
					logger.info("Logical strategy activated: TT (mandatory for VP) with default physical partitioning");
				}
				generateVP = true;
				logger.info("Logical strategy used: VP");
			}
			if (lpStrategies.contains("IWPT")) {
				if (generateTT == false) {
					generateTT = true;
					logger.info(
							"Logical strategy activated: TT (mandatory for IWPT) with default physical partitioning");
				}
				logger.info("Logical strategy used: IWPT");
				generateIWPT = true;
			}
		}

		if (cmd.hasOption("ttPartitionedByPredicate") && cmd.hasOption("ttPartitionedBySub")) {
			logger.error("Triple table cannot be partitioned by both subject and predicate.");
			return;
		}

		if (cmd.hasOption("ttPartitionedByPredicate")) {
			ttPartitionedByPred = true;
			logger.info("Triple Table will be partitioned by predicate.");
		}
		if (cmd.hasOption("ttPartitionedBySub")) {
			ttPartitionedBySub = true;
			logger.info("Triple Table will be partitioned by subject.");
		}
		if (cmd.hasOption("wptPartitionedBySub")) {
			wptPartitionedBySub = true;
			logger.info("Wide Property Table will be partitioned by subject.");
		}
		if (cmd.hasOption("stats")) {
			useStatistics = true;
			logger.info("Statistics active!");
		}

		// Set the loader from the inputFile to the outputDB
		final SparkSession spark = SparkSession.builder().appName("PRoST-Loader").enableHiveSupport().getOrCreate();

		// Removing previous instances of the database in case a database with
		// the same
		// name already exists.
		// In this case a new dataset with the same name will be created.
		spark.sql("DROP DATABASE IF EXISTS " + outputDB + " CASCADE");

		long startTime;
		long executionTime;

		if (generateTT) {
			startTime = System.currentTimeMillis();
			final TripleTableLoader tt_loader =
					new TripleTableLoader(input_location, outputDB, spark, ttPartitionedBySub, ttPartitionedByPred);
			tt_loader.load();
			executionTime = System.currentTimeMillis() - startTime;
			logger.info("Time in ms to build the Tripletable: " + String.valueOf(executionTime));
		}

		if (generateWPT) {
			startTime = System.currentTimeMillis();
			final WidePropertyTableLoader pt_loader =
					new WidePropertyTableLoader(input_location, outputDB, spark, wptPartitionedBySub);
			pt_loader.load();
			executionTime = System.currentTimeMillis() - startTime;
			logger.info("Time in ms to build the Property Table: " + String.valueOf(executionTime));
		}

		if (generateIWPT) {
			startTime = System.currentTimeMillis();
			final WidePropertyTableLoader ipt_loader =
					new WidePropertyTableLoader(input_location, outputDB, spark, wptPartitionedBySub, true);
			ipt_loader.load();
			executionTime = System.currentTimeMillis() - startTime;
			logger.info("Time in ms to build the Inverse Property Table: " + String.valueOf(executionTime));
		}

		if (generateVP) {
			startTime = System.currentTimeMillis();
			final VerticalPartitioningLoader vp_loader =
					new VerticalPartitioningLoader(input_location, outputDB, spark, useStatistics);
			vp_loader.load();
			executionTime = System.currentTimeMillis() - startTime;
			logger.info("Time in ms to build the Vertical partitioning: " + String.valueOf(executionTime));
		}
	}
}
