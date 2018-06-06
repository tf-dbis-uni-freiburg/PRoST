package run;

import loader.TripleTableLoader;
import loader.VerticalPartitioningLoader;
import loader.WidePropertyTableLoader;

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

/**
 * The Main class parses the CLI arguments and calls the executor.
 * <p>
 * Options: -h, --help prints the usage help message. -i, --input <file> HDFS
 * input path of the RDF graph. -o, --output <DBname> output database name. -s,
 * compute statistics
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

	public static void main(String[] args) throws Exception {
		InputStream inStream = Main.class.getClassLoader().getResourceAsStream(loj4jFileName);
		Properties props = new Properties();
		props.load(inStream);
		PropertyConfigurator.configure(props);
		boolean generateWPT = false;
		boolean generateVP = false;

		/*
		 * Manage the CLI options
		 */
		CommandLineParser parser = new PosixParser();
		Options options = new Options();

		Option inputOpt = new Option("i", "input", true, "HDFS input path of the RDF graph.");
		inputOpt.setRequired(true);
		options.addOption(inputOpt);

		Option outputOpt = new Option("o", "output", true, "Output database name.");
		outputOpt.setRequired(true);
		options.addOption(outputOpt);
		
		Option lpOpt = new Option("lp", "logicalPartitionStrategies", true, "Logical Partition Strategy. ");
		lpOpt.setRequired(false);
		options.addOption(lpOpt);

		Option helpOpt = new Option("h", "help", false, "Print this help.");
		options.addOption(helpOpt);

		Option statsOpt = new Option("s", "stats", false, "Flag to produce the statistics");
		options.addOption(statsOpt);

		HelpFormatter formatter = new HelpFormatter();
		CommandLine cmd = null;
		try {
			cmd = parser.parse(options, args);
		} catch (MissingOptionException e) {
			formatter.printHelp("JAR", "Load an RDF graph", options, "", true);
			return;
		} catch (ParseException e) {
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
		//default if a logical partition is not specified is to consider all three strategies.
		if (!cmd.hasOption("logicalPartitionStrategy")) {
			generateWPT = true;
			generateVP = true;
			logger.info("Logical strategy used: TT + WPT + VP");
		} else {
			logger.info("Logical strategy used: TT (mandatory)");
			lpStrategies = cmd.getOptionValue("logicalPartitionStrategies");
			if (lpStrategies.contains("WPT")) {
				generateWPT = true;
				logger.info("Logical strategy used: WPT");				
			}
			if (lpStrategies.contains("VP")) {
				generateVP = true;
				logger.info("Logical strategy used: VP");
			}
		}
		if (cmd.hasOption("stats")) {
			useStatistics = true;
			logger.info("Statistics active!");
		}

		// Set the loader from the inputFile to the outputDB
		SparkSession spark = SparkSession.builder().appName("PRoST-Loader").enableHiveSupport().getOrCreate();

		// Removing previous instances of the database in case a database with the same
		// name already exists.
		// In this case a new dataset with the same name will be created.
		spark.sql("DROP DATABASE IF EXISTS " + outputDB + " CASCADE");

		long startTime;
		long executionTime;

		startTime = System.currentTimeMillis();
		TripleTableLoader tt_loader = new TripleTableLoader(input_location, outputDB, spark);
		tt_loader.load();
		executionTime = System.currentTimeMillis() - startTime;
		logger.info("Time in ms to build the Tripletable: " + String.valueOf(executionTime));

		if (generateWPT) {
			startTime = System.currentTimeMillis();
			WidePropertyTableLoader pt_loader = new WidePropertyTableLoader(input_location, outputDB, spark);
			pt_loader.load();
			executionTime = System.currentTimeMillis() - startTime;
			logger.info("Time in ms to build the Property Table: " + String.valueOf(executionTime));
		}
		
		if (generateVP) {
			startTime = System.currentTimeMillis();
			VerticalPartitioningLoader vp_loader = new VerticalPartitioningLoader(input_location, outputDB, spark,
				useStatistics);
			vp_loader.load();
			executionTime = System.currentTimeMillis() - startTime;
			logger.info("Time in ms to build the Vertical partitioning: " + String.valueOf(executionTime));
		}
	}
}
