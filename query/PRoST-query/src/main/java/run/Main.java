package run;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
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

import executor.Executor;
import joinTree.JoinTree;
import translator.Translator;
import utils.EmergentSchema;
import utils.Stats;

/**
 * The Main class parses the CLI arguments and calls the translator and the
 * executor.
 *
 * @author Matteo Cossu
 * @author Polina Koleva
 */
public class Main {

	private static final Logger logger = Logger.getLogger("PRoST");

	private static String inputFile;
	private static String outputFile;
	private static String statsFileName = "";
	private static String database_name;

	// TODO remove the tree width if not used
	private static int treeWidth = -1;

	private static String lpStrategies;
	private static boolean useVerticalPartitioning = false;
	private static boolean usePropertyTable = false;
	private static boolean useInversePropertyTable = false;
	private static boolean useJoinedPropertyTable = false;
	// if triples have to be grouped when using property table
	private static boolean disablesGrouping = false;
	private static String emergentSchemaFile = "";
	private static String benchmarkFile = "";
	
	private static String loj4jFileName = "log4j.properties";

	public static void main(final String[] args) throws IOException {
		final InputStream inStream = Main.class.getClassLoader().getResourceAsStream(loj4jFileName);
		final Properties props = new Properties();
		props.load(inStream);
		PropertyConfigurator.configure(props);

		/*
		 * Manage the CLI options
		 */
		final CommandLineParser parser = new PosixParser();
		final Options options = new Options();
		final Option inputOpt = new Option("i", "input", true, "Input file with the SPARQL query.");
		inputOpt.setRequired(true);
		options.addOption(inputOpt);
		final Option outputOpt = new Option("o", "output", true, "Path for the results in HDFS.");
		options.addOption(outputOpt);
		final Option statOpt = new Option("s", "stats", true, "File with statistics (required)");
		options.addOption(statOpt);
		statOpt.setRequired(true);
		final Option emSchemaOpt = new Option("es", "emergentSchema", true, "File with emergent schema, if exists");
		options.addOption(emSchemaOpt);
		final Option databaseOpt = new Option("d", "DB", true, "Database containing the VP tables and the PT.");
		databaseOpt.setRequired(true);
		options.addOption(databaseOpt);
		final Option helpOpt = new Option("h", "help", true, "Print this help.");
		options.addOption(helpOpt);
		final Option widthOpt = new Option("w", "width", true, "The maximum Tree width");
		options.addOption(widthOpt);
		final Option lpOpt = new Option("lp", "logicalPartitionStrategies", true, "Logical Partition Strategy.");
		lpOpt.setRequired(false);
		options.addOption(lpOpt);
		final Option disableGroupingOpt = new Option("dg", "disablesGrouping", false,
				"If triples are not grouped by subject when WPT is used.");
		disableGroupingOpt.setRequired(false);
		options.addOption(disableGroupingOpt);
		final Option randomQueryExecutionOpt = new Option("r", "randomQueryExecution", false, "If queries have to be executed in a random order.");
		randomQueryExecutionOpt.setRequired(false);
		options.addOption(randomQueryExecutionOpt);
		final Option benchmarkOpt = new Option("t", "times", true, "Save the time results in a csv file.");
		options.addOption(benchmarkOpt);

		final HelpFormatter formatter = new HelpFormatter();
		CommandLine cmd = null;
		try {
			cmd = parser.parse(options, args);
		} catch (final MissingOptionException e) {
			formatter.printHelp("JAR", "Execute a  SPARQL query with Spark", options, "", true);
			return;
		} catch (final ParseException e) {
			e.printStackTrace();
		}
		if (cmd.hasOption("help")) {
			formatter.printHelp("JAR", "Execute a  SPARQL query with Spark", options, "", true);
			return;
		}
		if (cmd.hasOption("input")) {
			inputFile = cmd.getOptionValue("input");
		}
		if (cmd.hasOption("output")) {
			outputFile = cmd.getOptionValue("output");
			logger.info("Output file set to:" + outputFile);
		}
		if (cmd.hasOption("stats")) {
			statsFileName = cmd.getOptionValue("stats");
		}
		if (cmd.hasOption("width")) {
			treeWidth = Integer.valueOf(cmd.getOptionValue("width"));
			logger.info("Maximum tree width is set to " + String.valueOf(treeWidth));
		}
		if (cmd.hasOption("DB")) {
			database_name = cmd.getOptionValue("DB");
		}
		if (cmd.hasOption("times")) {
			benchmarkFile = cmd.getOptionValue("times");
		}

		// default if a logical partition is not specified is: WPT, and VP.
		if (!cmd.hasOption("logicalPartitionStrategies")) {
			useVerticalPartitioning = true;
			usePropertyTable = true;
			logger.info("Default strategies used: WPT + VP");
		} else {
			lpStrategies = cmd.getOptionValue("logicalPartitionStrategies");

			final List<String> strategies = Arrays.asList(lpStrategies.split(","));

			if (strategies.contains("VP")) {
				useVerticalPartitioning = true;
				logger.info("Logical strategy used: VP");
			}
			if (strategies.contains("WPT")) {
				usePropertyTable = true;
				logger.info("Logical strategy used: WPT");
			}
			if (strategies.contains("IWPT")) {
				useInversePropertyTable = true;
				logger.info("Logical strategy used: IWPT");

			}
			if (strategies.contains("JWPT")) {
				useJoinedPropertyTable = true;
				logger.info("Logical strategy used: JWPT");
			}
		}

		if (cmd.hasOption("disablesGrouping")) {
			disablesGrouping = true;
			logger.info("Grouping of multiple triples is disabled.");
		}

		// if emergent schema has to be applied
		if (cmd.hasOption("emergentSchema")) {
			logger.info("Emergent schema is used.");
			emergentSchemaFile = cmd.getOptionValue("emergentSchema");
			EmergentSchema.getInstance().readSchema(emergentSchemaFile);
		}

		// create a singleton parsing a file with statistics
		Stats.getInstance().parseStats(statsFileName);
		
		final File file = new File(inputFile);

		// create an executor
		final Executor executor = new Executor(database_name);

		// single file
		if (file.isFile()) {

			// translation phase
			final JoinTree translatedQuery = translateSingleQuery(inputFile, treeWidth);

			// set result file
			if (outputFile != null) {
				executor.setOutputFile(outputFile);
			}
			executor.execute(translatedQuery);

			// if benchmark file is presented, save results
			if (!benchmarkFile.isEmpty()) {
				executor.saveResultsCsv(benchmarkFile);
			}
		} else if (file.isDirectory()) {
			List<String> queryFiles = Arrays.asList(file.list());
			
			// if random order applied, shuffle the queries
			if(cmd.hasOption("randomQueryExecution")){
				logger.info("Executing queries in a random order.");
				Collections.shuffle(queryFiles);
			} else {
				Collections.sort(queryFiles);
			}
			// if the path is a directory execute every files inside
			for (final String fname : queryFiles) {
				logger.info("Starting: " + fname);

				// translation phase
				final JoinTree translatedQuery = translateSingleQuery(inputFile + "/" + fname, treeWidth);

				// execution phase
				executor.execute(translatedQuery);
			}

			// if benchmark file is presented, save results
			if (!benchmarkFile.isEmpty()) {
				executor.saveResultsCsv(benchmarkFile);
			}

		} else {
			logger.error("The input file is not set correctly or contains errors");
			return;
		}
	}

	private static JoinTree translateSingleQuery(final String query, final int width) {
		final Translator translator = new Translator(query, width);
		if (useVerticalPartitioning) {
			translator.setUseVerticalPartitioning(true);
		}
		if (usePropertyTable) {
			translator.setUsePropertyTable(true);
		}
		if (useInversePropertyTable) {
			translator.setUseInversePropertyTable(true);
		}
		if (useJoinedPropertyTable) {
			translator.setUseJoinedPropertyTable(true);
			translator.setUseInversePropertyTable(false);
			translator.setUsePropertyTable(false);
		}
		if (disablesGrouping) {
			translator.setGroupingDisabled(true);
		}
		return translator.translateQuery();
	}
}
