package utils;

import java.io.File;
import java.util.ArrayList;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.MissingOptionException;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.apache.log4j.Logger;
import org.ini4j.Ini;
import stats.DatabaseStatistics;

/**
 * Loads and validates initialization options for the query-executor component.
 */
public class Settings {
	private static final String DEFAULT_SETTINGS_FILE = "query-executor_default.ini";

	//General settings
	private String settingsPath;
	private String databaseName;
	private String queriesInputPath;
	private String statsPath;
	private String outputFilePath;
	private String benchmarkFilePath;

	// Node types
	private boolean usingTT = false;
	private boolean usingVP = false;
	private boolean usingWPT = false;
	private boolean usingIWPT = false;
	private boolean usingJWPTOuter = false;
	private boolean usingJWPTInner = false;
	private boolean usingJWPTLeftOuter = false;
	private boolean usingEmergentSchema = false;
	private String emergentSchemaPath;

	// Translator options
	private boolean groupingTriples = true;
	private int minGroupSize = 2;
	private boolean usingCharacteristicSets = false;

	// Executor options
	private boolean randomQueryOrder = false;
	private boolean savingBenchmarkFile = false;

	private Settings() {

	}

	public Settings(final String[] args) throws Exception {
		parseArguments(args);
		if (settingsPath == null) {
			settingsPath = DEFAULT_SETTINGS_FILE;
		}
		if (statsPath == null) {
			statsPath = databaseName + ".json";
		}

		final File file = new File(settingsPath);
		if (file.exists()) {
			//noinspection MismatchedQueryAndUpdateOfCollection
			final Ini settings = new Ini(file);
			this.usingTT = settings.get("nodeTypes", "TT", boolean.class);
			this.usingVP = settings.get("nodeTypes", "VP", boolean.class);
			this.usingWPT = settings.get("nodeTypes", "WPT", boolean.class);
			this.usingIWPT = settings.get("nodeTypes", "IWPT", boolean.class);
			this.usingJWPTOuter = settings.get("nodeTypes", "JWPT_outer", boolean.class);
			this.usingJWPTInner = settings.get("nodeTypes", "JWPT_inner", boolean.class);
			this.usingJWPTLeftOuter = settings.get("nodeTypes", "JWPT_leftouter", boolean.class);

			this.groupingTriples = settings.get("translator", "groupingTriples", boolean.class);
			this.minGroupSize = settings.get("translator", "minGroupSize", int.class);
			this.usingCharacteristicSets = settings.get("translator", "usingCharacteristicSets", boolean.class);

			this.randomQueryOrder = settings.get("executor", "randomQueryOrder", boolean.class);
			this.savingBenchmarkFile = settings.get("executor", "savingBenchmarkFile", boolean.class);
		}

		if (savingBenchmarkFile && benchmarkFilePath == null) {
			benchmarkFilePath = createCsvFilename();
		}

		validate();
		printLoggerInformation();
	}

	private void validate() {
		assert databaseName != null && !databaseName.equals("") : "Missing database name";
		assert (usingTT || usingVP || usingWPT || usingIWPT || usingJWPTOuter || usingJWPTLeftOuter)
				: "At least one data model containing all data must be enabled";

		int jwptCounter = 0;
		if (usingJWPTInner) {
			jwptCounter++;
		}
		if (usingJWPTLeftOuter) {
			jwptCounter++;
		}
		if (isUsingJWPTOuter()) {
			jwptCounter++;
		}
		assert (jwptCounter <= 1) : "Only one type of JWPT can be used";
	}

	/**
	 * Checks in the statistics file if the data models enabled in the settings were created in the database.
	 *
	 * @param statistics database statistics file.
	 */
	public void checkTablesAvailability(final DatabaseStatistics statistics) {
		assert (!this.isUsingTT() || statistics.hasTT()) : "TT enabled, but database does not have a TT";
		assert (!this.isUsingVP() || statistics.hasVPTables()) : "VP enabled, but database does not have VP tables";
		assert (!this.isUsingWPT() || statistics.hasWPT()) : "WPT enabled, but database does not have a WPT";
		assert (!this.isUsingIWPT() || statistics.hasIWPT()) : "IWPT enabled, but database does not have a IWPT";
		assert (!this.isUsingJWPTOuter() || statistics.hasJWPTOuter()) : "JWPT_outer enabled, but database does not "
				+ "have a JWPT";
		assert (!this.isUsingJWPTInner() || statistics.hasJWPTInner()) : "JWPT_inner enabled, but database does not "
				+ "have a JWPT";
		assert (!this.isUsingJWPTOuter() || statistics.hasJWPTOuter()) : "JWPT_leftouter enabled, but database does "
				+ "not have a JWPT";
		if (this.isUsingJWPTInner()) {
			assert (isUsingTT() || isUsingVP() || isUsingWPT()
					|| isUsingIWPT() || isUsingJWPTOuter() || isUsingJWPTLeftouter())
					: "JWPT_inner cannot execute all query types by itself";
		}
	}

	private void parseArguments(final String[] args) {
		final CommandLineParser parser = new PosixParser();
		final Options options = new Options();

		final Option databaseOpt = new Option("db", "DB", true, "Database containing the Vertically Partitioned and "
				+ "Property Tables.");
		databaseOpt.setRequired(true);
		options.addOption(databaseOpt);

		final Option inputOpt = new Option("i", "input", true, "Input file/folder with the SPARQL query/queries.");
		inputOpt.setRequired(true);
		options.addOption(inputOpt);

		final Option settingsPathOption = new Option("pref", "preferences", true, "[OPTIONAL] Path to settings "
				+ "profile file. The default initialization file is used by default.");
		settingsPathOption.setRequired(false);
		options.addOption(settingsPathOption);

		final Option outputOpt = new Option("o", "output", true, "[OPTIONAL] Output path for the results in HDFS.");
		options.addOption(outputOpt);

		final Option statOpt = new Option("s", "stats", true, "[OPTIONAL] File with statistics. <databaseName>.stats "
				+ "will be used if no value is provided");
		options.addOption(statOpt);

		final Option emSchemaOpt = new Option("es", "emergentSchema", true, "[OPTIONAL] File with emergent schema, if "
				+ "exists");
		options.addOption(emSchemaOpt);

		final Option csvPathOpt = new Option("bench", "benchmark", true, "[OPTIONAL] Filename to write execution "
				+ "times. ");
		options.addOption(csvPathOpt);

		final Option helpOpt = new Option("h", "help", true, "Print this help.");
		options.addOption(helpOpt);

		final HelpFormatter formatter = new HelpFormatter();
		CommandLine cmd = null;
		try {
			cmd = parser.parse(options, args);
		} catch (final MissingOptionException e) {
			formatter.printHelp("JAR", "Execute a  SPARQL query with Spark", options, "", true);
			System.exit(0);
		} catch (final ParseException e) {
			e.printStackTrace();
		}

		assert cmd != null;
		if (cmd.hasOption("help")) {
			formatter.printHelp("JAR", "Execute a  SPARQL query with Spark", options, "", true);
			System.exit(0);
		}
		if (cmd.hasOption("DB")) {
			databaseName = cmd.getOptionValue("DB");
		}
		if (cmd.hasOption("input")) {
			queriesInputPath = cmd.getOptionValue("input");
		}
		if (cmd.hasOption("preferences")) {
			settingsPath = cmd.getOptionValue("preferences");
		}
		if (cmd.hasOption("output")) {
			outputFilePath = cmd.getOptionValue("output");
		}
		if (cmd.hasOption("stats")) {
			statsPath = cmd.getOptionValue("stats");
		}
		if (cmd.hasOption("emergentSchema")) {
			emergentSchemaPath = cmd.getOptionValue("emergentSchema");
			usingEmergentSchema = true;
		}
		if (cmd.hasOption("benchmark")) {
			benchmarkFilePath = cmd.getOptionValue("benchmark");
			savingBenchmarkFile = true;
		}
	}

	private void printLoggerInformation() {
		final Logger logger = Logger.getLogger("PRoST");

		logger.info("Using preference settings: " + settingsPath);
		logger.info("Database set to: " + databaseName);
		logger.info("Input queries path set to: " + queriesInputPath);
		if (outputFilePath != null) {
			logger.info("Output path set to: " + outputFilePath);
		}
		if (statsPath != null) {
			logger.info("Statistics file path set to: " + statsPath);
		}

		final ArrayList<String> enabledNodeTypes = new ArrayList<>();
		if (usingTT) {
			enabledNodeTypes.add("TT");
		}
		if (usingVP) {
			enabledNodeTypes.add("VP");
		}
		if (usingWPT) {
			enabledNodeTypes.add("WPT");
		}
		if (usingIWPT) {
			enabledNodeTypes.add("IWPT");
		}
		if (usingJWPTOuter) {
			enabledNodeTypes.add("JWPT_outer");
		}
		if (usingJWPTInner) {
			enabledNodeTypes.add("JWPT_inner");
		}
		if (usingJWPTLeftOuter) {
			enabledNodeTypes.add("JWPT_leftouter");
		}
		logger.info("Enabled node types: " + String.join(", ", enabledNodeTypes));

		if (usingEmergentSchema) {
			logger.info("Using emergent schema. Path: " + emergentSchemaPath);
		}

		logger.info("#TRANSLATOR OPTIONS#");
		if (groupingTriples) {
			logger.info("Grouping of triples enabled");
		} else {
			logger.info("Grouping of triples disabled");
		}
		logger.info("Minimum group size: " + minGroupSize);

		logger.info("#EXECUTOR OPTIONS#");
		if (randomQueryOrder) {
			logger.info("Queries execution order: random");
		} else {
			logger.info("Queries execution order: static");
		}
		if (savingBenchmarkFile) {
			logger.info("Saving benchmark file to: " + benchmarkFilePath);
		}
	}

	private String createCsvFilename() {
		final ArrayList<String> csvFilenameElements = new ArrayList<>();
		csvFilenameElements.add(this.getDatabaseName());
		if (usingTT) {
			csvFilenameElements.add("TT");
		}
		if (usingVP) {
			csvFilenameElements.add("VP");
		}
		if (usingWPT) {
			csvFilenameElements.add("WPT");
		}
		if (usingIWPT) {
			csvFilenameElements.add("IWPT");
		}
		if (usingJWPTOuter) {
			csvFilenameElements.add("JWPT_outer");
		}
		if (usingJWPTInner) {
			csvFilenameElements.add("JWPT_inner");
		}
		if (usingJWPTLeftOuter) {
			csvFilenameElements.add("JWPT_leftouter");
		}
		if (usingEmergentSchema) {
			csvFilenameElements.add("EmergentSchema");
		}
		if (!groupingTriples) {
			csvFilenameElements.add("nonGrouped");
		}
		csvFilenameElements.add("minGroup-" + minGroupSize);

		if (randomQueryOrder) {
			csvFilenameElements.add("randomOrder");
		}

		if (usingCharacteristicSets) {
			csvFilenameElements.add("usingCharset");
		}
		return String.join("_", csvFilenameElements) + ".csv";
	}

	public String getDatabaseName() {
		return databaseName;
	}

	public String getQueriesInputPath() {
		return queriesInputPath;
	}

	public String getStatsPath() {
		return statsPath;
	}

	public String getOutputFilePath() {
		return outputFilePath;
	}

	public String getBenchmarkFilePath() {
		return benchmarkFilePath;
	}

	public boolean isUsingTT() {
		return usingTT;
	}

	public boolean isUsingVP() {
		return usingVP;
	}

	public boolean isUsingWPT() {
		return usingWPT;
	}

	public boolean isUsingIWPT() {
		return usingIWPT;
	}

	public boolean isUsingJWPTOuter() {
		return usingJWPTOuter;
	}

	public boolean isUsingJWPTInner() {
		return usingJWPTInner;
	}

	public boolean isUsingJWPTLeftouter() {
		return usingJWPTLeftOuter;
	}

	public boolean isUsingEmergentSchema() {
		return usingEmergentSchema;
	}

	public String getEmergentSchemaPath() {
		return emergentSchemaPath;
	}

	public boolean isGroupingTriples() {
		return groupingTriples;
	}

	public int getMinGroupSize() {
		return minGroupSize;
	}

	public boolean isRandomQueryOrder() {
		return randomQueryOrder;
	}

	public boolean isSavingBenchmarkFile() {
		return savingBenchmarkFile;
	}

	public boolean isUsingCharacteristicSets() {
		return usingCharacteristicSets;
	}

	public static class Builder {
		//General settings
		private String databaseName;
		private String inputPath = "/";
		private String statsPath;

		// Node types
		private boolean usingTT = false;
		private boolean usingVP = false;
		private boolean usingWPT = false;
		private boolean usingIWPT = false;
		private boolean usingJWPTOuter = false;
		private boolean usingJWPTInner = false;
		private boolean usingJWPTLeftOuter = false;


		// Translator options
		private boolean groupingTriples = true;
		private int minGroupSize = 1;
		private boolean usingCharacteriticSets = false;

		public Builder(final String databaseName) {
			this.databaseName = databaseName;
		}

		public Builder withQueriesInputPath(final String inputPath) {
			this.inputPath = inputPath;
			return this;
		}

		public Builder usingTTNodes() {
			this.usingTT = true;
			return this;
		}

		public Builder usingVPNodes() {
			this.usingVP = true;
			return this;
		}

		public Builder usingWPTNodes() {
			this.usingWPT = true;
			return this;
		}

		public Builder usingIWPTNodes() {
			this.usingIWPT = true;
			return this;
		}

		public Builder usingJWPTOuterNodes() {
			this.usingJWPTOuter = true;
			return this;
		}

		public Builder usingJWPTLeftouterNodes() {
			this.usingJWPTLeftOuter = true;
			return this;
		}

		public Builder usingJWPTInnerNodes() {
			this.usingJWPTInner = true;
			return this;
		}

		public Builder withUngroupedTriples() {
			this.groupingTriples = false;
			return this;
		}

		public Builder withMinimumGroupSize(final int minimumGroupSize) {
			this.minGroupSize = minimumGroupSize;
			return this;
		}

		public Builder usingCharacteristicSets(){
			this.usingCharacteriticSets = true;
			return this;
		}

		public Settings build() {
			final Settings settings = new Settings();
			settings.databaseName = this.databaseName;
			settings.queriesInputPath = this.inputPath;
			settings.statsPath = this.databaseName + ".json";
			settings.usingTT = this.usingTT;
			settings.usingVP = this.usingVP;
			settings.usingWPT = this.usingWPT;
			settings.usingIWPT = this.usingIWPT;
			settings.usingJWPTOuter = this.usingJWPTOuter;
			settings.usingJWPTLeftOuter = this.usingJWPTLeftOuter;
			settings.usingJWPTInner = this.usingJWPTInner;
			settings.groupingTriples = this.groupingTriples;
			settings.minGroupSize = this.minGroupSize;
			settings.usingCharacteristicSets = this.usingCharacteriticSets;
			return settings;
		}
	}

}
