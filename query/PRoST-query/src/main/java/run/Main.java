package run;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.MissingOptionException;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.apache.log4j.Logger;

import Executor.Executor;
import JoinTree.JoinTree;
import Translator.Translator;


/**
 * The Main class parses the CLI arguments and calls the translator and the executor
 * 
 * @author Matteo Cossu
 */
public class Main {
	private static String inputFile;
	private static String outputFile;
	private static String statsFileName = "";
	private static String database_name;
	private static final Logger logger = Logger.getLogger(Main.class);
	private static int treeWidth = -1;
	private static boolean useOnlyVP = false;
	private static int setGroupSize = -1;
	
	public static void main(String[] args) {
		
		/*
		 * Manage the CLI options
		 */
		CommandLineParser parser = new PosixParser();
		Options options = new Options();
		Option inputOpt = new Option("i", "input", true, "Input file with the SPARQL query.");
		inputOpt.setRequired(true);
		options.addOption(inputOpt);
		Option outputOpt = new Option("o", "output", true, "Custom results filename.");
		options.addOption(outputOpt);
		Option statOpt = new Option("s", "stats", true, "File with statistics (required)");
		options.addOption(statOpt);
		statOpt.setRequired(true);
		Option databaseOpt = new Option("d", "DB", true, "Database containing the VP tables.");
		databaseOpt.setRequired(true);
		options.addOption(databaseOpt);
		Option helpOpt = new Option("h", "help", true, "Print this help.");
		options.addOption(helpOpt);
		Option widthOpt = new Option("w", "width", true, "The maximum Tree width");
		options.addOption(widthOpt);
		Option propertyTableOpt = new Option("v", "only_vp", false, "Use only Vertical Partitioning");
		options.addOption(propertyTableOpt);
		Option groupsizeOpt = new Option("g", "groupsize", true, "Minimum Group Size for Property Table nodes");
		options.addOption(groupsizeOpt);
		
		HelpFormatter formatter = new HelpFormatter();
		CommandLine cmd = null;
		try {
			cmd = parser.parse(options, args);
		} catch(MissingOptionException e){
			 formatter.printHelp("JAR", "Execute a  SPARQL query with Spark", options, "", true);
			 return;
		} catch (ParseException e) {
			e.printStackTrace();
		}
		
		if(cmd.hasOption("help")){
			formatter.printHelp("JAR", "Execute a  SPARQL query with Spark", options, "", true);
			return;
		}
		if(cmd.hasOption("input")){
			inputFile = cmd.getOptionValue("input");
		}
		if(cmd.hasOption("output")){
			outputFile = cmd.getOptionValue("output");
			logger.info("Output file set to:" + outputFile);
		}
		if(cmd.hasOption("stats")){
			statsFileName = cmd.getOptionValue("stats");
		}
		if(cmd.hasOption("width")){
			treeWidth = Integer.valueOf(cmd.getOptionValue("width"));
			logger.info("Maximum tree width is set to " + String.valueOf(treeWidth));
		}
		if(cmd.hasOption("only_vp")){
			useOnlyVP = true;
			logger.info("Using Vertical Partitioning only.");
		}
		if(cmd.hasOption("groupsize")){
			setGroupSize = Integer.valueOf(cmd.getOptionValue("groupsize"));
			logger.info("Minimum Group Size set to " + String.valueOf(setGroupSize));
		}
		if(cmd.hasOption("DB")){
			database_name = cmd.getOptionValue("DB");
		}
		
		/*
		 * Translation Phase
		 */
		Translator translator = new Translator(inputFile, outputFile, statsFileName, treeWidth);
		if (useOnlyVP) translator.setPropertyTable(true);
		if (setGroupSize > 0) translator.setMinimumGroupSize(setGroupSize);
		JoinTree translatedQuery = translator.translateQuery();
		
		/*
		 * Execution Phase
		 */
		Executor executor = new Executor(translatedQuery, database_name);
		if (outputFile != null) executor.setOutputFile(outputFile); 
		executor.execute();		
		
	}

}
