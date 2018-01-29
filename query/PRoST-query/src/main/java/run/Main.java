package run;

import java.io.File;
import java.util.concurrent.ThreadLocalRandom;

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
	private static boolean usePropertyTable = false;
	private static boolean useReversePropertyTable = false;
	
	private static int setGroupSize = -1;
	private static boolean benchmarkMode = false;
	private static String benchmark_file;
	
	public static void main(String[] args) {
		
		/*
		 * Manage the CLI options
		 */
		CommandLineParser parser = new PosixParser();
		Options options = new Options();
		Option inputOpt = new Option("i", "input", true, "Input file with the SPARQL query.");
		inputOpt.setRequired(true);
		options.addOption(inputOpt);
		Option outputOpt = new Option("o", "output", true, "Path for the results in HDFS.");
		options.addOption(outputOpt);
		Option statOpt = new Option("s", "stats", true, "File with statistics (required)");
		options.addOption(statOpt);
		statOpt.setRequired(true);
		Option databaseOpt = new Option("d", "DB", true, "Database containing the VP tables and the PT.");
		databaseOpt.setRequired(true);
		options.addOption(databaseOpt);
		Option helpOpt = new Option("h", "help", true, "Print this help.");
		options.addOption(helpOpt);
		Option widthOpt = new Option("w", "width", true, "The maximum Tree width");
		options.addOption(widthOpt);
		Option vpOpt = new Option("v", "only_vp", false, "Use only Vertical Partitioning");
		options.addOption(vpOpt);
		Option propertyTableOpt = new Option("p", "property_table", false, "Use Propery Table");
		options.addOption(propertyTableOpt);
		Option reversePropertyTableOpt = new Option("rp", "reverse_property_table", false, "Use Reverse Property Table");
		options.addOption(reversePropertyTableOpt);
		Option benchmarkOpt = new Option("t", "times", true, "Save the time results in a csv file.");
		options.addOption(benchmarkOpt);
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
		if(cmd.hasOption("property_table")){
			usePropertyTable = true;
			logger.info("Using Property Table.");
		}
		if(cmd.hasOption("reverse_property_table")){
			useReversePropertyTable = true;
			logger.info("Using Reverse Property Table.");
		}
		if(cmd.hasOption("groupsize")){
			setGroupSize = Integer.valueOf(cmd.getOptionValue("groupsize"));
			logger.info("Minimum Group Size set to " + String.valueOf(setGroupSize));
		}
		if(cmd.hasOption("DB")){
			database_name = cmd.getOptionValue("DB");
		}
		if(cmd.hasOption("times")){
			benchmarkMode = true;
			benchmark_file = cmd.getOptionValue("times");
		}
		
		File file = new File(inputFile);
		
		// single file
		if(file.isFile()){
			
			// translation phase
			JoinTree translatedQuery = translateSingleQuery(inputFile, statsFileName, treeWidth);
			
			// execution phase
			Executor executor = new Executor(translatedQuery, database_name);
			if (outputFile != null) executor.setOutputFile(outputFile); 
			executor.execute();	
			
		} 
		
		// set of queries
		else if(file.isDirectory()){
			
			// empty executor to initialize Spark
			Executor executor = new Executor(null, database_name);
			
			if(benchmarkMode) {
				//executor.cacheTables();
				executeBatch(random_sample(file.list(), 3), executor);
				executor.clearQueryTimes();
			}
			
			// if the path is a directory execute every files inside
			executeBatch(file.list(), executor);
			
			if(benchmarkMode) {
				executor.saveResultsCsv(benchmark_file);
			}
			
		} else {
			logger.error("The input file is not set correctly or contains errors");
			return;
		}	
	}
	
	private static JoinTree translateSingleQuery(String query, String statsFile, int width) {
		Translator translator = new Translator(query, statsFile, width, database_name);
		if (usePropertyTable) {
			translator.setPropertyTable(true);
		} else if (useReversePropertyTable) {
			translator.setReversePropertyTable(true);
		}
		if (setGroupSize != -1) translator.setMinimumGroupSize(setGroupSize);
		
		return translator.translateQuery();
	}
	
	
	private static void executeBatch(String[] queries, Executor executor) {
		for(String fname : queries){
			logger.info("Starting: " + fname);
			
			// translation phase
			JoinTree translatedQuery = translateSingleQuery(inputFile +  "/" + fname, statsFileName, treeWidth);
			
			// execution phase
			executor.setQueryTree(translatedQuery);
			executor.execute();	
		}
	}
	
	private static String[] random_sample(String[] queries, int k) {
		String[] sample = new String[k];
		for (int i = 0; i < sample.length; i++) {
			int randomIndex = ThreadLocalRandom.current().nextInt(0, queries.length);
			sample[i] = queries[randomIndex];
		}
		return sample;
	}
	

}
