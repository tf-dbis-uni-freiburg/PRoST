import java.io.FileNotFoundException;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.MissingOptionException;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.log4j.Logger;


/**
 * The Main class parses the CLI arguments and calls the executor.
 * 
 * Options: 
 * -h, --help prints the usage help message.
 * -i, --input <file> HDFS input path of the RDF graph.
 * -o, --output <DBname> output database name.
 * 
 * @author Matteo Cossu
 */
public class Main {
	private static String inputFile;
	private static String outputDB;
	private static final Logger logger = Logger.getLogger(Main.class);
	public static void main(String[] args) {
		
		/*
		 * Manage the CLI options
		 */
		CommandLineParser parser = new DefaultParser();
		Options options = new Options();
		
		Option inputOpt = new Option("i", "input", true, "HDFS input path of the RDF graph.");
		inputOpt.setRequired(true);
		options.addOption(inputOpt);
		
		Option outputOpt = new Option("o", "output", true, "Output database name.");
		outputOpt.setRequired(true);
		options.addOption(outputOpt);
		
		Option helpOpt = new Option("h", "help", false, "Print this help.");
		options.addOption(helpOpt);
		
		HelpFormatter formatter = new HelpFormatter();
		CommandLine cmd = null;
		try {
			cmd = parser.parse(options, args);
		} catch(MissingOptionException e){
			 formatter.printHelp("JAR", "Execute a JoinTre on Spark", options, "", true);
			 return;
		} catch (ParseException e) {
			e.printStackTrace();
		}
		
		if(cmd.hasOption("help")){
			formatter.printHelp("JAR", "Load a RDF graph as Property Table using SparkSQL", options, "", true);
			return;
		}
		if(cmd.hasOption("input")){
			inputFile = cmd.getOptionValue("input");
			logger.info("Input path set to: " + inputFile);
		}
		if(cmd.hasOption("output")){
			outputDB = cmd.getOptionValue("output");
			logger.info("Output database set to: " + outputDB);
		}
	
		// Set the loader from the inputFile to the outputDB
		PropertyTableLoader propertyTable = new PropertyTableLoader(inputFile, outputDB);
		try {
			propertyTable.load();
		} catch (FileNotFoundException e) {
			logger.error("The input HDFS path does not exist: " + inputFile);
		}
		
	}


}
