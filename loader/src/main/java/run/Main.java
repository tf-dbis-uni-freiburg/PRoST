package run;
import loader.PropertyTableLoader;
import loader.TripleTableLoader;
import loader.VerticalPartitioningLoader;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.MissingOptionException;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.apache.log4j.Logger;
import org.apache.spark.sql.SparkSession;


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
	private static String input_file;
	private static String outputDB;
	private static final Logger logger = Logger.getLogger(Main.class);
	public static void main(String[] args) {
		
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
		
		Option helpOpt = new Option("h", "help", false, "Print this help.");
		options.addOption(helpOpt);
		
		HelpFormatter formatter = new HelpFormatter();
		CommandLine cmd = null;
		try {
			cmd = parser.parse(options, args);
		} catch(MissingOptionException e){
			 formatter.printHelp("JAR", "Load an RDF graph", options, "", true);
			 return;
		} catch (ParseException e) {
			e.printStackTrace();
		}
		
		if(cmd.hasOption("help")){
			formatter.printHelp("JAR", "Load an RDF graph as Property Table using SparkSQL", options, "", true);
			return;
		}
		if(cmd.hasOption("input")){
			input_file = cmd.getOptionValue("input");
			logger.info("Input path set to: " + input_file);
		}
		if(cmd.hasOption("output")){
			outputDB = cmd.getOptionValue("output");
			logger.info("Output database set to: " + outputDB);
		}
	
		// Set the loader from the inputFile to the outputDB
		//PropertyTableLoader propertyTable = new PropertyTableLoader(input_file, outputDB);
		SparkSession spark = SparkSession
		  .builder()
		  //.enableHiveSupport()
		  .appName("PRoST-Loader")
		  //.config("spark.master", "local")
		  //.config("spark.sql.warehouse.dir", "file:///C:\\Dev\\workspaces\\PRoST\\loader\\sparkwarehouse")
		  .getOrCreate();
		TripleTableLoader tt_loader = new TripleTableLoader(input_file, outputDB, spark);
		tt_loader.load();
		PropertyTableLoader pt_loader = new PropertyTableLoader(input_file, outputDB, spark);
		pt_loader.load();
		VerticalPartitioningLoader vp_loader = new VerticalPartitioningLoader(input_file, outputDB, spark);
		vp_loader.load();		
	}
}
