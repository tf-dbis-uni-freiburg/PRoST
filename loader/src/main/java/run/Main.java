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
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;


/**
 * The Main class parses the CLI arguments and calls the executor.
 * <p>
 * Options:
 * -h, --help prints the usage help message.
 * -i, --input <file> HDFS input path of the RDF graph.
 * -o, --output <DBname> output database name.
 * -s, compute statistics
 *
 * @author Matteo Cossu
 */
public class Main {
    private static String input_file;
    private static String outputDB;
    private static final Logger logger = Logger.getLogger(Main.class);
    private static boolean useStatistics = false;

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
            input_file = cmd.getOptionValue("input");
            logger.info("Input path set to: " + input_file);
        }
        if (cmd.hasOption("output")) {
            outputDB = cmd.getOptionValue("output");
            logger.info("Output database set to: " + outputDB);
        }
        if (cmd.hasOption("stats")) {
            useStatistics = true;
            logger.info("Statistics active!");
        }

        // Set the loader from the inputFile to the outputDB
        SparkSession spark = SparkSession
                .builder()
                .appName("PRoST-Loader")
                .getOrCreate();

        long startTime;
        long executionTime;

        startTime = System.currentTimeMillis();
        TripleTableLoader tt_loader = new TripleTableLoader(input_file, outputDB, spark);
        tt_loader.load();
        executionTime = System.currentTimeMillis() - startTime;
        System.out.println("Time Tripletable: " + String.valueOf(executionTime));

        startTime = System.currentTimeMillis();
        PropertyTableLoader pt_loader = new PropertyTableLoader(input_file, outputDB, spark);
        pt_loader.load();
        executionTime = System.currentTimeMillis() - startTime;
        System.out.println("Time Property Table: " + String.valueOf(executionTime));

        startTime = System.currentTimeMillis();
        VerticalPartitioningLoader vp_loader = new VerticalPartitioningLoader(input_file, outputDB, spark, useStatistics);
        vp_loader.load();
        executionTime = System.currentTimeMillis() - startTime;
        System.out.println("Time Vertical partitioning: " + String.valueOf(executionTime));

    }


}
