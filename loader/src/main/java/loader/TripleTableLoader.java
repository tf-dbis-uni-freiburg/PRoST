package loader;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;


/**
 * Class that constructs the triple table. It is created as external table, so it can use the input file
 * directly without loosing time to replicate the data, since the triple table will be used only for other
 * models e.g. Property Table, Vertical Partitioning.
 *
 * @author Matteo Cossu
 */
public class TripleTableLoader extends Loader {

    public TripleTableLoader(String hdfs_input_directory, String database_name,
                             SparkSession spark) {
        super(hdfs_input_directory, database_name, spark);
    }

    @Override
    public void load() {
    	logger.info("PHASE 1: loading all triples to a generic table...");
        String createTripleTable = String.format(
                "CREATE EXTERNAL TABLE IF NOT EXISTS %s(%s STRING, %s STRING, %s STRING) ROW FORMAT  SERDE"
                        + "'org.apache.hadoop.hive.serde2.RegexSerDe'  WITH SERDEPROPERTIES "
                        + "( \"input.regex\" = \"(\\\\S+)\\\\s+(\\\\S+)\\\\s+(.*)\")"
                        + "LOCATION '%s'",
                name_tripletable, column_name_subject, column_name_predicate, column_name_object, hdfs_input_directory);

        spark.sql(createTripleTable);
        logger.info("Created tripletable with " + createTripleTable);
        checkPropertiesOfTripleTable();
    }
    
    public void checkPropertiesOfTripleTable() {
    	logger.info("DESCRIBE EXTENDED: " + spark.sql("DESCRIBE EXTENDED " + name_tripletable));
    	long numLoadedTriples = spark.sql(String.format("SELECT * FROM %s", name_tripletable)).count();
    	logger.info("Number of raw triples loaded: " + numLoadedTriples);
    	String queryFailedSubjects = String.format("SELECT * FROM %s WHERE %s IS NULL", name_tripletable, column_name_subject);
    	long numFailedSubjects = spark.sql(queryFailedSubjects).count();
    	logger.info("Number of lines in which subject is null: " + numFailedSubjects);
    	String queryFailedPredicates = String.format("SELECT * FROM %s WHERE %s IS NULL", name_tripletable, column_name_predicate);
    	long numFailedPredicates = spark.sql(queryFailedPredicates).count();
    	logger.info("Number of lines in which predicate is null: " + numFailedPredicates);
    	String queryFailedObjects = String.format("SELECT * FROM %s WHERE %s IS NULL", name_tripletable, column_name_object);
    	long numFailedObjects = spark.sql(queryFailedObjects).count();
    	logger.info("Number of lines in which predicate is null: " + numFailedObjects);
    	if ((numFailedSubjects + numFailedPredicates + numFailedObjects) >0 ) {
    		logger.error("This dataset has some problems: " + numFailedObjects);
    		correctTripleTables();
    	}	
    }
    
    //TODO
    public void correctTripleTables(){
    	logger.info("The table could not upload all triples from HDFS. Attempting to remove corrupted triples.");
    }

    // this method exists for the sake of clarity instead of a constant String
    // Therefore, it should be called only once
    private static String build_triple_regex() {
        String uri_s = "<(?:[^:]+:[^\\s\"<>]+)>";
        String literal_s = "\"(?:[^\"\\\\]*(?:\\.[^\"\\\\]*)*)\"(?:@([a-z]+(?:-[a-zA-Z0-9]+)*)|\\^\\^" + uri_s + ")?";
        String subject_s = "(" + uri_s + "|" + literal_s + ")";
        String predicate_s = "(" + uri_s + ")";
        String object_s = "(" + uri_s + "|" + literal_s + ")";
        String space_s = "[ \t]+";
        return "[ \\t]*" + subject_s + space_s + predicate_s + space_s + object_s + "[ \\t]*\\.*[ \\t]*(#.*)?";
    }

}
