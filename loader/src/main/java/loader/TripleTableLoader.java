package loader;

import java.util.List;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import static org.apache.spark.sql.functions.regexp_replace;


/**
 * Class that constructs the triple table. It is created as external table, so
 * it can use the input file directly without loosing time to replicate the
 * data, since the triple table will be used only for other models e.g. Property
 * Table, Vertical Partitioning.
 *
 * @author Matteo Cossu
 * @author Victor Anthony Arrascue Ayala
 */
public class TripleTableLoader extends Loader {

	public TripleTableLoader(String hdfs_input_directory, String database_name, SparkSession spark) {
		super(hdfs_input_directory, database_name, spark);
	}

	@Override
	public void load() throws Exception {
		logger.info("PHASE 1: loading all triples to a generic table...");
		String createTripleTable = String.format(
				"CREATE EXTERNAL TABLE IF NOT EXISTS %s(%s STRING, %s STRING, %s STRING) ROW FORMAT  SERDE"
						+ "'org.apache.hadoop.hive.serde2.RegexSerDe'  WITH SERDEPROPERTIES "
						+ "( \"input.regex\" = \"(\\\\S+)\\\\s+(\\\\S+)\\\\s+(.*)\")" + "LOCATION '%s'",
				name_tripletable, column_name_subject, column_name_predicate, column_name_object, hdfs_input_directory);

		spark.sql(createTripleTable);
		logger.info("Created tripletable with " + createTripleTable);
		repairCorruptedTriples();
	}

	/**
	 * Searches for different kinds of corrupted triples and removes them.
	 * @throws Exception 
	 */
	public void repairCorruptedTriples() throws Exception {
		String queryDropTempTable = String.format("DROP TABLE IF EXISTS %s", name_tripletable+"_temp");
		spark.sql(queryDropTempTable);
		String queryDropFixedTable = String.format("DROP TABLE IF EXISTS %s", name_tripletable+"_fixed");
		spark.sql(queryDropFixedTable);
		
		Dataset<Row> allTriples = null;
		Dataset<Row> triplesWithNulls = null;
		Dataset<Row> triplesWithDotRes = null;
		Dataset<Row> objectsWithMultipleItems = null;
		
		long numLoadedTriples = spark.sql(String.format("SELECT * FROM %s", name_tripletable)).count();
		logger.info("Number of raw triples loaded: " + numLoadedTriples);
		String queryFailedSubjects = String.format("SELECT * FROM %s WHERE %s IS NULL", name_tripletable,
			column_name_subject);
		long numFailedSubjects = spark.sql(queryFailedSubjects).count();
		logger.info("Number of lines in which subject is null: " + numFailedSubjects);
		String queryFailedPredicates = String.format("SELECT * FROM %s WHERE %s IS NULL", name_tripletable,
				column_name_predicate);
		long numFailedPredicates = spark.sql(queryFailedPredicates).count();
		logger.info("Number of lines in which predicate is null: " + numFailedPredicates);
		String queryFailedObjects = String.format("SELECT * FROM %s WHERE %s IS NULL", name_tripletable,
				column_name_object);
		long numFailedObjects = spark.sql(queryFailedObjects).count();
		logger.info("Number of lines in which object is null: " + numFailedObjects);
			
		if ((numFailedSubjects + numFailedPredicates + numFailedObjects) > 0) {
			triplesWithNulls = identifyNullsInTriples();
			List corruptedList = triplesWithNulls.limit(5).collectAsList();
			logger.info("First 5 triples with nulls (less if there are less): " + corruptedList);
		}
		
		String querySubjectsAreDots = String.format("SELECT * FROM %s WHERE %s RLIKE '^\\s*\\.\\s*$'", name_tripletable,
				column_name_subject);	
		long numSubjectsAreDots = spark.sql(querySubjectsAreDots).count();
		logger.info("Number of lines in which subject is a dot: " + numSubjectsAreDots);
		String queryPredicatesAreDots = String.format("SELECT * FROM %s WHERE %s RLIKE '^\\s*\\.\\s*$'", name_tripletable,
				column_name_predicate);	
		long numPredicatesAreDots = spark.sql(queryPredicatesAreDots).count();
		logger.info("Number of lines in which predicate is a dot: " + numPredicatesAreDots);
		String queryObjectsAreDots = String.format("SELECT * FROM %s WHERE %s RLIKE '^\\s*\\.\\s*$'", name_tripletable,
				column_name_object);	
		long numObjectsAreDots = spark.sql(queryObjectsAreDots).count();
		logger.info("Number of lines in which object is a dot: " + numObjectsAreDots);
		
		if ((numSubjectsAreDots + numPredicatesAreDots + numObjectsAreDots) > 0) {
			triplesWithDotRes = identifyDotsInTriples();
			List corruptedList = triplesWithDotRes.limit(5).collectAsList();
			logger.info("First 5 triples with dots (less if there are less): " + corruptedList);
		}
		
		String queryMultipleObjectItems = String.format("SELECT * FROM %s WHERE %s RLIKE '^\\s*<.*<.*>' "
				+ "or %s RLIKE '^\\s*\"[^\"]+\".*\\s*\"'", name_tripletable,
				column_name_object, column_name_object);	
		long numObjectsWithMultipleObjects = spark.sql(queryMultipleObjectItems).count();
		
		if (numObjectsWithMultipleObjects > 0) {
			objectsWithMultipleItems = identifyMultipleObjects();
			List corruptedList = objectsWithMultipleItems.limit(5).collectAsList();
			logger.info("First 5 triples with multiple items as object (less if there are less): " + corruptedList);
		}
		
		//After all potential problems have been identified now we remove the identified triples:
		String queryAllTriples = String.format("SELECT * FROM %s", name_tripletable);
		allTriples = spark.sql(queryAllTriples);
		if (triplesWithNulls != null) {
			allTriples = allTriples.except(triplesWithNulls);
		}
		if (triplesWithDotRes != null) {
			allTriples = allTriples.except(triplesWithDotRes);
		}
		if (objectsWithMultipleItems != null) {
			allTriples = allTriples.except(objectsWithMultipleItems);
		}
		
		//One last step is required. If the triples contain a final .
		//it is necessary to remove it remove from the table, because it will appear there.
		allTriples = allTriples.withColumn(column_name_object+"_clean",  
				regexp_replace(allTriples.col(column_name_object), "(\\s*\\.\\s*)+$", ""));
		allTriples = allTriples.selectExpr(
				column_name_subject, column_name_predicate, column_name_object+"_clean AS " + column_name_object);
		
		if (allTriples.count()==0) {
			logger.error("Either your HDFS path does not contain any files or no triples were accepted in the given format (nt)");
			logger.error("The program will stop here.");
			throw new Exception("Empty HDFS directory or empty files within.");
		}
		
		List cleanedList = allTriples.limit(10).collectAsList();
		logger.info("First 10 cleaned triples with dots (less if there are less): " + cleanedList);
				
		allTriples.createOrReplaceTempView(name_tripletable+"_temp");
		String queryCreateFixedTable = 
				String.format("CREATE TABLE %s AS SELECT * FROM %s", 
						name_tripletable+"_fixed", name_tripletable+"_temp");
		spark.sql(queryCreateFixedTable);
		String queryDropOriginalTable = String.format("DROP TABLE IF EXISTS %s", name_tripletable);
		spark.sql(queryDropOriginalTable);
		name_tripletable = name_tripletable+"_fixed";		
	}
	
	/**
	 * Identifies rows containing nulls.
	 */
	public Dataset<Row> identifyNullsInTriples() {
		//We identify out from the rowCollection columns which contain nulls.
		String noNullsQuery = String.format("SELECT * FROM %s WHERE %s IS NULL or %s IS NULL or %s is NULL", name_tripletable, 
				column_name_subject, column_name_predicate, column_name_object);
		Dataset<Row> rowsWithNulls = spark.sql(noNullsQuery);						
		logger.info("Number of triples containing nulls : " + rowsWithNulls.count());
		return rowsWithNulls;
	}

	/**
	 * Identifies triples which contain only a dot (.). 
	 * This means the triple was not complete in the original file.
	 * @return
	 */
	public Dataset<Row> identifyDotsInTriples() {
		//We identify out from the rowCollection columns which contain dots.
		String noDotsQuery = 
				String.format("SELECT * FROM %s WHERE %s RLIKE '^\\s*\\.\\s*$' or %s RLIKE "
						+ "'^\\s*\\.\\s*$' or %s RLIKE '^\\s*\\.\\s*$'", name_tripletable, 
						column_name_subject, column_name_predicate, column_name_object);
		Dataset<Row> rowsWithDots = spark.sql(noDotsQuery);					
		logger.info("Number of triples containing dots : " + rowsWithDots.count());
		return rowsWithDots;
	}
	
	/**
	 * Identifies multiple resources or literals in the object column.
	 * This means that a triple in the original file had more than three elements.
	 * This method identifies multiple resources of the same kind, i.e. two or more 
	 * resources or two or more literals. The case where these are mixed is not considered.
	 * @return
	 */
	public Dataset<Row> identifyMultipleObjects() {
		//We identify out from the rowCollection columns which contain dots.
		String queryMultipleResources = 
				String.format("SELECT * FROM %s WHERE %s RLIKE '^\\s*<.*<.*>'", name_tripletable, 
						column_name_object);
		Dataset<Row> rowsWithMultipleResources = spark.sql(queryMultipleResources);		
		String queryMultipleLiterals = 
				String.format("SELECT * FROM %s WHERE %s RLIKE '^\\s*\"[^\"]+\".*\\s*\"'", name_tripletable, 
						column_name_object);
		Dataset<Row> rowsWithMultipleLiterals = spark.sql(queryMultipleLiterals);	
		
		Dataset<Row> rowsWithMultipleItems = rowsWithMultipleResources.union(rowsWithMultipleLiterals);								
		logger.info("Number of triples containing dots : " + rowsWithMultipleItems.count());
		return rowsWithMultipleItems;
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
