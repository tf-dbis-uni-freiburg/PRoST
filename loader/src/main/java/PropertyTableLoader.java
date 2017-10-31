import java.io.FileNotFoundException;
import java.util.List;

import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;


/**
 * Class that construct complex property table. It operates over set of rdf
 * triples, collects and transforms information about them into a table. If we have
 * a list of predicates/properties p1, ... , pN, then the scheme of the table is
 * (s: STRING, p1: LIST<STRING> OR STRING, ..., pN: LIST<STRING> OR STRING).
 * Column s contains subjects. For each subject , there is only one row in the
 * table. Each predicate can be of complex or simple type. If a predicate is of
 * simple type means that there is no subject which has more than one triple
 * containing this property/predicate. Then the predicate column is of type
 * STRING. Otherwise, if a predicate is of complex type which means that there
 * exists at least one subject which has more than one triple containing this
 * property/predicate. Then the predicate column is of type LIST<STRING>.
 * 
 * @author Matteo Cossu
 *
 */
public class PropertyTableLoader {

	protected String hdfs_input_directory;
	
	/** The separators used in the rdf data. */
	public String field_terminator = "\\t";
	public String line_terminator = "\\n";


	public String column_name_subject = "s";
	public String column_name_predicate = "p";
	public String column_name_object = "o";
	private String tablename_tripletable  = "tripletable";
	private String tablename_properties = "properties";
	
	/** Separator used internally to distinguish two values in the same string  */
	public String columns_separator = "\\$%";
	public boolean keepTemporaryTables = false;

	protected String output_db_name;
	protected static final String output_tablename = "property_table";
	protected static final String table_format = "parquet";

	private SparkSession spark;
	private static final Logger logger = Logger.getLogger(Main.class);
	
	
	public PropertyTableLoader(String inputPath, String outputDB) {
		this.hdfs_input_directory = inputPath;
		this.output_db_name = outputDB;
		
		// initialize the Spark environment 
		this.spark = SparkSession
					  .builder()
					  .appName("PRoST-Loader-PropertyTable")
					  .getOrCreate();
	}

	
	public void load() throws FileNotFoundException {
		
		useOutputDatabase();
		
		buildTripleTable();
		
		buildProperties();
		
		// collect information for all properties
		List<Row> props = spark.sql(String.format("SELECT * FROM %s", tablename_properties)).collectAsList();
		String[] allProperties = new String[props.size()];
		Boolean[] isComplexProperty = new Boolean[props.size()];
		
		for (int i = 0; i < props.size(); i++) {
			allProperties[i] = props.get(i).getString(0);
			isComplexProperty[i] = props.get(i).getInt(1) == 1;
		}
		
		// create complex property table
		buildComplexPropertyTable(allProperties, isComplexProperty);
		
		// Drop intermediate tables
		if (!keepTemporaryTables) {
			dropTables(tablename_tripletable, tablename_properties);
		}
	}
	
	
	private void useOutputDatabase() {
		spark.sql("CREATE DATABASE IF NOT EXISTS " + output_db_name);
		spark.sql("USE "  + output_db_name);
		logger.info("Using the database: " + output_db_name);
	}


	/**
	 * Build a table that contains all rdf triples. If a file with prefixes is
	 * given, they will be replaced. 
	 */
	public void buildTripleTable() throws FileNotFoundException{
		String createTripleTable = String.format(
				"CREATE EXTERNAL TABLE IF NOT EXISTS %s(%s STRING, %s STRING, %s STRING) ROW FORMAT DELIMITED"
						+ " FIELDS TERMINATED BY '%s'  LINES TERMINATED BY '%s' LOCATION '%s'",
						tablename_tripletable  , column_name_subject, column_name_predicate, column_name_object,
				field_terminator, line_terminator, hdfs_input_directory);

		spark.sql(createTripleTable);
		logger.info("Created tripletable");
	}

	public void buildProperties() {
		// return rows of format <predicate, is_complex>
		// is_complex can be 1 or 0
		// 1 for multivalued predicate, 0 for single predicate

		// select the properties that are complex
		Dataset<Row> multivaluedProperties = spark.sql(String.format(
				"SELECT DISTINCT(%1$s) AS %1$s FROM (SELECT %2$s, %1$s, COUNT(*) AS rc FROM %3$s GROUP BY %2$s, %1$s HAVING rc > 1) AS grouped",
				column_name_predicate, column_name_subject, tablename_tripletable));

		// select all the properties
		Dataset<Row> allProperties = spark.sql(String.format("SELECT DISTINCT(%1$s) AS %1$s FROM %2$s",
				column_name_predicate, tablename_tripletable));

		// select the properties that are not complex
		Dataset<Row> singledValueProperties = allProperties.except(multivaluedProperties);

		// combine them
		Dataset<Row> combinedProperties = singledValueProperties
				.selectExpr(column_name_predicate, "0 AS is_complex")
				.union(multivaluedProperties.selectExpr(column_name_predicate, "1 AS is_complex"));
		
		// remove '<' and '>', convert the characters
		Dataset<Row> cleanedProperties = combinedProperties.withColumn("p", functions.regexp_replace(functions.translate(combinedProperties.col("p"), "<>", ""), 
				"[[^\\w]+]", "_"));
		
		// write the result
		cleanedProperties.write().mode(SaveMode.Overwrite).saveAsTable(tablename_properties);
		logger.info("Created properties table with name: " + tablename_properties);
	}

	/**
	 * Create the final property table, allProperties contains the list of all
	 * possible properties isComplexProperty contains (in the same order used by
	 * allProperties) the boolean value that indicates if that property is
	 * complex (called also multi valued) or simple.
	 */
	public void buildComplexPropertyTable(String[] allProperties, Boolean[] isComplexProperty) {

		// create a new aggregation environment
		PropertiesAggregateFunction aggregator = new PropertiesAggregateFunction(allProperties, columns_separator);

		String predicateObjectColumn = "po";
		String groupColumn = "group";

		// get the compressed table
		Dataset<Row> compressedTriples = spark.sql(String.format("SELECT %s, CONCAT(%s, '%s', %s) AS po FROM %s",
				column_name_subject, column_name_predicate, columns_separator, column_name_object, tablename_tripletable));

		// group by the subject and get all the data
		Dataset<Row> grouped = compressedTriples.groupBy(column_name_subject)
				.agg(aggregator.apply(compressedTriples.col(predicateObjectColumn)).alias(groupColumn));

		// build the query to extract the property from the array
		String[] selectProperties = new String[allProperties.length + 1];
		selectProperties[0] = column_name_subject;
		for (int i = 0; i < allProperties.length; i++) {
			
			// if property is a full URI, remove the < at the beginning end > at the end
			String rawProperty = allProperties[i].startsWith("<") && allProperties[i].endsWith(">") ? 
					allProperties[i].substring(1, allProperties[i].length() - 1) :  allProperties[i];
			// if is not a complex type, extract the value
			String newProperty = isComplexProperty[i]
					? " " + groupColumn + "[" + String.valueOf(i) + "] AS " + getValidColumnName(rawProperty)
					: " " + groupColumn + "[" + String.valueOf(i) + "][0] AS " + getValidColumnName(rawProperty);
			selectProperties[i + 1] = newProperty;
		}

		Dataset<Row> propertyTable = grouped.selectExpr(selectProperties);

		// write the final one
		propertyTable.write().mode(SaveMode.Overwrite).format(table_format)
				.saveAsTable(output_tablename);
		logger.info("Created property table with name: " + output_tablename);

	}

	/**
	 * Replace all not allowed characters of a DB column name by an
	 * underscore("_") and return a valid DB column name.
	 * 
	 * @param columnName
	 *            column name that will be validated and fixed
	 * @return name of a DB column
	 */
	private String getValidColumnName(String columnName) {
		return columnName.replaceAll("[^a-zA-Z0-9_]", "_");
	}
	
	public void dropTables(String... tableNames) {
		for (String tb : tableNames)
			spark.sql("DROP TABLE " + tb);
		logger.info("Removed tables: " + tableNames);
	}

}