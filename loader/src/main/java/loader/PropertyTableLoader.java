package loader;
import java.util.List;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;


/**
 * Class that constructs complex property table. It operates over set of RDF triples, collects and transforms
 * information about them into a table. If we have  a list of predicates/properties p1, ... , pN, then 
 * the scheme of the table is (s: STRING, p1: LIST<STRING> OR STRING, ..., pN: LIST<STRING> OR STRING).
 * Column s contains subjects. For each subject , there is only one row in the table. Each predicate can be 
 * of complex or simple type. If a predicate is of simple type means that there is no subject which has more
 * than one triple containing this property/predicate. Then the predicate column is of type STRING. Otherwise, 
 * if a predicate is of complex type which means that there exists at least one subject which has more 
 * than one triple containing this property/predicate. Then the predicate column is of type LIST<STRING>.
 * 
 * @author Matteo Cossu
 *
 */
public class PropertyTableLoader extends Loader{
	private static final String TABLENAME_REVERSE_PROPERTIES = "reverse_property_table";
	
	protected String hdfs_input_directory;
	
	private String tablename_properties = "properties";
	
	/** Separator used internally to distinguish two values in the same string  */
	public String columns_separator = "\\$%";

	protected String output_db_name;
	protected static final String output_tablename = "property_table";
	
	public PropertyTableLoader(String hdfs_input_directory,
			String database_name, SparkSession spark) {
		super(hdfs_input_directory, database_name, spark);
	}
	
	public void load() {
		buildProperties(tablename_properties, column_name_predicate, column_name_subject, name_tripletable);
			
		Dataset<Row> propertyTable = buildComplexPropertyTable(tablename_properties, column_name_subject, 
				column_name_predicate, column_name_object, name_tripletable, false, false);
		
		buildProperties(TABLENAME_REVERSE_PROPERTIES, column_name_predicate, column_name_object, name_tripletable);
		
		Dataset<Row> reversePropertyTable = buildComplexPropertyTable(TABLENAME_REVERSE_PROPERTIES, column_name_subject, 
				column_name_predicate, column_name_object, name_tripletable, true, true);
		
		Dataset<Row> fullPropertyTable = propertyTable.union(reversePropertyTable);
		
		fullPropertyTable.write().mode(SaveMode.Overwrite).format(table_format).saveAsTable(output_tablename);
		logger.info("Created property table with name: " + output_tablename);	
	}
		
	/**
	 * Creates a table containing possible properties and their complexities
	 * 
	 * @param propertyTableName name of the table to be created
	 * @param predicateColumnName name of the column containing predicates in the triplestore table
	 * @param sourceEntityColumnName name of the source column in the triplestore table (either subject or object entity)
	 * @param tripleTableName name of the triplestore table
	 */
	public void buildProperties(String propertyTableName, String predicateColumnName, String sourceEntityColumnName, String tripleTableName) {
		// return rows of format <predicate, is_complex>
		// is_complex can be 1 or 0
		// 1 for multivalued predicate, 0 for single predicate
		
		
		// select the properties that are complex
		Dataset<Row> multivaluedProperties = spark.sql(String.format(
				"SELECT DISTINCT(%1$s) AS %1$s FROM "
				+ "(SELECT %2$s, %1$s, COUNT(*) AS rc FROM %3$s GROUP BY %2$s, %1$s HAVING rc > 1) AS grouped",
				predicateColumnName, sourceEntityColumnName, tripleTableName));
		
		// select all the properties
		Dataset<Row> allProperties = spark.sql(String.format("SELECT DISTINCT(%1$s) AS %1$s FROM %2$s",
				predicateColumnName, tripleTableName));
		
		// select the properties that are not complex
		Dataset<Row> singledValueProperties = allProperties.except(multivaluedProperties);
		
		// combine them
		Dataset<Row> combinedProperties = singledValueProperties
				.selectExpr(predicateColumnName, "0 AS is_complex")
				.union(multivaluedProperties.selectExpr(predicateColumnName, "1 AS is_complex"));
		
		// remove '<' and '>', convert the characters
		Dataset<Row> cleanedProperties = combinedProperties.withColumn(predicateColumnName, 
				functions.regexp_replace(functions.translate(combinedProperties.col(predicateColumnName), "<>", ""), 
				"[[^\\w]+]", "_"));
		
		// write the result
		cleanedProperties.write().mode(SaveMode.Overwrite).saveAsTable(propertyTableName);
		logger.info("Created properties table with name: " + propertyTableName);
	}
	
	/**
	 * Returns a dataset with the property table generated from the given triplestore table
	 * 
	 * Given a triplestore table (s-p-o), columns of the type List[String] or String are created for each predicate.
	 * <p>A column named <subjectColumnName> is created containing the source column. The source column is either 
	 * <subjectColumnName> if <isReverseOrder> is False, or <predicateColumnName> otherwise</p>
	 * A column named is_reverse_order is also created
	 * <p>Each property column contains a List[String] or String with the appropriate entities from the target column,
	 * or is Null otherwise</p>
	 * 
	 * @param propertiesTableName name of the table of properties to be used
	 * @param subjectColumnName name of the column containing the subjects in the triplestore table
	 * @param predicateColumnName name of the column containing the predicates in the triplestore table
	 * @param objectColumnName name of the column containing the objects in the triplestore table
	 * @param tripleTableName name of the triplestore table
	 * @param isReverseOrder sets the order of the source and target columns (False = subject->predicate->object; True= object->predicate->subject)
	 * @param removeLiterals sets whether literals should be removed from the source column
	 * @return return a dataset with the following schema: (subjectColumnName: STRING, is_reverse_order: Boolean, p1: LIST<STRING> OR STRING, ..., pN: LIST<STRING> OR STRING).
	 */
	public Dataset<Row> buildComplexPropertyTable(String propertiesTableName, String subjectColumnName, String predicateColumnName, String objectColumnName, 
			String tripleTableName, Boolean isReverseOrder, Boolean removeLiterals) {
				
		if (isReverseOrder) {
			// swap subject and object column names
			String temp = subjectColumnName;
			subjectColumnName = objectColumnName;
			objectColumnName = temp;
		}
		
		// collect information for all properties
		// allProperties contains the list of all possible properties
		// isComplexProperty indicates with a boolean value whether the property with the same index in allProperties is complex (multivalued) or simple
		
		List<Row> props = spark.sql(String.format("SELECT * FROM %s", propertiesTableName)).collectAsList();
		String[] allProperties = new String[props.size()];
		Boolean[] isComplexProperty = new Boolean[props.size()];
				
		for (int i = 0; i < props.size(); i++) {
			allProperties[i] = props.get(i).getString(0);
			isComplexProperty[i] = props.get(i).getInt(1) == 1;
		}
		//this.properties_names = allProperties;
			
		// create a new aggregation environment
		PropertiesAggregateFunction aggregator = new PropertiesAggregateFunction(allProperties, columns_separator);

		String predicateObjectColumn = "po";
		String groupColumn = "group";
		
		// get the compressed table
		Dataset<Row> compressedTriples;
		if (removeLiterals) {
			compressedTriples = spark.sql(String.format("SELECT %s, CONCAT(%s, '%s', %s) AS %s FROM %s WHERE %s NOT RLIKE \".*\"",
					subjectColumnName, predicateColumnName, columns_separator, objectColumnName, predicateObjectColumn, tripleTableName, subjectColumnName));
		} else {
			compressedTriples = spark.sql(String.format("SELECT %s, CONCAT(%s, '%s', %s) AS %s FROM %s",
					subjectColumnName, predicateColumnName, columns_separator, objectColumnName, predicateObjectColumn, tripleTableName));
		}
		
		// group by the subject and get all the data
		Dataset<Row> grouped = compressedTriples.groupBy(subjectColumnName)
				.agg(aggregator.apply(compressedTriples.col(predicateObjectColumn)).alias(groupColumn));

		// build the query to extract the property from the array
		String[] selectProperties = new String[allProperties.length + 2];
		selectProperties[0] = subjectColumnName;
		if (isReverseOrder) {
			selectProperties[1] = "True as is_reverse_order";
		} else {
			selectProperties[1] = "False as is_reverse_order";
		}
		for (int i = 0; i < allProperties.length; i++) {
			// if property is a full URI, remove the < at the beginning end > at the end
			String rawProperty = allProperties[i].startsWith("<") && allProperties[i].endsWith(">") 
					? allProperties[i].substring(1, allProperties[i].length() - 1) 
					:  allProperties[i];
					
			// if is not a complex type, extract the value
			String newProperty = isComplexProperty[i]
					? " " + groupColumn + "[" + String.valueOf(i) + "] AS " + getValidHiveName(rawProperty)
					: " " + groupColumn + "[" + String.valueOf(i) + "][0] AS " + getValidHiveName(rawProperty);
			selectProperties[i + 2] = newProperty;
		}		
		
		if (isReverseOrder) {
			// renames the column so that its name is consistent with the non-reverse dataset
			return grouped.selectExpr(selectProperties).withColumnRenamed(subjectColumnName, predicateColumnName);
		}
		else{
			return grouped.selectExpr(selectProperties);
		}
	}
}