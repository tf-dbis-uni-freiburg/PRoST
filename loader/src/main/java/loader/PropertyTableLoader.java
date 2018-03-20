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
	
	private final String nonReversePropertiesTableName = "properties";
	private final String reverseTableName = "reverse_properties";
	protected final String outputNonReversePropertyTableName = "property_table";
	protected final String outputReversePropertyTableName = "reverse_property_table";
	
	/** Separator used internally to distinguish two values in the same string  */
	private static final  String COLLUMNS_SEPARATOR = "\\$%";
	
	public PropertyTableLoader(String hdfs_input_directory,	String database_name, SparkSession spark) {
		super(hdfs_input_directory, database_name, spark);
	}
	
	public void load() {
		long startTime;
		long executionTime;
		
		startTime = System.currentTimeMillis();
		//creates non reverse property table
		buildProperties(nonReversePropertiesTableName, false, false);
		buildComplexPropertyTable(nonReversePropertiesTableName, false, false).write().mode(SaveMode.Overwrite).format(TABLE_FORMAT).saveAsTable(outputNonReversePropertyTableName);
		executionTime = System.currentTimeMillis() - startTime;
		LOGGER.info("Created property table with name: " + outputNonReversePropertyTableName);	
		LOGGER.info("Property table created in: " + String.valueOf(executionTime));
		
		startTime = System.currentTimeMillis();
		//creates reverse property table
		buildProperties(reverseTableName, true, false);	
		buildComplexPropertyTable(reverseTableName, true, false).write().mode(SaveMode.Overwrite).format(TABLE_FORMAT).saveAsTable(outputReversePropertyTableName);	
		executionTime = System.currentTimeMillis() - startTime;
		LOGGER.info("Created reverse property table with name: " + outputReversePropertyTableName);	
		LOGGER.info("Reverse property table created in: " + String.valueOf(executionTime));
	}
		
	/**
	 * Creates a table containing possible properties and their complexities
	 * 
	 * <p>The scheme of the created table is (&ltpredicateColumnName&gt: String, is_complex: Integer), where is_complex has the value of 1 in case there is more than one
	 * triple containing the predicate-object or subject-predicate (depending on the <b>isReverseOrder</b> parameter).
	 * 
	 * @param propertiesTableName name of the table to be created
	 * @param isReverseOrder indicates whether the complexity of a property is determined by the count of elements in a predicate-object group (case TRUE),
	 * or the count of elements in a subject-predicate group (case FALSE)
	 * @param ignoreLiterals indicates whether to ignore triples where the object is a literal. Only applicable when isReverse is True, as no subject is a literal.
	 */
	private void buildProperties(String propertiesTableName, Boolean isReverseOrder, Boolean ignoreLiterals) {
		// return rows of format <predicate, is_complex>
		// is_complex can be 1 or 0
		// 1 for multivalued predicate, 0 for single predicate
				
		// select the properties that are complex
		Dataset<Row> multivaluedProperties;
		if (isReverseOrder) {
			//grouping by predicate-object
			if (ignoreLiterals) {	
				//literals begin and ends with the char '"'
				multivaluedProperties = spark.sql(String.format(
						"SELECT DISTINCT(%1$s) AS %1$s FROM "
						+ "(SELECT %2$s, %1$s, COUNT(*) AS rc FROM %3$s WHERE %2$s NOT RLIKE \"^\\\".*\\\"$\" GROUP BY %2$s, %1$s HAVING rc > 1) AS grouped",
						predicateColumnName, objectColumnName, tripleTableName));
			}
			else {
				multivaluedProperties = spark.sql(String.format(
						"SELECT DISTINCT(%1$s) AS %1$s FROM "
						+ "(SELECT %2$s, %1$s, COUNT(*) AS rc FROM %3$s GROUP BY %2$s, %1$s HAVING rc > 1) AS grouped",
						predicateColumnName, objectColumnName, tripleTableName));
			}
		}else {
			//grouping by subject-predicate
			multivaluedProperties = spark.sql(String.format(
					"SELECT DISTINCT(%1$s) AS %1$s FROM "
					+ "(SELECT %2$s, %1$s, COUNT(*) AS rc FROM %3$s GROUP BY %2$s, %1$s HAVING rc > 1) AS grouped",
					predicateColumnName, subjectColumnName, tripleTableName));
		}
		
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
		cleanedProperties.write().mode(SaveMode.Overwrite).saveAsTable(propertiesTableName);
		LOGGER.info("Created properties table with name: " + propertiesTableName);
	}
	
	/**
	 * Returns a dataset with the property table generated from the given triplestore table
	 * 
	 * <p>Given a triplestore table (s-p-o), columns of the type List[String] or String are created for each predicate.</p>
	 * <p>A column named &ltsubjectColumnName&gt is created containing the source column. The source column is either 
	 * &ltsubjectColumnName&gt if <b>isReverseOrder</b> is <b>false</b>, or &ltpredicateColumnName&gt otherwise</p>
	 * <p>Each property column contains a List[String] if complex,  or String otherwise, with the appropriate entities from the target column</p>
	 * 
	 * @param propertiesTableName name of the properties table (schema: properties, is_complex) to be used
	 * @param isReverseOrder sets the order of the source and target columns (False = subject->predicate->object; True= object->predicate->subject)
	 * @param removeLiterals sets whether literals should be removed from the source column.
	 * @return return a dataset with the following schema: 
	 * (&ltsubjectColumnName&gt: STRING, p1: LIST&ltSTRING&gt OR STRING, ..., pN: LIST&ltSTRING&gt OR STRING).
	 */
	public Dataset<Row> buildComplexPropertyTable(String propertiesTableName, Boolean isReverseOrder, Boolean removeLiterals) {
		final String sourceColumn;
		final String targetColumn;
		
		if (isReverseOrder) {
			sourceColumn = objectColumnName;
			targetColumn = subjectColumnName;
		}
		else {
			sourceColumn = subjectColumnName;
			targetColumn = objectColumnName;
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
		PropertiesAggregateFunction aggregator = new PropertiesAggregateFunction(allProperties, COLLUMNS_SEPARATOR);

		String predicateObjectColumnName = "po";
		String groupColumnName = "group";
		
		// get the compressed table
		Dataset<Row> compressedTriples;
		if (removeLiterals) {
			compressedTriples = spark.sql(String.format("SELECT %s, CONCAT(%s, '%s', %s) AS %s FROM %s WHERE %s NOT RLIKE \"^\\\".*\\\"$\"",
					sourceColumn, predicateColumnName, COLLUMNS_SEPARATOR, targetColumn, predicateObjectColumnName, tripleTableName, sourceColumn));
		} else {
			compressedTriples = spark.sql(String.format("SELECT %s, CONCAT(%s, '%s', %s) AS %s FROM %s",
					sourceColumn, predicateColumnName, COLLUMNS_SEPARATOR, targetColumn, predicateObjectColumnName, tripleTableName));
		}
		
		// group by the source column and get all the data
		Dataset<Row> grouped = compressedTriples.groupBy(sourceColumn)
				.agg(aggregator.apply(compressedTriples.col(predicateObjectColumnName)).alias(groupColumnName));

		// build the query to extract the property from the array
		String[] selectProperties = new String[allProperties.length + 1];
		selectProperties[0] = sourceColumn;
		for (int i = 0; i < allProperties.length; i++) {
			// if property is a full URI, remove the < at the beginning end > at the end
			String rawProperty = allProperties[i].startsWith("<") && allProperties[i].endsWith(">") 
					? allProperties[i].substring(1, allProperties[i].length() - 1) 
					:  allProperties[i];
					
			// if is not a complex type, extract the value
			String newProperty = isComplexProperty[i]
					? " " + groupColumnName + "[" + String.valueOf(i) + "] AS " + getValidHiveName(rawProperty)
					: " " + groupColumnName + "[" + String.valueOf(i) + "][0] AS " + getValidHiveName(rawProperty);
			selectProperties[i + 1] = newProperty;
		}		
		
		if (isReverseOrder) {
			// renames the column so that its name is consistent with the non-reverse dataset
			// this guarantees that any method that access a Property Table can be used with a Reverse Property Table
			return grouped.selectExpr(selectProperties).withColumnRenamed(sourceColumn, targetColumn);
		}
		else{
			return grouped.selectExpr(selectProperties);
		}
	}
}