package loader;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;

import scala.Tuple2;

/**
 * Class that constructs complex property table. It operates over set of RDF triples,
 * collects and transforms information about them into a table. If we have a list of
 * predicates/properties p1, ... , pN, then the scheme of the table is (s: STRING, p1:
 * LIST<STRING> OR STRING, ..., pN: LIST<STRING> OR STRING). Column s contains subjects.
 * For each subject , there is only one row in the table. Each predicate can be of complex
 * or simple type. If a predicate is of simple type means that there is no subject which
 * has more than one triple containing this property/predicate. Then the predicate column
 * is of type STRING. Otherwise, if a predicate is of complex type which means that there
 * exists at least one subject which has more than one triple containing this
 * property/predicate. Then the predicate column is of type LIST<STRING>.
 *
 * @author Matteo Cossu
 * @author Victor Anthony Arrascue Ayala
 * @author Guilherme Schievelbein
 */

public class WidePropertyTableLoader extends Loader {
	/**
	 * Type of the property table to be created. WPT = Wide Property Table IWPT = Inverse
	 * Wide; Property Table; JWPT = Joined Wide Property Table
	 */
	public static enum PropertyTableType {
	WPT, IWPT, JWPT
	}

	/**
	 * Separator used internally to distinguish two values in the same string.
	 */
	public String columns_separator = "\\$%";
	public String column_name_subject = super.column_name_subject;
	public String column_name_object = super.column_name_object;

	protected String output_db_name;
	protected String output_tablename = "wide_property_table";
	protected boolean wptPartitionedBySub = false;

	private final String tablename_properties = "properties";
	private final String inversePropertiesTableName = "inverse_properties";
	private PropertyTableType propertyTableType = PropertyTableType.WPT;

	/**
	 * Constructor for a Wide Property Table loader with the default
	 * <code>propertyTableType</code> value.
	 */
	public WidePropertyTableLoader(final String database_name,
			final SparkSession spark, final boolean wptPartitionedBySub) {
		super(database_name, spark);
		this.wptPartitionedBySub = wptPartitionedBySub;
	}

	public WidePropertyTableLoader(final String database_name,
			final SparkSession spark, final boolean wptPartitionedBySub, final PropertyTableType propertyTableType) {
		super(database_name, spark);
		this.wptPartitionedBySub = wptPartitionedBySub;
		this.propertyTableType = propertyTableType;

		if (propertyTableType == PropertyTableType.IWPT) {
			output_tablename = "inverse_" + output_tablename;
			final String temp = column_name_subject;
			column_name_subject = column_name_object;
			column_name_object = temp;
		} else if (propertyTableType == PropertyTableType.JWPT) {
			output_tablename = "joined_".concat(output_tablename);
		}
	}

	@Override
	public void load() {
		if (propertyTableType == PropertyTableType.JWPT) {
			final Dataset<Row> wpt = loadDataset("o_", false, tablename_properties);
			final String temp = column_name_subject;
			column_name_subject = column_name_object;
			column_name_object = temp;
			final Dataset<Row> iwpt = loadDataset("s_", true, inversePropertiesTableName);
			final Dataset<Row> joinedPT = wpt.join(iwpt, scala.collection.JavaConverters
					.asScalaIteratorConverter(Arrays.asList("s").iterator()).asScala().toSeq(), "outer");
			saveTable(joinedPT);

		} else {
			saveTable(loadDataset());
		}
	}

	/**
	 * This method handles the problem when two predicate are the same in a case-insensitive
	 * context but different in a case-sensitve one. For instance:
	 * <http://example.org/somename> and <http://example.org/someName>. Since Hive is case
	 * insensitive the problem will be solved removing one of the entries from the list of
	 * predicates.
	 *
	 */
	public Map<String, Boolean> handleCaseInsPredAndCard(final Map<String, Boolean> propertiesMultivaluesMap) {
		final Set<String> seenPredicates = new HashSet<>();
		final Set<String> originalRemovedPredicates = new HashSet<>();

		final Iterator<String> it = propertiesMultivaluesMap.keySet().iterator();
		while (it.hasNext()) {
			final String predicate = it.next();
			if (seenPredicates.contains(predicate.toLowerCase())) {
				originalRemovedPredicates.add(predicate);
			} else {
				seenPredicates.add(predicate.toLowerCase());
			}
		}

		for (final String predicateToBeRemoved : originalRemovedPredicates) {
			propertiesMultivaluesMap.remove(predicateToBeRemoved);
		}

		if (originalRemovedPredicates.size() > 0) {
			logger.info("The following predicates had to be removed from the list of predicates "
					+ "(it is case-insensitive equal to another predicate): " + originalRemovedPredicates);
		}
		return propertiesMultivaluesMap;
	}

	/**
	 * Creates a table with all properties and their cardinalities.
	 *
	 * @param tableName
	 *            the name of the table to be created
	 */
	public void buildPropertiesAndCardinalities(final String tableName) {
		// return rows of format <predicate, is_complex>
		// is_complex can be 1 or 0
		// 1 for multivalued predicate, 0 for single predicate
		// select all the properties
		final Dataset<Row> allProperties = spark
				.sql(String.format("SELECT DISTINCT(%1$s) AS %1$s FROM %2$s", column_name_predicate, name_tripletable));

		logger.info("Total Number of Properties found: " + allProperties.count());

		// select the properties that are multivalued
		final Dataset<Row> multivaluedProperties = spark.sql(String.format(
				"SELECT DISTINCT(%1$s) AS %1$s FROM "
						+ "(SELECT %2$s, %1$s, COUNT(*) AS rc FROM %3$s GROUP BY %2$s, %1$s HAVING rc > 1) AS grouped",
				column_name_predicate, column_name_subject, name_tripletable));

		logger.info("Number of Multivalued Properties found: " + multivaluedProperties.count());

		// select the properties that are not multivalued
		final Dataset<Row> singledValueProperties = allProperties.except(multivaluedProperties);
		logger.info("Number of Single-valued Properties found: " + singledValueProperties.count());

		// combine them
		final Dataset<Row> combinedProperties =
				singledValueProperties.selectExpr(column_name_predicate, "0 AS is_complex")
						.union(multivaluedProperties.selectExpr(column_name_predicate, "1 AS is_complex"));

		// remove '<' and '>', convert the characters
		final Dataset<Row> cleanedProperties = combinedProperties.withColumn("p",
				functions.regexp_replace(functions.translate(combinedProperties.col("p"), "<>", ""), "[[^\\w]+]", "_"));

		final List<Tuple2<String, Integer>> cleanedPropertiesList =
				cleanedProperties.as(Encoders.tuple(Encoders.STRING(), Encoders.INT())).collectAsList();
		if (cleanedPropertiesList.size() > 0) {
			logger.info("Clean Properties (stored): " + cleanedPropertiesList);
		}

		// write the result
		cleanedProperties.write().mode(SaveMode.Overwrite).saveAsTable(tableName);
	}

	/**
	 * Create the property table dataset.
	 *
	 * @param allProperties
	 *            contains the list of all possible properties
	 * @param isMultivaluedProperty
	 *            contains (in the same order used by <code>allProperties</code> the boolean
	 *            value that indicates if that property is multi-valued or not
	 * @param prefix
	 *            The prefix to be added to the columns. <code>null</code> if no prefix is
	 *            added.
	 * @param swapSubjectObjectColumnNames
	 *            <code>true</code> to change the first column name in the final table for the
	 *            <code>column_name_object</code> value. I.e.: changes "o" to "s" in an IWPT.
	 * @return
	 */
	private Dataset<Row> buildWidePropertyTable(final String[] allProperties, final Boolean[] isMultivaluedProperty,
			final String prefix, final Boolean swapSubjectObjectColumnNames) {
		logger.info("Building the complete property table.");

		// create a new aggregation environment
		final PropertiesAggregateFunction aggregator =
				new PropertiesAggregateFunction(allProperties, columns_separator);

		final String predicateObjectColumn = "po";
		final String groupColumn = "group";

		// get the compressed table
		final Dataset<Row> compressedTriples = spark.sql(String.format("SELECT %s, CONCAT(%s, '%s', %s) AS po FROM %s",
				column_name_subject, column_name_predicate, columns_separator, column_name_object, name_tripletable));

		// group by the subject and get all the data
		final Dataset<Row> grouped = compressedTriples.groupBy(column_name_subject)
				.agg(aggregator.apply(compressedTriples.col(predicateObjectColumn)).alias(groupColumn));

		// build the query to extract the property from the array
		final String[] selectProperties = new String[allProperties.length + 1];
		selectProperties[0] = column_name_subject;
		for (int i = 0; i < allProperties.length; i++) {

			// if property is a full URI, remove the < at the beginning end > at
			// the end
			final String rawProperty = allProperties[i].startsWith("<") && allProperties[i].endsWith(">")
					? allProperties[i].substring(1, allProperties[i].length() - 1)
					: allProperties[i];
			// if is not a complex type, extract the value
			final String newProperty = isMultivaluedProperty[i]
					? " " + groupColumn + "[" + String.valueOf(i) + "] AS " + getValidHiveName(rawProperty)
					: " " + groupColumn + "[" + String.valueOf(i) + "][0] AS " + getValidHiveName(rawProperty);
			selectProperties[i + 1] = newProperty;
		}

		final List<String> allPropertiesList = Arrays.asList(selectProperties);
		logger.info("Columns of  Property Table: " + allPropertiesList);

		Dataset<Row> propertyTable = grouped.selectExpr(selectProperties);

		// renames the column so that its name is consistent with the non
		// inverse Wide Property Table.This guarantees that any method that access a Property
		// Table can be used with a Inverse Property Table without any changes
		if (swapSubjectObjectColumnNames) {
			propertyTable = propertyTable.withColumnRenamed(column_name_subject, column_name_object);
		}

		if (prefix != null) {
			for (final String property : allProperties) {
				propertyTable = propertyTable.withColumnRenamed(property, prefix.concat(property));
			}
		}
		return propertyTable;
	}

	/**
	 * Generates a dataset with the Property Table.
	 *
	 * @return Property table dataset
	 */
	private Dataset<Row> loadDataset() {
		if (propertyTableType == PropertyTableType.IWPT) {
			return loadDataset(null, true, inversePropertiesTableName);
		} else {
			return loadDataset(null, false, tablename_properties);
		}

	}

	/**
	 * Generates a dataset with the Property Table.
	 *
	 * @param prefix
	 *            The prefix to be added to columns. <code>null</code> if none is added.
	 * @param swapSubjectObjectColumnNames
	 *            <code>true</code> to change the first column name in the final table for the
	 *            <code>column_name_object</code> value. I.e.: changes "o" to "s" in an IWPT.
	 * @param propertiesTableName
	 *            Name of the table to be created with all properties and their cardinalities
	 * @return
	 */
	private Dataset<Row> loadDataset(final String prefix, final Boolean swapSubjectObjectColumnNames,
			final String propertiesTableName) {
		logger.info("PHASE 2: creating the property table...");

		buildPropertiesAndCardinalities(propertiesTableName);

		// collect information for all properties
		final List<Row> props = spark.sql(String.format("SELECT * FROM %s", propertiesTableName)).collectAsList();
		String[] allProperties = new String[props.size()];
		Boolean[] isMultivaluedProperty = new Boolean[props.size()];

		for (int i = 0; i < props.size(); i++) {
			allProperties[i] = props.get(i).getString(0);
			isMultivaluedProperty[i] = props.get(i).getInt(1) == 1;
		}

		// We create a map with the properties and the boolean.
		final Map<String, Boolean> propertiesMultivaluesMap = new HashMap<>();
		for (int i = 0; i < allProperties.length; i++) {
			final String property = allProperties[i];
			final Boolean multivalued = isMultivaluedProperty[i];
			propertiesMultivaluesMap.put(property, multivalued);
		}

		final Map<String, Boolean> fixedPropertiesMultivaluesMap = handleCaseInsPredAndCard(propertiesMultivaluesMap);

		final List<String> allPropertiesList = new ArrayList<>();
		final List<Boolean> isMultivaluedPropertyList = new ArrayList<>();
		allPropertiesList.addAll(fixedPropertiesMultivaluesMap.keySet());

		for (int i = 0; i < allPropertiesList.size(); i++) {
			final String property = allPropertiesList.get(i);
			isMultivaluedPropertyList.add(fixedPropertiesMultivaluesMap.get(property));
		}

		logger.info("All properties as array: " + allPropertiesList);
		logger.info("Multi-values flag as array: " + isMultivaluedPropertyList);

		allProperties = allPropertiesList.toArray(new String[allPropertiesList.size()]);
		properties_names = allProperties;
		isMultivaluedProperty = isMultivaluedPropertyList.toArray(new Boolean[allPropertiesList.size()]);

		// create wide property table
		return buildWidePropertyTable(allProperties, isMultivaluedProperty, prefix, swapSubjectObjectColumnNames);
	}

	/**
	 * Saves the dataset in Hive.
	 *
	 * @param propertyTableDataset
	 *            Property table dataset to be saved
	 */
	private void saveTable(final Dataset<Row> propertyTableDataset) {
		// write the final one, partitioned by subject
		// propertyTable = propertyTable.repartition(1000, column_name_subject);
		if (wptPartitionedBySub) {
			if (propertyTableType == PropertyTableType.IWPT) {
				propertyTableDataset.write().mode(SaveMode.Overwrite).partitionBy(column_name_object)
						.format(table_format).saveAsTable(output_tablename);
			} else {
				propertyTableDataset.write().mode(SaveMode.Overwrite).partitionBy(column_name_subject)
						.format(table_format).saveAsTable(output_tablename);
			}
		} else if (!wptPartitionedBySub) {
			propertyTableDataset.write().mode(SaveMode.Overwrite).format(table_format).saveAsTable(output_tablename);
		}
		logger.info("Created property table with name: " + output_tablename);

	}
}