package loader;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
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
import stats.DatabaseStatistics;

/**
 * Abstracts the loading of (I/J)wide property tables.
 */
abstract class PropertyTableLoader extends Loader {

	private static final String COLUMNS_SEPARATOR = "\\$%";
	private final boolean isPartitioned;
	private final String ptTableName;

	PropertyTableLoader(final String databaseName, final SparkSession spark, final boolean isPartitioned,
						final String tableName, final DatabaseStatistics statistics) {
		super(databaseName, spark, statistics);
		this.isPartitioned = isPartitioned;
		this.ptTableName = tableName;
	}

	abstract Dataset<Row> loadDataset();

	/**
	 * Generates and saves a property table.
	 */
	@Override
	public void load() {
		final Dataset<Row> pt = loadDataset();
		saveTable(pt, ptTableName, isPartitioned);
	}

	/**
	 * Generates the dataset with the predicates and their complexity.
	 *
	 * @param valueColumnName The column name of the values field. The object column
	 *                        for a WPT, and subject column for a IWPT
	 * @return Returns dataset with all predicates and the value "1" if complex, "0" if not.
	 */
	Dataset<Row> calculatePropertiesComplexity(final String valueColumnName) {
		// return rows of format <predicate, is_complex>
		// is_complex can be 1 or 0
		// 1 for multivalued predicate, 0 for single predicate
		// select all the properties
		final Dataset<Row> allProperties = spark
				.sql(String.format("SELECT DISTINCT(%1$s) AS %1$s FROM %2$s", COLUMN_NAME_PREDICATE, TRIPLETABLE_NAME));

		logger.info("Total Number of Properties found: " + allProperties.count());

		// select the properties that are multivalued
		final Dataset<Row> multivaluedProperties = spark.sql(String.format(
				"SELECT DISTINCT(%1$s) AS %1$s FROM "
						+ "(SELECT %2$s, %1$s, COUNT(*) AS rc FROM %3$s GROUP BY %2$s, %1$s HAVING rc > 1) AS grouped",
				COLUMN_NAME_PREDICATE, valueColumnName, TRIPLETABLE_NAME));

		logger.info("Number of Multivalued Properties found: " + multivaluedProperties.count());

		// select the properties that are not multivalued
		final Dataset<Row> singledValueProperties = allProperties.except(multivaluedProperties);
		logger.info("Number of Single-valued Properties found: " + singledValueProperties.count());

		// combine them
		final Dataset<Row> combinedProperties = singledValueProperties
				.selectExpr(COLUMN_NAME_PREDICATE, "0 AS is_complex")
				.union(multivaluedProperties.selectExpr(COLUMN_NAME_PREDICATE, "1 AS is_complex"));

		// remove '<' and '>', convert the characters
		final Dataset<Row> cleanedProperties = combinedProperties.withColumn("p",
				functions.regexp_replace(functions.translate(combinedProperties.col("p"), "<>", ""), "[[^\\w]+]", "_"));

		final List<Tuple2<String, Integer>> cleanedPropertiesList = cleanedProperties
				.as(Encoders.tuple(Encoders.STRING(), Encoders.INT())).collectAsList();
		/*if (cleanedPropertiesList.size() > 0) {
			logger.info("Clean Properties (stored): " + cleanedPropertiesList);
		}*/
		return cleanedProperties;
	}

	/**
	 * Saves the dataset in the database.
	 *
	 * @param dataset   DataSet to be saved.
	 * @param tableName Name of the table to be generated
	 */
	void saveTable(final Dataset<Row> dataset, final String tableName) {
		saveTable(dataset, tableName, false);
	}

	/**
	 * Saves the dataset in the database.
	 *
	 * @param dataset       DataSet to be saved.
	 * @param tableName     Name of the table to be generated
	 * @param isPartitioned Partition by <code>COLUMN_NAME_SUBJECT</code> if true
	 */
	private void saveTable(final Dataset<Row> dataset, final String tableName, final Boolean isPartitioned) {
		if (isPartitioned) {
			if (this instanceof JoinedWidePropertyTableLoader) {
				dataset.write().mode(SaveMode.Overwrite).format(TABLE_FORMAT).partitionBy("r")
						.saveAsTable(tableName);
				logger.info("Saved table: " + tableName + ", partitioned by : " + "r");
			} else {
				dataset.write().mode(SaveMode.Overwrite).format(TABLE_FORMAT).partitionBy(COLUMN_NAME_SUBJECT)
						.saveAsTable(tableName);
				logger.info("Saved table: " + tableName + ", partitioned by : " + COLUMN_NAME_SUBJECT);
			}

		} else {
			dataset.write().mode(SaveMode.Overwrite).format(TABLE_FORMAT).saveAsTable(tableName);
			logger.info("Saved table: " + tableName);
		}
	}

	/**
	 * This method handles the problem when two predicate are the same in a
	 * case-insensitive context but different in a case-sensitive one. For instance:
	 * &lt;http://example.org/somename&gt; and &lt;http://example.org/someName&gt;. Since Hive
	 * is case insensitive the problem will be solved removing one of the entries
	 * from the list of predicates.
	 */
	private Map<String, Boolean> handleCaseInsensitivePredicate(final Map<String, Boolean> propertiesMultivaluesMap) {
		final Set<String> seenPredicates = new HashSet<>();
		final Set<String> originalRemovedPredicates = new HashSet<>();

		for (final String predicate : propertiesMultivaluesMap.keySet()) {
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
	 * Generates a mapping between a property name and their complexity.
	 *
	 * @param propertiesCardinalities The dataset with all properties and their
	 *                                complexity.
	 * @return Returns the a mapping of all information that was in the input dataset.
	 */
	Map<String, Boolean> createPropertiesComplexitiesMap(final Dataset<Row> propertiesCardinalities) {
		final List<Row> properties = propertiesCardinalities.collectAsList();
		final Map<String, Boolean> propertiesMultivaluesMap = new HashMap<>();
		for (final Row row : properties) {
			propertiesMultivaluesMap.put(row.getString(0), row.getInt(1) == 1);
		}
		return handleCaseInsensitivePredicate(propertiesMultivaluesMap);
	}

	/**
	 * Generates a property table dataset.
	 *
	 * @param propertiesCardinalities Mapping of all properties to their complexity.
	 * @param keyColumnName           The column name in the TT that will be the
	 *                                primary key in the PT.
	 * @param valuesColumnName        The column name in TT that will contains the
	 *                                names of the values columns in the final PT.
	 * @return A PT dataset.
	 */
	Dataset<Row> createPropertyTableDataset(final Map<String, Boolean> propertiesCardinalities,
											final String keyColumnName, final String valuesColumnName) {
		logger.info("Building the complete property table.");

		// create a new aggregation environment
		final PropertiesAggregateFunction aggregator = new PropertiesAggregateFunction(
				propertiesCardinalities.keySet().toArray(new String[0]), COLUMNS_SEPARATOR);

		final String PREDICATE_OBJECT_COLUMN = "po";
		final String GROUP_COLUMN = "group";

		// get the compressed table
		final Dataset<Row> compressedTriples = spark.sql(String.format("SELECT %s, CONCAT(%s, '%s', %s) AS po FROM %s",
				keyColumnName, COLUMN_NAME_PREDICATE, COLUMNS_SEPARATOR, valuesColumnName, TRIPLETABLE_NAME));

		// group by the subject and get all the data
		final Dataset<Row> grouped = compressedTriples.groupBy(keyColumnName)
				.agg(aggregator.apply(compressedTriples.col(PREDICATE_OBJECT_COLUMN)).alias(GROUP_COLUMN));

		// build the query to extract the property from the array
		final List<String> propertiesList = new ArrayList<>();
		propertiesList.add(keyColumnName);
		for (final String propertyName : propertiesCardinalities.keySet()) {
			// if property is a full URI, remove the < at the beginning end > at the end
			final String rawProperty = propertyName.startsWith("<") && propertyName.endsWith(">")
					? propertyName.substring(1, propertyName.length() - 1)
					: propertyName;
			// if is not a complex type, extract the value
			final String newProperty = propertiesCardinalities.get(propertyName)
					? " " + GROUP_COLUMN + "[" + (propertiesList.size() - 1) + "] AS "
					+ getValidHiveName(rawProperty)
					: " " + GROUP_COLUMN + "[" + (propertiesList.size() - 1) + "][0] AS "
					+ getValidHiveName(rawProperty);
			propertiesList.add(newProperty);
		}

		//logger.info("Columns of  Property Table: " + propertiesList);

		return grouped.selectExpr(propertiesList.toArray(new String[0]));
	}
}
