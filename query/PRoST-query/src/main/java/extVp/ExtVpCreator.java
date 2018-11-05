package extVp;

import java.util.List;
import java.util.ListIterator;

import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.shared.PrefixMapping;

import joinTree.ElementType;
import joinTree.TriplePattern;
import translator.Stats;

/**
 * Class for the creation and maintenance of ExtVP database and tables.
 *
 */
public class ExtVpCreator {
	private static final Logger logger = Logger.getLogger("PRoST");

	/**
	 * Types of semi-join tables.
	 */
	public enum ExtVPType {
		SS, SO, OS, OO
	}

	/**
	 * Creates a ExtVP table.
	 *
	 * <p>
	 * Creates a ExtVP table for the given predicates, only if they do not exists in the
	 * database. Creates a new ExtVP database if it does not exists already. Updates the given
	 * ExtVP database statistics object. Only creates the ExtVP table for the predicate1.
	 * </p>
	 *
	 *
	 * @param predicate1
	 *            first predicate
	 * @param predicate2
	 *            second predicate
	 * @param extVPType
	 *            Type of the ExtVP table to be created.
	 * @param spark
	 *            spark session environment variable
	 * @param databaseStatistics
	 *            semi-joins database statistics
	 * @param extVPDatabaseName
	 *            semi-joins database name
	 * @return The created table name. Blank if none created.
	 */
	public static String createExtVPTable(final String predicate1, final String predicate2, final ExtVPType extVPType,
			final SparkSession spark, final DatabaseStatistics databaseStatistics, final String extVPDatabaseName) {
		final String vp1TableName = "vp_" + Stats.getInstance().findTableName(predicate1);
		final String vp2TableName = "vp_" + Stats.getInstance().findTableName(predicate2);
		final String extVpTableName = getExtVPTableName(predicate1, predicate2, extVPType);
		String createdTableName = "";

		final String tableNameWithDatabaseIdentifier = extVPDatabaseName + "." + extVpTableName;

		spark.sql("CREATE DATABASE IF NOT EXISTS " + extVPDatabaseName);

		if (!databaseStatistics.getTables().containsKey(extVpTableName) ||
				(databaseStatistics.getTables().containsKey(extVpTableName) && !databaseStatistics.getTables().get(extVpTableName).getTableExists())){

		//if (!spark.catalog().tableExists(tableNameWithDatabaseIdentifier)) {
			logger.info("table " + tableNameWithDatabaseIdentifier + " does not exist. Creating it.");
			final String createTableQuery =
					String.format("create table if not exists %1$s(s string, o string) stored as parquet",
							tableNameWithDatabaseIdentifier);

			spark.sql(createTableQuery);

			String queryOnCommand = "";
			switch (extVPType) {
			case SO:
				queryOnCommand = String.format("%1$s.s=%2$s.o", vp1TableName, vp2TableName);
				break;
			case OS:
				queryOnCommand = String.format("%1$s.o=%2$s.s", vp1TableName, vp2TableName);
				break;
			case OO:
				queryOnCommand = String.format("%1$s.o=%2$s.o", vp1TableName, vp2TableName);
				break;
			case SS:
			default:
				queryOnCommand = String.format("%1$s.s=%2$s.s", vp1TableName, vp2TableName);
				break;
			}
			final String insertDataQuery = String.format(
					"insert overwrite table %1$s select %2$s.s as "
							+ "s, %2$s.o as o from %2$s left semi join %3$s on %4$s",
					tableNameWithDatabaseIdentifier, vp1TableName, vp2TableName, queryOnCommand);
			spark.sql(insertDataQuery);

			logger.info("ExtVP: " + tableNameWithDatabaseIdentifier + " created.");

			final TableStatistic tableStatistic = databaseStatistics.getTables().get(extVpTableName);
			if (tableStatistic != null) {
				tableStatistic.setTableExists(true);
				logger.info("UPDATED statistics for table " + extVpTableName);
			} else {
				// Protobuff stats for VP tables size are not correctly saved, therefore we check the size
				// directly from
				// the hive table
				final Dataset<Row> vpDF = spark.sql("SELECT * FROM " + vp1TableName);
				final Long vpTableSize = vpDF.count();

				final Dataset<Row> extVPDataFrame = spark.sql("SELECT * FROM " + tableNameWithDatabaseIdentifier);
				final Long extVPSize = extVPDataFrame.count();

				// logger.info("size: " + extVPSize + " - selectivity: " +
				// (float)extVPSize/(float)vpTableSize + " vp
				// table size: " + vpTableSize);

				databaseStatistics.getTables().put(extVpTableName,
						new TableStatistic(extVpTableName, (float) extVPSize / (float) vpTableSize, extVPSize));
				databaseStatistics.setSize(databaseStatistics.getSize() + extVPSize);

				logger.info("CREATED statistics for table " + extVpTableName);
			}
		}
		createdTableName = extVpTableName;
		return createdTableName;
	}

	/**
	 * Creates an ExtVP table for the give TriplePatterns, if possible. Only creates the ExtVP
	 * table for the pattern1.
	 *
	 *
	 * @param pattern1
	 *            first triple pattern
	 * @param pattern2
	 *            second triple pattern
	 * @param spark
	 *            spark environment variable
	 * @param databaseStatistics
	 *            semi-joins database statistics
	 * @param extVPDatabaseName
	 *            name of semi-joins database
	 * @param prefixes
	 *            PrefixMapping used by the TriplePatterns
	 * @return The created table name. Blank if none created.
	 */
	// TODO check if patterns have variables. Selecting extvp tables from statistics should
	// also check for variables, if
	// the database only contains a partial join (check hash)
	public String createExtVPTable(final TriplePattern pattern1, final TriplePattern pattern2, final SparkSession spark,
			final DatabaseStatistics databaseStatistics, final String extVPDatabaseName, final PrefixMapping prefixes) {
		String extVpTableName = "";

		// TODO check statistics if the ExtVP table was created before, and only creates it again
		// if selectivity is high
		// enough.

		if (pattern1.subjectType == ElementType.VARIABLE && pattern2.subjectType == ElementType.VARIABLE
				&& pattern1.subject.equals(pattern2.subject)) {
			final ExtVPType vpType = ExtVPType.SS;
			extVpTableName = createExtVPTable(pattern1.triple.getPredicate().toString(prefixes),
					pattern2.triple.getPredicate().toString(prefixes), vpType, spark, databaseStatistics,
					extVPDatabaseName);
		} else if (pattern1.objectType == ElementType.VARIABLE && pattern2.objectType == ElementType.VARIABLE
				&& pattern1.object.equals(pattern2.object)) {
			final ExtVPType vpType = ExtVPType.OO;
			extVpTableName = createExtVPTable(pattern1.triple.getPredicate().toString(prefixes),
					pattern2.triple.getPredicate().toString(prefixes), vpType, spark, databaseStatistics,
					extVPDatabaseName);
		} else if (pattern1.objectType == ElementType.VARIABLE && pattern2.subjectType == ElementType.VARIABLE
				&& pattern1.object.equals(pattern2.subject)) {
			final ExtVPType vpType = ExtVPType.OS;
			extVpTableName = createExtVPTable(pattern1.triple.getPredicate().toString(prefixes),
					pattern2.triple.getPredicate().toString(prefixes), vpType, spark, databaseStatistics,
					extVPDatabaseName);
			// vpType = extVPType.SO;
			// extVpTableName = getExtVPTableName(pattern2.predicate, pattern1.predicate, vpType);
			// createExtVPTable(pattern2.predicate, pattern1.predicate, vpType, spark,
			// databaseStatistics,
			// extVPDatabaseName);
		} else if (pattern1.subjectType == ElementType.VARIABLE && pattern2.objectType == ElementType.VARIABLE
				&& pattern1.subject.equals(pattern2.object)) {
			final ExtVPType vpType = ExtVPType.SO;
			extVpTableName = createExtVPTable(pattern1.triple.getPredicate().toString(prefixes),
					pattern2.triple.getPredicate().toString(prefixes), vpType, spark, databaseStatistics,
					extVPDatabaseName);
			// vpType = extVPType.OS;
			// extVpTableName = getExtVPTableName(pattern2.predicate, pattern1.predicate, vpType);
			// createExtVPTable(pattern2.predicate, pattern1.predicate, vpType, spark,
			// databaseStatistics,
			// extVPDatabaseName);
		}
		return extVpTableName;
	}

	/**
	 * Creates possible ExtVP tables for the give triples list.
	 *
	 * <p>
	 * Given the list of triples of a query, create all possible extVP tables if they have not
	 * been created before
	 * </p>
	 *
	 * @param triples
	 *            list of original triples
	 * @param prefixes
	 *            prefix mapping of triple patterns
	 * @param spark
	 *            spark environment variable
	 * @param databaseStatistic
	 *            semi-join statistics
	 * @param extVPDatabaseName
	 *            semi-joins database name
	 */
	public void createExtVPFromTriples(final List<Triple> triples, final PrefixMapping prefixes,
			final SparkSession spark, final DatabaseStatistics databaseStatistic, final String extVPDatabaseName) {
		for (final ListIterator<Triple> outerTriplesListIterator = triples.listIterator(); outerTriplesListIterator
				.hasNext();) {
			final Triple outerTriple = outerTriplesListIterator.next();
			final String outerSubject = outerTriple.getSubject().toString(prefixes);
			final String outerPredicate = outerTriple.getPredicate().toString(prefixes);
			final String outerObject = outerTriple.getObject().toString(prefixes);

			for (final ListIterator<Triple> innerTriplesListIterator =
					triples.listIterator(outerTriplesListIterator.nextIndex()); innerTriplesListIterator.hasNext();) {
				final Triple innerTriple = innerTriplesListIterator.next();
				final String innerSubject = innerTriple.getSubject().toString(prefixes);
				final String innerPredicate = innerTriple.getPredicate().toString(prefixes);
				final String innerObject = innerTriple.getObject().toString(prefixes);

				// TODO Check if table exists in the statistics. If it exists, do not create it again, if
				// selectivity
				// not high enough

				if (outerTriple.getSubject().isVariable() && innerTriple.getSubject().isVariable()
						&& outerSubject.equals(innerSubject)) {
					// SS
					createExtVPTable(outerPredicate, innerPredicate, ExtVPType.SS, spark, databaseStatistic,
							extVPDatabaseName);
					createExtVPTable(innerPredicate, outerPredicate, ExtVPType.SS, spark, databaseStatistic,
							extVPDatabaseName);
				}
				if (outerTriple.getObject().isVariable() && innerTriple.getObject().isVariable()
						&& outerObject.equals(innerObject)) {
					// OO
					createExtVPTable(outerPredicate, innerPredicate, ExtVPType.OO, spark, databaseStatistic,
							extVPDatabaseName);
					createExtVPTable(innerPredicate, outerPredicate, ExtVPType.OO, spark, databaseStatistic,
							extVPDatabaseName);
				}
				if (outerTriple.getObject().isVariable() && innerTriple.getSubject().isVariable()
						&& outerObject.equals(innerSubject)) {
					// OS
					createExtVPTable(outerPredicate, innerPredicate, ExtVPType.OS, spark, databaseStatistic,
							extVPDatabaseName);
					createExtVPTable(innerPredicate, outerPredicate, ExtVPType.SO, spark, databaseStatistic,
							extVPDatabaseName);
				}
				if (outerTriple.getSubject().isVariable() && innerTriple.getObject().isVariable()
						&& outerSubject.equals(innerObject)) {
					// SO
					createExtVPTable(outerPredicate, innerPredicate, ExtVPType.SO, spark, databaseStatistic,
							extVPDatabaseName);
					createExtVPTable(innerPredicate, outerPredicate, ExtVPType.OS, spark, databaseStatistic,
							extVPDatabaseName);
				}
			}

		}
	}

	public static String getValidHiveName(final String columnName) {
		return columnName.replaceAll("[<>]", "").trim().replaceAll("[[^\\w]+]", "_");
	}

	/**
	 * Creates a ExtVP table name for the given predicates and join type.
	 *
	 * <p>
	 * Format of generated table name is
	 * extVP_<i>type</i>_<i>predicate1</i>__<i>predicate2</i>
	 * </p>
	 *
	 * @param predicate1
	 *            first predicate
	 * @param predicate2
	 *            second predicate
	 * @param type
	 *            Join type
	 * @return name of ExtVP table
	 */
	public static String getExtVPTableName(final String predicate1, final String predicate2, final ExtVPType type) {
		final String extVpTableName =
				"extVP_" + type.toString() + "_" + getValidHiveName(predicate1) + "__" + getValidHiveName(predicate2);

		return extVpTableName;
	}

	// TODO function to insert intermediate data in a table (receives Dataset<Row>
}
