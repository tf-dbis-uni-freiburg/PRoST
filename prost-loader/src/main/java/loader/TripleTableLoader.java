package loader;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import statistics.DatabaseStatistics;

/**
 * Class that constructs a triples table. First, the loader creates an external
 * table ("raw"). The data is read using SerDe capabilities and by means of a
 * regular expression. An additional table ("fixed") is created to make sure that
 * only valid triples are passed to the next stages in which other models e.g.
 * Property Table, or Vertical Partitioning are built.
 *
 * @author Matteo Cossu
 * @author Victor Anthony Arrascue Ayala
 */
public class TripleTableLoader extends Loader {
	private final boolean ttPartitionedBySubject;
	private final boolean ttPartitionedByPredicate;
	private final boolean dropDuplicates;
	private final String hdfsInputDirectory;

	public TripleTableLoader(final Settings settings, final SparkSession spark, final DatabaseStatistics statistics) {
		super(settings.getDatabaseName(), spark, statistics);
		this.hdfsInputDirectory = settings.getInputPath();
		this.ttPartitionedBySubject = settings.isTtPartitionedBySubject();
		this.ttPartitionedByPredicate = settings.isTtPartitionedByPredicate();
		this.dropDuplicates = settings.isDroppingDuplicateTriples();
	}

	@SuppressWarnings("CheckStyle")
	@Override
	public void load() throws Exception {
		final String queryDropTripleTable = String.format("DROP TABLE IF EXISTS %s", TRIPLETABLE_NAME);
		String createTripleTableFixed = null;
		String repairTripleTableFixed = null;

		spark.sql(queryDropTripleTable);

		final String createTripleTableRaw = String.format(
				"CREATE EXTERNAL TABLE IF NOT EXISTS %1$s(%2$s STRING, %3$s STRING, %4$s STRING) ROW FORMAT SERDE "
						+ "'org.apache.hadoop.hive.serde2.RegexSerDe'  WITH SERDEPROPERTIES "
						+ "( \"input.regex\" = \"(\\\\S+)\\\\s+(\\\\S+)\\\\s+(.+)\\\\s*\\\\.\\\\s*$\")"
						+ "LOCATION '%5$s'",
				TRIPLETABLE_NAME + "_ext", COLUMN_NAME_SUBJECT, COLUMN_NAME_PREDICATE, COLUMN_NAME_OBJECT,
				hdfsInputDirectory);
		spark.sql(createTripleTableRaw);


		if (ttPartitionedBySubject) {
			createTripleTableFixed = String.format(
					"CREATE TABLE  IF NOT EXISTS  %1$s(%3$s STRING, %4$s STRING) "
							+ "PARTITIONED BY (%2$s STRING) STORED AS PARQUET",
					TRIPLETABLE_NAME, COLUMN_NAME_SUBJECT, COLUMN_NAME_PREDICATE, COLUMN_NAME_OBJECT);
		} else if (ttPartitionedByPredicate) {
			createTripleTableFixed = String.format(
					"CREATE TABLE  IF NOT EXISTS  %1$s(%2$s STRING, %4$s STRING) "
							+ "PARTITIONED BY (%3$s STRING) STORED AS PARQUET",
					TRIPLETABLE_NAME, COLUMN_NAME_SUBJECT, COLUMN_NAME_PREDICATE, COLUMN_NAME_OBJECT);
		} else {
			createTripleTableFixed = String.format(
					"CREATE TABLE  IF NOT EXISTS  %1$s(%2$s STRING, %3$s STRING, %4$s STRING) STORED AS PARQUET",
					TRIPLETABLE_NAME, COLUMN_NAME_SUBJECT, COLUMN_NAME_PREDICATE, COLUMN_NAME_OBJECT);
		}
		spark.sql(createTripleTableFixed);

		String distinctStatement = "";
		if (dropDuplicates) {
			distinctStatement = "DISTINCT";
		}

		if (ttPartitionedBySubject) {
			repairTripleTableFixed = String.format(
					"INSERT OVERWRITE TABLE %1$s PARTITION (%2$s) " + "SELECT " + distinctStatement
							+ " %3$s, trim(%4$s), %2$s " + "FROM %5$s "
							+ "WHERE %2$s is not null AND %3$s is not null AND %4$s is not null AND "
							+ "NOT(%2$s RLIKE '^\\s*\\.\\s*$')  "
							+ "AND NOT(%3$s RLIKE '^\\s*\\.\\s*$') AND NOT(%4$s RLIKE '^\\s*\\.\\s*$') AND "
							+ "NOT(%4$s RLIKE '^\\s*<.*<.*>')  "
							+ "AND NOT(%4$s RLIKE '(?<!\\u005C\\u005C)\".*(?<!\\u005C\\u005C)\".*(?<!\\u005C\\u005C)"
							+ "\"') AND "
							+ "LENGTH(%3$s) < %6$s",
					TRIPLETABLE_NAME, COLUMN_NAME_SUBJECT, COLUMN_NAME_PREDICATE, COLUMN_NAME_OBJECT,
					TRIPLETABLE_NAME + "_ext", MAX_LENGTH_COL_NAME);
		} else if (ttPartitionedByPredicate) {
			repairTripleTableFixed = String.format(
					"INSERT OVERWRITE TABLE %1$s PARTITION (%3$s) " + "SELECT " + distinctStatement
							+ " %2$s, trim(%4$s), %3$s " + "FROM %5$s "
							+ "WHERE %2$s is not null AND %3$s is not null AND %4$s is not null AND "
							+ "NOT(%2$s RLIKE '^\\s*\\.\\s*$')  AND NOT(%3$s RLIKE '^\\s*\\.\\s*$') "
							+ "AND NOT(%4$s RLIKE '^\\s*\\.\\s*$') AND " + "NOT(%4$s RLIKE '^\\s*<.*<.*>')  "
							+ "AND NOT(%4$s RLIKE '(?<!\\u005C\\u005C)\".*(?<!\\u005C\\u005C)\".*(?<!\\u005C\\u005C)"
							+ "\"') AND "
							+ "LENGTH(%3$s) < %6$s",
					TRIPLETABLE_NAME, COLUMN_NAME_SUBJECT, COLUMN_NAME_PREDICATE, COLUMN_NAME_OBJECT,
					TRIPLETABLE_NAME + "_ext", MAX_LENGTH_COL_NAME);
		} else {
			repairTripleTableFixed = String.format(
					"INSERT OVERWRITE TABLE %1$s  " + "SELECT " + distinctStatement + " %2$s, %3$s, trim(%4$s)  "
							+ "FROM %5$s " + "WHERE %2$s is not null AND %3$s is not null AND %4$s is not null AND "
							+ "NOT(%2$s RLIKE '^\\s*\\.\\s*$')  AND NOT(%3$s RLIKE '^\\s*\\.\\s*$')"
							+ " AND NOT(%4$s RLIKE '^\\s*\\.\\s*$') AND " + "NOT(%4$s RLIKE '^\\s*<.*<.*>')  "
							+ "AND NOT(%4$s RLIKE '(?<!\\u005C\\u005C)\".*(?<!\\u005C\\u005C)\".*(?<!\\u005C\\u005C)"
							+ "\"') AND "
							+ "LENGTH(%3$s) < %6$s",
					TRIPLETABLE_NAME, COLUMN_NAME_SUBJECT, COLUMN_NAME_PREDICATE, COLUMN_NAME_OBJECT,
					TRIPLETABLE_NAME + "_ext", MAX_LENGTH_COL_NAME);
		}
		spark.sql(repairTripleTableFixed);

		//logger.info("Created tripletable with: " + createTripleTableRaw);
		//logger.info("Cleaned tripletable created with: " + repairTripleTableFixed);

		final String queryRawTriples = String.format("SELECT * FROM %s", TRIPLETABLE_NAME + "_ext");
		final String queryAllTriples = String.format("SELECT * FROM %s", TRIPLETABLE_NAME);
		final Dataset<Row> allTriples = spark.sql(queryAllTriples);
		final Dataset<Row> rawTriples = spark.sql(queryRawTriples);

		final long tuplesCount = allTriples.count();
		if (tuplesCount == 0) {
			logger.error("Either your HDFS path does not contain any files or "
					+ "no triples were accepted in the given format (nt)");
			logger.error("The program will stop here.");
			throw new Exception("Empty HDFS directory or empty files within.");
		} else {
			logger.info("Total number of triples loaded: " + tuplesCount);
		}

		this.getStatistics().setTuplesNumber(tuplesCount);

		// The following part just outputs to the log in case there have been
		// problems parsing the files.
		if (rawTriples.count() != allTriples.count()) {
			final Dataset<Row> triplesWithDuplicates = spark
					.sql("SELECT * FROM " + TRIPLETABLE_NAME + "_ext" + " GROUP BY " + COLUMN_NAME_SUBJECT + ", "
							+ COLUMN_NAME_PREDICATE + ", " + COLUMN_NAME_OBJECT + " HAVING COUNT(*) > 1");
			logger.info("Number of duplicates found: " + (triplesWithDuplicates.count()));

			logger.info("Number of removed triples: " + (rawTriples.count() - allTriples.count()));

			// TODO: at the moment this is just counting the number of triples
			// which could not be uploaded.
			// The idea would be to sample some of the triples which are not
			// working and write them to the log file.
			final Dataset<Row> triplesWithNullSubjects = spark
					.sql("SELECT * FROM " + TRIPLETABLE_NAME + "_ext" + " WHERE " + COLUMN_NAME_SUBJECT + " is null");
			final Dataset<Row> triplesWithNullPredicates = spark
					.sql("SELECT * FROM " + TRIPLETABLE_NAME + "_ext" + " WHERE " + COLUMN_NAME_PREDICATE + " is null");
			final Dataset<Row> triplesWithNullObjects = spark
					.sql("SELECT * FROM " + TRIPLETABLE_NAME + "_ext" + " WHERE " + COLUMN_NAME_OBJECT + " is null");
			if (triplesWithNullSubjects.count() > 0) {
				logger.info(
						"---of which " + triplesWithNullSubjects.count() + " had a null value in the subject column");
			}
			if (triplesWithNullPredicates.count() > 0) {
				logger.info("---of which " + triplesWithNullPredicates.count()
						+ " had a null value in the predicate column");
			}
			if (triplesWithNullObjects.count() > 0) {
				logger.info("---of which " + triplesWithNullObjects.count() + " had a null value in the object column");
			}
			final Dataset<Row> triplesWithMalformedSubjects = spark.sql("SELECT * FROM " + TRIPLETABLE_NAME + "_ext"
					+ " WHERE " + COLUMN_NAME_SUBJECT + " RLIKE '^\\s*\\.\\s*$'");
			final Dataset<Row> triplesWithMalformedPredicates = spark.sql("SELECT * FROM " + TRIPLETABLE_NAME + "_ext"
					+ " WHERE " + COLUMN_NAME_PREDICATE + " RLIKE '^\\s*\\.\\s*$'");
			final Dataset<Row> triplesWithMalformedObjects = spark.sql("SELECT * FROM " + TRIPLETABLE_NAME + "_ext"
					+ " WHERE " + COLUMN_NAME_OBJECT + " RLIKE '^\\s*\\.\\s*$'");
			if (triplesWithMalformedSubjects.count() > 0) {
				logger.info("---of which " + triplesWithMalformedSubjects.count() + " have malformed subjects");
			}
			if (triplesWithMalformedPredicates.count() > 0) {
				logger.info("---of which " + triplesWithMalformedPredicates.count() + " have malformed predicates");
			}
			if (triplesWithMalformedObjects.count() > 0) {
				logger.info("---of which " + triplesWithMalformedObjects.count() + " have malformed objects");
			}
			final Dataset<Row> triplesWithMultipleObjects = spark.sql(
					"SELECT * FROM " + TRIPLETABLE_NAME + "_ext" + " WHERE "
							+ COLUMN_NAME_OBJECT + " RLIKE '^\\s*<.*<.*>'");
			if (triplesWithMultipleObjects.count() > 0) {
				logger.info("---of which " + triplesWithMultipleObjects.count() + " have multiple objects");
			}
			final Dataset<Row> objectsWithMultipleLiterals = spark
					.sql("SELECT * FROM " + TRIPLETABLE_NAME + "_ext" + " WHERE " + COLUMN_NAME_OBJECT
							+ " RLIKE '(?<!\\u005C\\u005C)\".*(?<!\\u005C\\u005C)\".*(?<!\\u005C\\u005C)\"'");
			if (objectsWithMultipleLiterals.count() > 0) {
				logger.info("---of which " + objectsWithMultipleLiterals.count() + " have multiple literals");
			}
			final Dataset<Row> longPredicates = spark.sql(
					"SELECT * FROM " + TRIPLETABLE_NAME + "_ext" + " WHERE LENGTH("
							+ COLUMN_NAME_PREDICATE + ") > 128");
			if (longPredicates.count() > 0) {
				logger.info("---of which " + longPredicates.count() + " have predicates with more than 128 characters");
			}
		}
		// final List<Row> cleanedList = allTriples.limit(10).collectAsList();
		// logger.info("First 10 cleaned triples (less if there are less): " + cleanedList);
	}
}