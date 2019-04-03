package stats;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.collect_list;
import static org.apache.spark.sql.functions.explode;
import static org.apache.spark.sql.functions.monotonically_increasing_id;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.Vector;

import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import stats.ProtobufStats.CharacteristicSet;
import stats.ProtobufStats.Graph;
import stats.ProtobufStats.TableStats;

public class StatisticsWriter {

	protected static final Logger logger = Logger.getLogger("PRoST");

	// single instance of the statistics
	private static StatisticsWriter instance = null;

	private boolean useStatistics = false;
	private String stats_file_suffix = ".stats";
	private Vector<TableStats> tableStatistics;
	private Vector<CharacteristicSet> characteristicSets;

	protected StatisticsWriter() {
		// Exists only to defeat instantiation.
	}

	public static StatisticsWriter getInstance() {
		if (instance == null) {
			instance = new StatisticsWriter();
			instance.tableStatistics = new Vector<TableStats>();
			instance.characteristicSets = new Vector<CharacteristicSet>();
		}
		return instance;
	}

	/*
	 * Save the statistics in a serialized file
	 */
	public void saveStatistics(final String fileName) {
		// if statistics are not needed
		if (!useStatistics) {
			return;
		}

		final Graph.Builder graph_stats_builder = Graph.newBuilder();
		// add table statistics
		if (tableStatistics != null) {
			graph_stats_builder.addAllTables(this.tableStatistics);
		}
		if (characteristicSets != null) {
			graph_stats_builder.addAllCharacteristicSets(this.characteristicSets);
		}
		final Graph serializedStats = graph_stats_builder.build();
		final FileOutputStream fStream; // s
		final File file;
		try {
			file = new File(fileName + stats_file_suffix);
			fStream = new FileOutputStream(file);
			serializedStats.writeTo(fStream);
		} catch (final IOException e) {
			e.printStackTrace();
		}
	}

	/*
	 * Calculate the statistics for a single table: size, number of distinct
	 * subjects and isComplex. It returns a protobuf object defined in
	 * ProtobufStats.proto
	 */
	public void addStatsTable(final Dataset<Row> table, final String tableName, final String subjectColumnName) {
		// if statistics are not needed
		if (!useStatistics) {
			return;
		}

		final TableStats.Builder table_stats_builder = TableStats.newBuilder();

		// calculate the stats
		final int table_size = (int) table.count();
		final int distinct_subjects = (int) table.select(subjectColumnName).distinct().count();
		final boolean is_complex = table_size != distinct_subjects;

		table_stats_builder.setSize(table_size).setDistinctSubjects(distinct_subjects).setIsComplex(is_complex)
				.setName(tableName);

		if (table.sparkSession().catalog().tableExists("inverse_properties")) {
			final String query = "select is_complex from inverse_properties where p='" + tableName + "'";
			final boolean isInverseComplex = table.sparkSession().sql(query).head().getInt(0) == 1;
			// put them in the protobuf object
			table_stats_builder.setIsInverseComplex(isInverseComplex);
		}

		logger.info(
				"Adding these properties to Protobuf object. Table size:" + table_size + ", " + "Distinct subjects: "
						+ distinct_subjects + ", Is complex:" + is_complex + ", " + "tableName:" + tableName);

		tableStatistics.add(table_stats_builder.build());
	}

	/**
	 * Calculate the characteristic sets and store them into stats file.
	 *
	 * @param triples
	 */
	public void addCharacteristicSetsStats(final Dataset<Row> triples) {
		final Dataset<Row> subjectCharSet = triples.select(col("s"), col("p")).groupBy(col("s"))
				.agg(collect_list(col("p")).alias("charSet"));
		Dataset<Row> charSets = subjectCharSet.select(col("charSet")).distinct();
		// add index to each set
		charSets = charSets.withColumn("id", monotonically_increasing_id()).withColumn("p", explode(col("charSet")));
		// join with TT based on p
		charSets = charSets.join(triples, "p");
		// calculate the predicate set count for each set
		final Dataset<Row> charSetPredicateStats = charSets.groupBy("id", "p").count().alias("pred_stats").drop("s", "o");
		final Dataset<Row> charSetSubject = charSets.select("id", "s").distinct().groupBy("id").count()
				.alias("distinct_subjects").drop("s");
		final List<Row> charSetSubjectCount = charSetSubject.collectAsList();
		for (final Row row : charSetSubjectCount) {
			final CharacteristicSet.Builder charSetStatsBuilder = CharacteristicSet.newBuilder();

			//TODO update protobuf file to long values
			final int charSetId = (int) row.getLong(0);
			final int distinctSubjects = (int) row.getLong(1);
			charSetStatsBuilder.setDistinctSubjectsCount(distinctSubjects);
			final List<Row> predicateStats = charSetPredicateStats.where("id ==" + charSetId).collectAsList();
			for (final Row row2 : predicateStats) {
				final String predicate = row2.getString(1);
				final int predicateCount = row2.getInt(2);
				charSetStatsBuilder.getMutableTriplesPerPredicate().put(predicate, predicateCount);
			}
			// save into stats file
			characteristicSets.add(charSetStatsBuilder.build());
		}
	}

	public List<TableStats> getTableStatistics() {
		return tableStatistics;
	}

	public void setTableStatistics(final Vector<TableStats> tableStatistics) {
		this.tableStatistics = tableStatistics;
	}

	public boolean isUseStatistics() {
		return useStatistics;
	}

	public void setUseStatistics(final boolean useStatistics) {
		this.useStatistics = useStatistics;
	}

	public Vector<CharacteristicSet> getCharacteristicSets() {
		return characteristicSets;
	}

	public void setCharacteristicSets(final Vector<CharacteristicSet> characteristicSets) {
		this.characteristicSets = characteristicSets;
	}
}
