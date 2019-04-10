package stats;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.collect_list;
import static org.apache.spark.sql.functions.explode;
import static org.apache.spark.sql.functions.monotonically_increasing_id;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;



/**
 * Handles statistical information about a whole database.
 */
public class DatabaseStatistics {
	private String databaseName;
	private HashMap<String, PropertyStatistics> properties;
	private ArrayList<CharacteristicSetStatistics> characteristicSets;

	public DatabaseStatistics() {

	}

	public DatabaseStatistics(final String databaseName) {
		this.databaseName = databaseName;
		this.properties = new HashMap<>();
		this.characteristicSets = new ArrayList<>();
	}

	public void saveToFile(final String path) {

		final Gson gson = new GsonBuilder().setPrettyPrinting().create();
		final String json = gson.toJson(this);
		try {
			final FileWriter writer = new FileWriter(path);
			writer.write(json);
			writer.close();
		} catch (final IOException e) {
			e.printStackTrace();
		}
	}

	public void loadFromFile(final String path) {
		final Gson gson = new Gson();
		try {
			final BufferedReader br = new BufferedReader(new FileReader(path));
			final Type type = new TypeToken<HashMap<String, PropertyStatistics>>() {
			}.getType();


			final DatabaseStatistics ds2 = gson.fromJson(br, type);
			this.databaseName = ds2.databaseName;
			this.properties = ds2.properties;

		} catch (final IOException e) {
			e.printStackTrace();
		}
	}

	public void computeCharacteristicSetsStatistics(final Dataset<Row> tripletable){
		final Dataset<Row> subjectCharSet = tripletable.select(col("s"), col("p")).groupBy(col("s"))
				.agg(collect_list(col("p")).alias("charSet"));
		Dataset<Row> charSets = subjectCharSet.select(col("charSet")).distinct();
		// add index to each set
		charSets = charSets.withColumn("id", monotonically_increasing_id()).withColumn("p", explode(col("charSet")));
		// join with TT based on p
		charSets = charSets.join(tripletable, "p");
		// calculate the predicate set count for each set
		final Dataset<Row> charSetPredicateStats = charSets.groupBy("id", "p").count().alias("pred_stats").drop("s",
				"o");
		final Dataset<Row> charSetSubject = charSets.select("id", "s").distinct().groupBy("id").count()
				.alias("distinct_subjects").drop("s");
		final List<Row> charSetSubjectCount = charSetSubject.collectAsList();

		for (final Row row : charSetSubjectCount) {
			final CharacteristicSetStatistics characteristicSetStatistics = new CharacteristicSetStatistics();
			final Long charSetId = row.getLong(0);
			final Long distinctSubjects = row.getLong(1);
			characteristicSetStatistics.setDistinctSubjects(distinctSubjects);

			// TODO  predicates are not being retrieved
			final List<Row> predicateStats = charSetPredicateStats.where("id ==" + charSetId).collectAsList();
			for (final Row row2 : predicateStats) {
				final String predicate = row2.getString(1);
				final long predicateCount = row2.getLong(2);
				characteristicSetStatistics.getTuplesPerPredicate().put(predicate, predicateCount);
			}
			characteristicSets.add(characteristicSetStatistics);
		}
	}

	public HashMap<String, PropertyStatistics> getProperties() {
		return properties;
	}

}
