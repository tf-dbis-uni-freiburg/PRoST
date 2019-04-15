package stats;

import static org.apache.spark.sql.functions.array;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.collect_list;
import static org.apache.spark.sql.functions.collect_set;
import static org.apache.spark.sql.functions.count;
import static org.apache.spark.sql.functions.explode;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.HashMap;

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

	public void computeCharacteristicSetsStatistics(final Dataset<Row> tripletable) {

		Dataset<Row> charSets = tripletable.select("s", "p");

		charSets =
				charSets.groupBy("s").agg(collect_set("p").as("charSet"), collect_list("p").as("predicates"));

		charSets = charSets.groupBy("charSet").agg(count("s").as("subjectCount"),
				collect_list("predicates").as("predicates"));
		charSets = charSets.withColumn("predicates", explode(col("predicates")));
		charSets = charSets.withColumn("predicates", explode(col("predicates")));
		charSets = charSets.groupBy("charSet", "subjectCount", "predicates").agg(count("predicates"));

		charSets = charSets.withColumn("tuplesPerPredicate", array(col("predicates"), col("count(predicates)")));

		charSets = charSets.groupBy("charSet", "subjectCount").agg(collect_list("tuplesPerPredicate"));

		//TODO save statistics
		/*List<Row> collectedCharSets = charSets.collectAsList();
		for (charSet: collectedCharSets){

		}*/


	}

	public HashMap<String, PropertyStatistics> getProperties() {
		return properties;
	}

}
