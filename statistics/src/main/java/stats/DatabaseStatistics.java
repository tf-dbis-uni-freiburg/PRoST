package stats;

import static org.apache.spark.sql.functions.array;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.collect_list;
import static org.apache.spark.sql.functions.collect_set;
import static org.apache.spark.sql.functions.count;
import static org.apache.spark.sql.functions.explode;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import scala.collection.Iterator;
import scala.collection.immutable.Vector;
import scala.collection.mutable.WrappedArray;


/**
 * Handles statistical information about a whole database.
 */
public class DatabaseStatistics {
	private String databaseName;
	private Long tuplesNumber;
	private HashMap<String, PropertyStatistics> properties;
	private ArrayList<CharacteristicSetStatistics> characteristicSets;

	private Boolean hasTT = false;
	private Boolean hasVPTables = false;
	private Boolean hasWPT = false;
	private Boolean hasIWPT = false;
	private Boolean hasJWPTOuter = false;
	private Boolean hasJWPTInner = false;
	private Boolean hasJWPTLeftOuter = false;

	private Boolean ttPartitionedByPredicate;
	private Boolean ttPartitionedBySubject;
	private Boolean wptPartitionedBySubject;
	private Boolean iwptPartitionedByObject;
	private Boolean jwptPartitionedByResource;
	private Boolean vpPartitionedBySubject;


	public DatabaseStatistics(final String databaseName) {
		this.databaseName = databaseName;
		this.tuplesNumber = Long.valueOf("0");
		this.properties = new HashMap<>();
		this.characteristicSets = new ArrayList<>();
	}

	public static DatabaseStatistics loadFromFile(final String path) {
		final Gson gson = new Gson();
		try {
			final BufferedReader br = new BufferedReader(new FileReader(path));
			return gson.fromJson(br, DatabaseStatistics.class);
		} catch (final FileNotFoundException e) {

			return new DatabaseStatistics(path.substring(0, path.length() - 5));//remove the substring ".json"
		}
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

	/*
		The goal is to compute the number of distinct subjects for each characteristic set, and the number of tuples
		for each property of the characteristic set.

		initial schema: s:string, charSet:array<string>, predicates:array<string>; with charSet the set of
		predicates, and predicates the list of predicates. That is, predicates might contain duplicates

		final schema: charSet:array<string>, distinctSubjects:long, tuplesPerPredicate:array<array<string>> ->
		arrays of the type <<"propertyName","count">,...,<"pn","cn">>
	 */
	public void computeCharacteristicSetsStatistics(final SparkSession spark) {
		spark.sql("USE " + databaseName);
		final Dataset<Row> tripletable = spark.sql("select * from tripletable");

		Dataset<Row> characteristicSets = tripletable.groupBy("s").agg(collect_set("p").as("charSet"), collect_list(
				"p").as("predicates"));
		characteristicSets = characteristicSets.groupBy("charSet").agg(count("s").as("subjectCount"),
				collect_list("predicates").as("predicates"));
		// the distinct list of predicates are exploded so they can be grouped and counted (distinct predicate lists
		// of the same charSet are different rows
		characteristicSets = characteristicSets.withColumn("predicates", explode(col("predicates")));
		characteristicSets = characteristicSets.withColumn("predicates", explode(col("predicates")));
		// the string predicate must be kept to be added to the final array together with its count
		characteristicSets = characteristicSets.groupBy("charSet", "subjectCount", "predicates").agg(count(
				"predicates"));
		characteristicSets = characteristicSets.withColumn("tuplesPerPredicate", array(col("predicates"), col("count"
				+ "(predicates)")));
		characteristicSets = characteristicSets.groupBy("charSet", "subjectCount").agg(collect_list(
				"tuplesPerPredicate").as("tuplesPerPredicate"));

		final List<Row> collectedCharSets = characteristicSets.collectAsList();
		for (final Row charSet : collectedCharSets) {
			final CharacteristicSetStatistics characteristicSetStatistics = new CharacteristicSetStatistics();
			characteristicSetStatistics.setDistinctSubjects(charSet.getAs("subjectCount"));

			final WrappedArray<WrappedArray<String>> properties = charSet.getAs("tuplesPerPredicate");
			final Iterator<WrappedArray<String>> iterator = properties.toIterator();
			while (iterator.hasNext()) {
				final Vector<String> v = iterator.next().toVector();
				characteristicSetStatistics.getTuplesPerPredicate().put(v.getElem(0, 1), Long.valueOf(v.getElem(1, 1)));
			}
			this.characteristicSets.add(characteristicSetStatistics);
		}

		//tuplesNumber!=0 if the tripletable was loaded with PRoST
		if (this.tuplesNumber == 0) {
			this.setTuplesNumber(tripletable.count());
		}
	}

	public void computePropertyStatistics(final SparkSession spark) {
		spark.sql("USE " + databaseName);
		final String[] propertiesNames = extractProperties(spark);

		for (final String property : propertiesNames) {
			final Dataset<Row> vpTableDataset = spark.sql("SELECT * FROM " + "vp_" + getValidHiveName(property));
			this.getProperties().put(property, new PropertyStatistics(vpTableDataset,
					getValidHiveName(property)));
		}
	}

	private String getValidHiveName(final String columnName) {
		return columnName.replaceAll("[<>]", "").trim().replaceAll("[[^\\w]+]", "_");
	}

	private String[] extractProperties(final SparkSession spark) {
		final List<Row> props = spark
				.sql("SELECT DISTINCT(p) AS p FROM tripletable")
				.collectAsList();
		final String[] properties = new String[props.size()];
		for (int i = 0; i < props.size(); i++) {
			properties[i] = props.get(i).getString(0);
		}
		return handleCaseInsensitivePredicates(properties);
	}

	private String[] handleCaseInsensitivePredicates(final String[] properties) {
		final Set<String> seenPredicates = new HashSet<>();
		final Set<String> originalRemovedPredicates = new HashSet<>();
		final Set<String> propertiesSet = new HashSet<>(Arrays.asList(properties));

		for (final String predicate : propertiesSet) {
			if (seenPredicates.contains(predicate.toLowerCase())) {
				originalRemovedPredicates.add(predicate);
			} else {
				seenPredicates.add(predicate.toLowerCase());
			}
		}
		for (final String predicateToBeRemoved : originalRemovedPredicates) {
			propertiesSet.remove(predicateToBeRemoved);
		}

		return propertiesSet.toArray(new String[0]);
	}

	public HashMap<String, PropertyStatistics> getProperties() {
		return properties;
	}

	public ArrayList<CharacteristicSetStatistics> getCharacteristicSets() {
		return characteristicSets;
	}

	public void setTuplesNumber(final Long tuplesNumber) {
		this.tuplesNumber = tuplesNumber;
	}

	public long getTuplesNumber(){
		return tuplesNumber;
	}

	public Boolean hasTT() {
		return hasTT;
	}

	public void setHasTT(final Boolean hasTT) {
		this.hasTT = hasTT;
	}

	public Boolean hasVPTables() {
		return hasVPTables;
	}

	public void setHasVPTables(final Boolean hasVPTables) {
		this.hasVPTables = hasVPTables;
	}

	public Boolean hasWPT() {
		return hasWPT;
	}

	public void setHasWPT(final Boolean hasWPT) {
		this.hasWPT = hasWPT;
	}

	public Boolean hasIWPT() {
		return hasIWPT;
	}

	public void setHasIWPT(final Boolean hasIWPT) {
		this.hasIWPT = hasIWPT;
	}

	public Boolean hasJWPTOuter() {
		return hasJWPTOuter;
	}

	public void setHasJWPTOuter(final Boolean hasJWPTOuter) {
		this.hasJWPTOuter = hasJWPTOuter;
	}

	public Boolean hasJWPTInner() {
		return hasJWPTInner;
	}

	public void setHasJWPTInner(final Boolean hasJWPTInner) {
		this.hasJWPTInner = hasJWPTInner;
	}

	public void setHasJWPTLeftOuter(final Boolean hasJWPTLeftOuter) {
		this.hasJWPTLeftOuter = hasJWPTLeftOuter;
	}

	public void setTtPartitionedByPredicate(final Boolean ttPartitionedByPredicate) {
		this.ttPartitionedByPredicate = ttPartitionedByPredicate;
	}

	public void setTtPartitionedBySubject(final Boolean ttPartitionedBySubject) {
		this.ttPartitionedBySubject = ttPartitionedBySubject;
	}

	public void setWptPartitionedBySubject(final Boolean wptPartitionedBySubject) {
		this.wptPartitionedBySubject = wptPartitionedBySubject;
	}

	public void setIwptPartitionedByObject(final Boolean iwptPartitionedByObject) {
		this.iwptPartitionedByObject = iwptPartitionedByObject;
	}

	public void setJwptPartitionedByResource(final Boolean jwptPartitionedByResource) {
		this.jwptPartitionedByResource = jwptPartitionedByResource;
	}

	public void setVpPartitionedBySubject(final Boolean vpPartitionedBySubject) {
		this.vpPartitionedBySubject = vpPartitionedBySubject;
	}
}
