package statistics;

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
import java.util.ListIterator;
import java.util.Map;
import java.util.Set;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;
import scala.collection.Iterator;
import scala.collection.JavaConverters;
import scala.collection.Seq;
import scala.collection.immutable.Vector;
import scala.collection.mutable.WrappedArray;

/**
 * Handles statistical information about a whole database.
 */
public class DatabaseStatistics {
	private static final String loj4jFileName = "log4j.properties";
	private static final Logger logger = Logger.getLogger("PRoST");
	private final String databaseName;
	private Long tuplesNumber;
	private final HashMap<String, PropertyStatistics> properties;
	private ArrayList<CharacteristicSetStatistics> characteristicSets;
	private ArrayList<EmergentSchemaStatistics> emergentSchemas;

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

	public static DatabaseStatistics loadFromFile(final String path) throws FileNotFoundException {
		final Gson gson = new Gson();
		try {
			final BufferedReader br = new BufferedReader(new FileReader(path));
			return gson.fromJson(br, DatabaseStatistics.class);
		} catch (final FileNotFoundException e) {
			throw new FileNotFoundException("Statistics file " + path + " does not exist.");
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
	@Deprecated
	public void computeCharacteristicSetsStatisticsFromTT(final SparkSession spark) {
		spark.sql("USE " + databaseName);
		final Dataset<Row> tripletable = spark.sql("select * from tripletable");

		Dataset<Row> characteristicSets = tripletable.groupBy("s").agg(functions.collect_set("p").as("charSet"),
				functions.collect_list("p").as("predicates"));
		characteristicSets = characteristicSets.groupBy("charSet").agg(functions.count("s").as("subjectCount"),
				functions.collect_list("predicates").as("predicates"));
		// the distinct list of predicates are exploded so they can be grouped and counted (distinct predicate lists
		// of the same charSet are different rows
		characteristicSets = characteristicSets.withColumn("predicates",
				functions.explode(functions.col("predicates")));
		characteristicSets = characteristicSets.withColumn("predicates",
				functions.explode(functions.col("predicates")));
		// the string predicate must be kept to be added to the final array together with its count
		characteristicSets = characteristicSets.groupBy("charSet", "subjectCount", "predicates").agg(functions.count(
				"predicates"));
		characteristicSets = characteristicSets.withColumn("tuplesPerPredicate",
				functions.array(functions.col("predicates"), functions.col("count(predicates)")));
		characteristicSets = characteristicSets.groupBy("charSet", "subjectCount").agg(functions.collect_list(
				"tuplesPerPredicate").as("tuplesPerPredicate"));

		final List<Row> collectedCharSets = characteristicSets.collectAsList();
		for (final Row charSet : collectedCharSets) {
			final CharacteristicSetStatistics characteristicSetStatistics = new CharacteristicSetStatistics();
			characteristicSetStatistics.setDistinctSubjects(charSet.getAs("subjectCount"));

			final WrappedArray<WrappedArray<String>> properties = charSet.getAs("tuplesPerPredicate");
			final Iterator<WrappedArray<String>> iterator = properties.toIterator();
			while (iterator.hasNext()) {
				final Vector<String> v = iterator.next().toVector();
				characteristicSetStatistics.addProperty(v.getElem(0, 1), Long.parseLong(v.getElem(1, 1)));
			}
			this.characteristicSets.add(characteristicSetStatistics);
		}

		//tuplesNumber!=0 if the tripletable was loaded with PRoST
		if (this.tuplesNumber == 0) {
			this.setTuplesNumber(tripletable.count());
		}
	}

	/**
	 * Encodes predicates to integer values.
	 *
	 * @param tripletable Dataset with the full tripletable
	 * @return Dictionary String->Integer
	 */
	private HashMap<String, Integer> encodePredicates(final Dataset<Row> tripletable) {
		final List<Row> predicates = tripletable.select(functions.col("p")).distinct().collectAsList();

		final HashMap<String, Integer> encoding = new HashMap<>();
		int code = 0;
		for (final Row row : predicates) {
			final String predicate = row.getString(0);
			encoding.put(predicate, code);
			code++;
		}
		return encoding;
	}

	/**
	 * Creates a dictionary Int->String from a dictionary String->Int to decode predicates.
	 *
	 * @param encoding the predicates encoding
	 * @return the dictionary for the decoding of predicates.
	 */
	private HashMap<Integer, String> createDecoder(final HashMap<String, Integer> encoding) {
		final HashMap<Integer, String> decoder = new HashMap<>();
		for (final Map.Entry<String, Integer> entry : encoding.entrySet()) {
			decoder.put(entry.getValue(), entry.getKey());
		}
		return decoder;
	}

	public void computeCharacteristicSetsStatistics(final SparkSession spark) {
		this.characteristicSets = new ArrayList<>(); //clears existing characteristic sets statistics

		//UDF that maps a column with a list of list of predicates to a list of list with two elements: [predicate
		// (encoded), tuples per predicate]
		spark.sqlContext().udf().register("CHARSET_UDF",
				(UDF1<Seq<Seq<Integer>>, Seq<Seq<Integer>>>) (columnValue) -> {
					final HashMap<Integer, Integer> predicatesDictionary = new HashMap<>();

					final List<Seq<Integer>> listSeq = scala.collection.JavaConversions.seqAsJavaList(columnValue);
					for (final Seq<Integer> sequence : listSeq) {
						final List<Integer> predicates = scala.collection.JavaConversions.seqAsJavaList(sequence);
						for (final Integer predicate : predicates) {

							if (predicatesDictionary.containsKey(predicate)) {
								final Integer count = predicatesDictionary.get(predicate);
								predicatesDictionary.put(predicate, count + 1);
							} else {
								predicatesDictionary.put(predicate, 1);
							}
						}
					}

					final List<Seq<Integer>> resultList = new ArrayList<>();
					for (final Map.Entry<Integer, Integer> entry : predicatesDictionary.entrySet()) {
						final List<Integer> entryAsList = new ArrayList<>();
						entryAsList.add(entry.getKey());
						entryAsList.add(entry.getValue());
						resultList.add(JavaConverters.asScalaIteratorConverter
								(entryAsList.iterator()).asScala().toSeq());
					}
					return JavaConverters.asScalaIteratorConverter(resultList.iterator()).asScala().toSeq();
				}, DataTypes.createArrayType(DataTypes.createArrayType(DataTypes.IntegerType)));

		spark.sql("USE " + databaseName);
		Dataset<Row> tripletable = spark.sql("select * from tripletable");

		final HashMap<String, Integer> encoding = encodePredicates(tripletable);

		//Encodes the properties in tripletable to avoid operations on RDDs larger than 2GB.
		spark.sqlContext().udf().register("ENCODE_UDF",
				(UDF1<String, Integer>) encoding::get, DataTypes.IntegerType);
		tripletable = tripletable.withColumn("p", functions.callUDF("ENCODE_UDF", functions.col("p")));

		Dataset<Row> characteristicSets = tripletable.groupBy("s").agg(functions.collect_set("p").as("charSet"),
				functions.collect_list("p").as("predicates"));

		characteristicSets = characteristicSets.groupBy("charSet").agg(functions.count("s").as("subjectCount"),
				functions.collect_list("predicates").as("predicates"));

		characteristicSets = characteristicSets.withColumn("tuplesPerPredicate",
				functions.callUDF("CHARSET_UDF",
						functions.col("predicates")));

		characteristicSets = characteristicSets.select("charSet", "subjectCount", "tuplesPerPredicate");

		final List<Row> collectedCharSets = characteristicSets.collectAsList();

		final HashMap<Integer, String> decoder = createDecoder(encoding);

		for (final Row charSet : collectedCharSets) {
			final CharacteristicSetStatistics characteristicSetStatistics = new CharacteristicSetStatistics();
			characteristicSetStatistics.setDistinctSubjects(charSet.getAs("subjectCount"));

			final WrappedArray<WrappedArray<Integer>> properties = charSet.getAs("tuplesPerPredicate");
			final Iterator<WrappedArray<Integer>> iterator = properties.toIterator();
			while (iterator.hasNext()) {
				final Vector<Integer> v = iterator.next().toVector();
				characteristicSetStatistics.addProperty(decoder.get(v.getElem(0, 1)),
						v.getElem(1, 1));
			}

			this.characteristicSets.add(characteristicSetStatistics);
		}

		//tuplesNumber!=0 if the tripletable was loaded with PRoST
		if (this.tuplesNumber == 0) {
			this.setTuplesNumber(tripletable.count());
		}
	}

	/**
	 * Computes the characteristic sets, without computing the number of tuples of each property. Uses the TT.
	 */
	@Deprecated
	public void computeCharacteristicSetsStatisticsWithWPT(final SparkSession spark) {
		this.characteristicSets = new ArrayList<>();
		spark.sql("USE " + databaseName);
		final Dataset<Row> tripletable = spark.sql("select * from tripletable");

		Dataset<Row> characteristicSets = tripletable.groupBy("s").agg(functions.collect_set("p").as("charSet"),
				functions.collect_list("p").as("predicates"));
		characteristicSets = characteristicSets.groupBy("charSet").agg(functions.count("s").as("subjectCount"));

		final List<Row> collectedCharSets = characteristicSets.collectAsList();
		for (final Row charSet : collectedCharSets) {
			final CharacteristicSetStatistics characteristicSetStatistics = new CharacteristicSetStatistics();
			characteristicSetStatistics.setDistinctSubjects(charSet.getAs("subjectCount"));

			final WrappedArray<String> properties = charSet.getAs("charSet");
			final Iterator<String> iterator = properties.toIterator();
			while (iterator.hasNext()) {
				final String property = iterator.next();
				//number of tuples per predicate is computed afterwards
				characteristicSetStatistics.addProperty(property, 0L);
			}
			this.characteristicSets.add(characteristicSetStatistics);
		}
		//tuplesNumber!=0 if the tripletable was loaded with PRoST
		if (this.tuplesNumber == 0) {
			this.setTuplesNumber(tripletable.count());
		}

		computeTuplesPerPredicate(spark);
	}

	/**
	 * Computes the number of tuples per property for the already computed characteristic sets using the WPT.
	 */
	private void computeTuplesPerPredicate(final SparkSession spark) {
		final ListIterator<CharacteristicSetStatistics> characteristicSetIterator =
				this.characteristicSets.listIterator();
		while (characteristicSetIterator.hasNext()) {
			final CharacteristicSetStatistics characteristicSet =
					characteristicSetIterator.next();
			final Set<String> characteristicsSetProperties = characteristicSet.getProperties();
			final ArrayList<String> whereElements = new ArrayList<>();
			final ArrayList<String> selectElements = new ArrayList<>();
			for (final Map.Entry<String, PropertyStatistics> propertyStatisticsEntry : this.properties.entrySet()) {
				final String columnName = propertyStatisticsEntry.getValue().getInternalName();
				if (characteristicsSetProperties.contains(propertyStatisticsEntry.getKey())) {
					whereElements.add(columnName + " IS NOT NULL");
					selectElements.add(columnName);
				} else {
					whereElements.add(columnName + " IS NULL");
				}
			}
			String query = "SELECT " + String.join(", ", selectElements);
			query += " FROM wide_property_table";
			query += " WHERE " + String.join(" AND ", whereElements);

			Dataset<Row> tuples = spark.sql(query);

			for (final String property : characteristicsSetProperties) {
				final String internalName = this.properties.get(property).getInternalName();
				if (this.properties.get(property).isComplex()) {
					tuples = tuples.withColumn(internalName, functions.size(new Column(internalName)));
				} else {
					tuples = tuples.withColumn(internalName, functions.lit(1));
				}
			}
			tuples = tuples.groupBy().sum();
			final Row sums = tuples.collectAsList().get(0);

			final CharacteristicSetStatistics newCharset = new CharacteristicSetStatistics();
			newCharset.setDistinctSubjects(characteristicSet.getDistinctSubjects());
			for (final String property : characteristicsSetProperties) {
				final String internalName = this.properties.get(property).getInternalName();
				newCharset.addProperty(property, sums.getAs("sum(" + internalName + ")"));
			}
			characteristicSetIterator.set(newCharset);
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

	public long getTuplesNumber() {
		return tuplesNumber;
	}

	public void setTuplesNumber(final Long tuplesNumber) {
		this.tuplesNumber = tuplesNumber;
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

	public boolean hasPropertiesStatistics() {
		return !this.properties.isEmpty();
	}

	//does not guarantee superset
	/*public void mergeCharacteristicSetsIntoSuperSets() {
		final ArrayList<CharacteristicSetStatistics> mergedCharsets = new ArrayList<>();

		for (final CharacteristicSetStatistics originalCharset : characteristicSets) {
			boolean merged = false;
			for (final CharacteristicSetStatistics newCharset : mergedCharsets) {
				if (newCharset.hasCommonProperties(originalCharset)) {
					newCharset.merge(originalCharset);
					merged = true;
					break;
				}
			}
			if (!merged) {
				mergedCharsets.add(originalCharset);
			}
		}
		this.characteristicSets = mergedCharsets;*/
	//}

	/*public ArrayList<HashSet<String>> getSuperSets() {
		ArrayList<HashSet<String>> superSetsList = new ArrayList<>();

		for (final CharacteristicSetStatistics originalCharset : characteristicSets) {
			boolean merged = false;
			for (final HashSet<String> superSet : superSetsList) {
				for (String property : originalCharset.getProperties()){
					if (superSet.contains(property)) {
						newCharset.merge(originalCharset);
						merged = true;
						break;
				}
			}
			if (!merged) {
				mergedCharsets.add(originalCharset);
			}
		}
		this.characteristicSets = mergedCharsets;

	}*/

	public void addEmergentSchema(final HashSet<String> properties, final String tableName) {
		final EmergentSchemaStatistics emergentSchemaStatistics = new EmergentSchemaStatistics(properties, tableName);
		this.emergentSchemas.add(emergentSchemaStatistics);
	}

	/*public void mergeCharacteristicSetsToSuperSets() {
		final ListIterator<CharacteristicSetStatistics> baseIterator = characteristicSets.listIterator();

		while (baseIterator.hasNext()) {
			boolean deleteBaseCharset = false;
			final CharacteristicSetStatistics baseCharset = baseIterator.next();

			final ListIterator<CharacteristicSetStatistics> compareIterator = characteristicSets.listIterator();
			while (compareIterator.hasNext()) {
				final CharacteristicSetStatistics compareCharset = compareIterator.next();
				if (baseCharset != compareCharset) {
					if (compareCharset.containsSubset(new HashSet<>(baseCharset.getTuplesPerPredicate().keySet()))) {
						//TODO update values from compareCharset
						deleteBaseCharset = true;
					}
				}
			}
			if (deleteBaseCharset) {
				baseIterator.remove();
			}
		}
	}*/
}
