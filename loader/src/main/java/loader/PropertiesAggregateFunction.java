package loader;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.expressions.MutableAggregationBuffer;
import org.apache.spark.sql.expressions.UserDefinedAggregateFunction;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

/**
 * Spark user defined function that returns for each subject a list of its properties. Each property is represented by a
 * list which contains the objects connected to a subject by this property. For example, for a subject "s1", we have the
 * corresponding triples: "s1 likes o1" , "s1 likes o2" , "s1 has o3", "s1 contains o4" In addition, a list of all
 * predicates is as follows &lt likes, has, contains, is>. So it can be seen that the subject s1 does not participate
 * into
 * a triple with the predicate "is". Let's also assume that the order of predicates is as the given above. Therefore,
 * for the subject s1, the result is (List&lt List&lt String>>) &lt &lt o1, o2>, &lt o3>, &lt o4>, NULL>. The order
 * of results for each predicate will be the same as the order of the predicates specified in the creation of the
 * function.
 *
 * @author Matteo Cossu
 */
public class PropertiesAggregateFunction extends UserDefinedAggregateFunction {
	private static final long serialVersionUID = 1L;

	// contains all predicates for a table
	// the returned properties for each subject from this function
	// are ordered in the same way as their order in this array
	private final String[] allProperties;
	// string used to distinguish between two values inside a single column
	private final String columns_separator;
	private int mAllProps;

	public PropertiesAggregateFunction(final String[] allProperties, final String separator) {
		this.allProperties = allProperties;
		columns_separator = separator;
	}

	@Override
	public StructType inputSchema() {
		return new StructType().add("p_o", DataTypes.StringType);
	}

	@Override
	public StructType bufferSchema() {
		return new StructType().add("map",
				DataTypes.createMapType(DataTypes.StringType, DataTypes.createArrayType(DataTypes.StringType), true));
	}

	// the aggregate function returns an Array Type
	@Override
	public DataType dataType() {
		return DataTypes.createArrayType(DataTypes.createArrayType(DataTypes.StringType));
	}

	@Override
	public boolean deterministic() {
		return true;
	}

	// initialize the temporary structure
	@Override
	public void initialize(final MutableAggregationBuffer buffer) {
		buffer.update(0, new HashMap<String, List<String>>());
	}

	@SuppressWarnings("unchecked")
	// it performs the conversion/casting from the scala array to Java list
	private List<String> getStringList(final Object rawList) {
		return new ArrayList<>(scala.collection.JavaConverters
				.seqAsJavaListConverter((scala.collection.mutable.WrappedArray.ofRef<String>) rawList).asJava());
	}

	// for each element inside a group, add the new value to the right property
	// in the buffer
	@Override
	public void update(final MutableAggregationBuffer buffer, final Row input) {

		// split the property from the object
		final String[] po = input.getString(0).split(columns_separator);
		final String property = po[0].startsWith("<") && po[0].endsWith(">")
				? po[0].substring(1, po[0].length() - 1).replaceAll("[[^\\w]+]", "_")
				: po[0].replaceAll("[[^\\w]+]", "_");
		final String value = po[1];

		final HashMap<Object, Object> properties =
				new HashMap<>(scala.collection.JavaConversions.mapAsJavaMap(buffer.getMap(0)));

		// if the property already exists, append the value at the end of the
		// list
		if (properties.containsKey(property)) {
			final List<String> values = getStringList(properties.get(property));
			values.add(value);
			properties.replace(property, values);
		} else { // otherwise just create a new list with that value
			final List<String> values = new ArrayList<>();
			values.add(value);
			properties.put(property, values);
		}
		// update the buffer
		buffer.update(0, properties);
	}

	// Merge two different part of the group (each group could be split by Spark)
	@Override
	public void merge(final MutableAggregationBuffer buffer1, final Row buffer2) {

		// part1 and part2 contain the two buffers to be merged
		final Map<Object, Object> part1 = scala.collection.JavaConversions.mapAsJavaMap(buffer1.getMap(0));
		final Map<Object, Object> part2 = scala.collection.JavaConversions.mapAsJavaMap(buffer2.getMap(0));
		final Object[] objectKeys1 = part1.keySet().toArray();
		final String[] sortedKeys1 = Arrays.copyOf(objectKeys1, objectKeys1.length, String[].class);
		Arrays.sort(sortedKeys1);
		final Object[] objectKeys2 = part2.keySet().toArray();
		final String[] sortedKeys2 = Arrays.copyOf(objectKeys2, objectKeys2.length, String[].class);
		Arrays.sort(sortedKeys2);

		final HashMap<String, List<String>> merged = new HashMap<>();

		// perform the merge
		int i = 0;
		int j = 0;
		while (i < sortedKeys1.length || j < sortedKeys2.length) {

			// one of the lists is finished before, add element and skip to next
			// while cycle
			if (i >= sortedKeys1.length) {
				final List<String> values = getStringList(part2.get(sortedKeys2[j]));
				merged.put(sortedKeys2[j], values);
				j++;
				continue;
			}
			if (j >= sortedKeys2.length) {
				final List<String> values = getStringList(part1.get(sortedKeys1[i]));
				merged.put(sortedKeys1[i], values);
				i++;
				continue;
			}

			final String key1 = sortedKeys1[i];
			final String key2 = sortedKeys2[j];
			final int comparisonKeys = key1.compareTo(key2);

			// the two list for the same key have to be merged (duplicates
			// inside the lists ignored)
			if (comparisonKeys == 0) {
				final List<String> mergedValues = getStringList(part1.get(key1));
				final List<String> part2Values = getStringList(part2.get(key2));
				mergedValues.addAll(part2Values);
				merged.put(key1, mergedValues);
				i++;
				j++;
			} else if (comparisonKeys < 0) {
				final List<String> mergedValues = getStringList(part1.get(key1));
				merged.put(key1, mergedValues);
				i++;
			} else {
				final List<String> mergedValues = getStringList(part2.get(key2));
				merged.put(key2, mergedValues);
				j++;
			}
		}

		// write the result back in the buffer
		buffer1.update(0, merged);
	}

	// produce the final value for each group, a row containing all values
	@Override
	public Object evaluate(final Row buffer) {
		final Map<Object, Object> completeRowMap = scala.collection.JavaConversions.mapAsJavaMap(buffer.getMap(0));
		final ArrayList<List<String>> resultRow = new ArrayList<>();

		// keep the order of the properties
		for (final String property : allProperties) {
			if (completeRowMap.containsKey(property)) {
				final List<String> values = getStringList(completeRowMap.get(property));
				resultRow.add(values);
			} else {
				resultRow.add(null);
			}
		}

		return resultRow;
	}
}