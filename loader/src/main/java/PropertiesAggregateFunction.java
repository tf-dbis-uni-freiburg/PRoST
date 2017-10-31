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
 * Spark user defined function that returns for each subject a list of its
 * properties. Each property is represented by a list which contains the objects
 * connected to a subject by this property. For example, for a subject "s1", we
 * have the corresponding triples: 
 * 	"s1 likes o1" , 
 * 	"s1 likes o2" , 
 *	"s1 has o3", 
 * 	"s1 contains o4"
 *  In addition, a list of all predicates is as follows
 * <likes, has, contains, is>. So it can be seen that the subject s1 does not
 * participate into a triple with the predicate "is". Let's also assume that the
 * order of predicates is as the given above. Therefore, for the subject s1, the
 * result is (List<List<String>>) <<o1, o2>, <o3>, <o4>, NULL>. The order of
 * results for each predicate will be the same as the order of the predicates
 * specified in the creation of the function.
 * 
 * @author Matteo Cossu
 *
 */
public class PropertiesAggregateFunction extends UserDefinedAggregateFunction {
	private static final long serialVersionUID = 1L;

	// contains all predicates for a table
	// the returned properties for each subject from this function
	// are ordered in the same way as their order in this array
	private String[] allProperties;
	
	// string used to distinguish between two values inside a single column
	private String columns_separator;

	public PropertiesAggregateFunction(String[] allProperties, String separator) {
		this.allProperties = allProperties;
		this.columns_separator = separator;
	}

	public StructType inputSchema() {
		return new StructType().add("p_o", DataTypes.StringType);
	}

	public StructType bufferSchema() {
		return new StructType().add("map",
				DataTypes.createMapType(DataTypes.StringType, DataTypes.createArrayType(DataTypes.StringType), true));
	}

	// the aggregate function returns an Array Type
	public DataType dataType() {
		return DataTypes.createArrayType(DataTypes.createArrayType(DataTypes.StringType));
	}

	public boolean deterministic() {
		return true;
	}

	// initialize the temporary structure
	public void initialize(MutableAggregationBuffer buffer) {
		buffer.update(0, new HashMap<String, List<String>>());
	}

	@SuppressWarnings("unchecked")
	// it performs the conversion/casting from the scala array to Java list
	private List<String> getStringList(Object rawList) {
		return new ArrayList<String>(scala.collection.JavaConverters
				.seqAsJavaListConverter((scala.collection.mutable.WrappedArray.ofRef<String>) rawList).asJava());
	}

	// for each element inside a group, add the new value to the right property
	// in the buffer
	public void update(MutableAggregationBuffer buffer, Row input) {

		// split the property from the object
		String[] po = input.getString(0).split(columns_separator);
		String property = po[0].startsWith("<") && po[0].endsWith(">") ? 
				po[0].substring(1, po[0].length() - 1 ).replaceAll("[[^\\w]+]", "_")
				: po[0].replaceAll("[[^\\w]+]", "_");
		String value = po[1];

		HashMap<Object, Object> properties = new HashMap<Object, Object>(
				scala.collection.JavaConversions.mapAsJavaMap(buffer.getMap(0)));

		// if the property already exists, append the value at the end of the
		// list
		if (properties.containsKey(property)) {
			List<String> values = getStringList(properties.get(property));
			values.add(value);
			properties.replace(property, values);
		} else { // otherwise just create a new list with that value
			List<String> values = new ArrayList<String>();
			values.add(value);
			properties.put(property, values);
		}
		// update the buffer
		buffer.update(0, properties);
	}

	// Merge two different part of the group (each group could be split by Spark)
	public void merge(MutableAggregationBuffer buffer1, Row buffer2) {

		// part1 and part2 contain the two buffers to be merged
		Map<Object, Object> part1 = scala.collection.JavaConversions.mapAsJavaMap(buffer1.getMap(0));
		Map<Object, Object> part2 = scala.collection.JavaConversions.mapAsJavaMap(buffer2.getMap(0));
		Object[] objectKeys1 = part1.keySet().toArray();
		String[] sortedKeys1 = Arrays.copyOf(objectKeys1, objectKeys1.length, String[].class);
		Arrays.sort(sortedKeys1);
		Object[] objectKeys2 = part2.keySet().toArray();
		String[] sortedKeys2 = Arrays.copyOf(objectKeys2, objectKeys2.length, String[].class);
		Arrays.sort(sortedKeys2);

		HashMap<String, List<String>> merged = new HashMap<String, List<String>>();

		// perform the merge
		int i = 0;
		int j = 0;
		while (i < sortedKeys1.length || j < sortedKeys2.length) {

			// one of the lists is finished before, add element and skip to next
			// while cycle
			if (i >= sortedKeys1.length) {
				List<String> values = getStringList(part2.get(sortedKeys2[j]));
				merged.put(sortedKeys2[j], values);
				j++;
				continue;
			}
			if (j >= sortedKeys2.length) {
				List<String> values = getStringList(part1.get(sortedKeys1[i]));
				merged.put(sortedKeys1[i], values);
				i++;
				continue;
			}

			String key1 = sortedKeys1[i];
			String key2 = sortedKeys2[j];
			int comparisonKeys = key1.compareTo(key2);

			// the two list for the same key have to be merged (duplicates
			// inside the lists ignored)
			if (comparisonKeys == 0) {
				List<String> mergedValues = getStringList(part1.get(key1));
				List<String> part2Values = getStringList(part2.get(key2));
				mergedValues.addAll(part2Values);
				merged.put(key1, mergedValues);
				i++;
				j++;
			} else if (comparisonKeys < 0) {
				List<String> mergedValues = getStringList(part1.get(key1));
				merged.put(key1, mergedValues);
				i++;
			} else {
				List<String> mergedValues = getStringList(part2.get(key2));
				merged.put(key2, mergedValues);
				j++;
			}
		}

		// write the result back in the buffer
		buffer1.update(0, merged);
	}

	// produce the final value for each group, a row containing all values
	public Object evaluate(Row buffer) {
		Map<Object, Object> completeRowMap = scala.collection.JavaConversions.mapAsJavaMap(buffer.getMap(0));
		ArrayList<List<String>> resultRow = new ArrayList<List<String>>();

		// keep the order of the properties
		for (String property : this.allProperties) {
			if (completeRowMap.containsKey(property)) {
				List<String> values = getStringList(completeRowMap.get(property));
				resultRow.add(values);
			} else
				resultRow.add(null);
		}

		return resultRow;
	}
}