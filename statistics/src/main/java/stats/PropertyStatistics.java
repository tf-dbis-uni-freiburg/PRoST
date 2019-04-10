package stats;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

/**
 * handles statistical information about a single property of an rdf graph.
 */
public class PropertyStatistics {
	private final String internalName;
	private final int tuplesNumber;
	private final boolean isComplex;
	private final boolean isInverseComplex;

	public PropertyStatistics(final Dataset<Row> table, final String tableName) {
		this.tuplesNumber = (int) table.count();
		this.isComplex = tuplesNumber != (int) table.select("s").distinct().count();
		this.isInverseComplex = tuplesNumber != (int) table.select("o").distinct().count();
		this.internalName = tableName;
	}

	public String getInternalName() {
		return internalName;
	}

	public int getTuplesNumber() {
		return tuplesNumber;
	}

	public boolean isComplex() {
		return isComplex;
	}

	public boolean isInverseComplex() {
		return isInverseComplex;
	}
}
