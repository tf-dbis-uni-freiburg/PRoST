package stats;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

/**
 * handles statistical information about a single property of an rdf graph.
 */
public class PropertyStatistics {
	private final String vpTableName;
	private final int tuplesNumber;
	private final boolean isComplex;
	private final int distinctSubjects;
	private final int distinctObjects;
	private final boolean isInverseComplex;

	public PropertyStatistics(final Dataset<Row> table, final String tableName) {
		this.tuplesNumber = (int) table.count();
		this.distinctSubjects = (int) table.select("s").distinct().count();
		this.distinctObjects = (int) table.select("o").distinct().count();
		this.isComplex = tuplesNumber != distinctSubjects;
		this.isInverseComplex = tuplesNumber != distinctObjects;
		this.vpTableName = tableName;
	}

	public String getVpTableName() {
		return vpTableName;
	}

	public int getTuplesNumber() {
		return tuplesNumber;
	}

	public int getDistinctSubjects() {
		return distinctSubjects;
	}

	public boolean isComplex() {
		return isComplex;
	}

	public boolean isInverseComplex() {
		return isInverseComplex;
	}
}
