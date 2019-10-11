package statistics;

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
	private final Long distinctObjects;
	private final Long distinctSubjects;
	// private final Long selectivity;

	public PropertyStatistics(final Dataset<Row> table, final String tableName) {
		this.tuplesNumber = (int) table.count();
		this.distinctSubjects = table.select("s").distinct().count();
		this.isComplex = tuplesNumber != table.select("s").distinct().count();
		this.distinctObjects = table.select("o").distinct().count();
		this.isInverseComplex = tuplesNumber != this.distinctObjects;
		this.internalName = tableName;
		// this.selectivity = this.tuplesNumber / databaseSize;
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

	public double getBoundObjectEstimatedSelectivity() {
		return this.distinctObjects.doubleValue() / this.tuplesNumber;
	}

	public double getBoundSubjectEstimatedSelectivity() {
		return this.distinctSubjects.doubleValue() / this.tuplesNumber;
	}
}
