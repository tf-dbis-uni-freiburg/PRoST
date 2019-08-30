package translator.algebraTree;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import com.hp.hpl.jena.shared.PrefixMapping;
import com.hp.hpl.jena.sparql.algebra.Op;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.functions;
import statistics.DatabaseStatistics;
import utils.Settings;

/**
 * A union operation between two sub-operations.
 */
public class Union extends CompoundOperation {
	Union(final Op jenaLeftSubOperation, final Op jenaRightSubOperation,
		  final DatabaseStatistics statistics, final Settings settings,
		  final PrefixMapping prefixes) throws Exception {
		super(jenaLeftSubOperation, jenaRightSubOperation, statistics, settings, prefixes);
	}

	public Dataset<Row> computeOperation(final SQLContext sqlContext) {
		Dataset<Row> left = this.computeLeftSubOperation(sqlContext);
		Dataset<Row> right = this.computeRightSubOperation(sqlContext);

		// Spark's union operation required the same number of columns. Union is done considering the column index
		// only. Therefore, we add missing columns with null values.
		final Set<String> leftColumnsSet = new HashSet<>(Arrays.asList(left.columns()));
		final Set<String> rightColumnsSet = new HashSet<>(Arrays.asList(right.columns()));
		final Set<String> columns = new HashSet<>();
		columns.addAll(leftColumnsSet);
		columns.addAll(rightColumnsSet);

		final Set<String> leftDatasetMissingColumns = new HashSet<>(columns);
		leftDatasetMissingColumns.removeAll(leftColumnsSet);
		for (final String column : leftDatasetMissingColumns) {
			left = left.withColumn(column, functions.lit(null));
		}

		final Set<String> rightDatasetMissingColumns = new HashSet<>(columns);
		rightDatasetMissingColumns.removeAll(rightColumnsSet);
		for (final String column : rightDatasetMissingColumns) {
			right = right.withColumn(column, functions.lit(null));
		}

		// Union method considers order of columns only, therefore we select the columns beforehand. In Spark 2.3,
		// unionByName can be used instead.
		final String[] columnsAsList = columns.toArray(new String[0]);
		final String first = columnsAsList[0];
		final String[] rest = Arrays.copyOfRange(columnsAsList, 1, columnsAsList.length);
		return left.select(first, rest).union(right.select(first, rest));
	}

	@Override
	public String toString() {
		return "{" + this.getLeftSubOperation().toString() + "} UNION {" + this.getRightSubOperation() + "}";
	}
}
