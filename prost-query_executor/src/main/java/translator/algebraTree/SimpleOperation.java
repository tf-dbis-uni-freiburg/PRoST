package translator.algebraTree;

import com.hp.hpl.jena.shared.PrefixMapping;
import com.hp.hpl.jena.sparql.algebra.Op;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import statistics.DatabaseStatistics;
import utils.Settings;

/**
 * An algebra tree operation on a single sub-operation.
 */
public abstract class SimpleOperation extends Operation {
	private Operation subOperation;

	SimpleOperation(final Op jenaSubOperation, final DatabaseStatistics statistics, final Settings settings,
					final PrefixMapping prefixes) throws Exception {
		this.subOperation = new Operation.Builder(jenaSubOperation, statistics, settings, prefixes).build();
	}

	SimpleOperation(final Operation subOperation) {
		this.subOperation = subOperation;
	}

	Dataset<Row> computeSubOperation(final SQLContext sqlContext) {
		return this.subOperation.computeOperation(sqlContext);
	}

	public Operation getSubOperation() {
		return subOperation;
	}
}
