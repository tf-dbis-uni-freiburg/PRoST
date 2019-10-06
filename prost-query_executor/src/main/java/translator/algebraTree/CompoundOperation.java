package translator.algebraTree;

import com.hp.hpl.jena.shared.PrefixMapping;
import com.hp.hpl.jena.sparql.algebra.Op;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import statistics.DatabaseStatistics;
import utils.Settings;

/**
 * An algebra tree operation between two sub-operations.
 */
public abstract class CompoundOperation extends Operation {
	private Operation leftSubOperation;
	private Operation rightSubOperation;

	CompoundOperation(final Op jenaLeftSubOperation, final Op jenaRightSubOperation,
					  final DatabaseStatistics statistics, final Settings settings,
					  final PrefixMapping prefixes) throws Exception {
		this.leftSubOperation = new Operation.Builder(jenaLeftSubOperation, statistics, settings, prefixes).build();
		this.rightSubOperation = new Operation.Builder(jenaRightSubOperation, statistics, settings, prefixes).build();
	}

	CompoundOperation(final Operation leftSubOperation, final Operation rightSubOperation) {
		this.leftSubOperation = leftSubOperation;
		this.rightSubOperation = rightSubOperation;
	}

	Dataset<Row> computeLeftSubOperation(final SQLContext sqlContext) {
		return this.leftSubOperation.computeOperation(sqlContext);
	}

	Dataset<Row> computeRightSubOperation(final SQLContext sqlContext) {
		return this.rightSubOperation.computeOperation(sqlContext);
	}

	public Operation getLeftSubOperation() {
		return leftSubOperation;
	}

	public Operation getRightSubOperation() {
		return rightSubOperation;
	}
}
