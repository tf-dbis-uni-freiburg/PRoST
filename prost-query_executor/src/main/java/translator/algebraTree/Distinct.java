package translator.algebraTree;

import com.hp.hpl.jena.shared.PrefixMapping;
import com.hp.hpl.jena.sparql.algebra.op.OpDistinct;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import statistics.DatabaseStatistics;
import utils.Settings;

/**
 * A "Distinct" algebra operation on a sub-operation.
 */
public class Distinct extends SimpleOperation {
	Distinct(final OpDistinct jenaAlgebraTree, final DatabaseStatistics statistics, final Settings settings,
			 final PrefixMapping prefixes) throws Exception {
		super(jenaAlgebraTree.getSubOp(), statistics, settings, prefixes);
	}

	public Dataset<Row> computeOperation(final SQLContext sqlContext) {
		return this.computeSubOperation(sqlContext).distinct();
	}

	@Override
	public String toString() {
		return "DISTINCT{" + System.lineSeparator() + this.getSubOperation().toString() + System.lineSeparator() + "}";
	}
}
