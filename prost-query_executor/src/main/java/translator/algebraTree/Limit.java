package translator.algebraTree;

import com.hp.hpl.jena.shared.PrefixMapping;
import com.hp.hpl.jena.sparql.algebra.op.OpSlice;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import statistics.DatabaseStatistics;
import utils.Settings;

public class Limit extends SimpleOperation {
	private Integer limit;

	Limit(final OpSlice jenaAlgebraTree, final DatabaseStatistics statistics, final Settings settings,
		  final PrefixMapping prefixes) throws Exception {
		super(jenaAlgebraTree.getSubOp(), statistics, settings, prefixes);
		this.limit = (int)jenaAlgebraTree.getLength();
	}

	public Dataset<Row> computeOperation(final SQLContext sqlContext) {
		return this.computeSubOperation(sqlContext).limit(this.limit);
	}

	@Override
	public String toString() {
		return "LIMIT " + this.limit + "{" + System.lineSeparator()
				+ this.getSubOperation().toString() + System.lineSeparator() + "}";
	}
}
