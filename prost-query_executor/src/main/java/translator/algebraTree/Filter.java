package translator.algebraTree;

import java.util.ArrayList;
import java.util.List;

import com.hp.hpl.jena.shared.PrefixMapping;
import com.hp.hpl.jena.sparql.algebra.op.OpFilter;
import com.hp.hpl.jena.sparql.expr.Expr;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import statistics.DatabaseStatistics;
import utils.Settings;

/**
 * A filter algebra operation on a sub-operation.
 */
public class Filter extends SimpleOperation {
	private final ArrayList<String> filters = new ArrayList<>();

	Filter(final OpFilter jenaAlgebraTree, final DatabaseStatistics statistics, final Settings settings,
		   final PrefixMapping prefixes) throws Exception {
		super(jenaAlgebraTree.getSubOp(), statistics, settings, prefixes);
		for (final Expr jenaExpression : jenaAlgebraTree.getExprs().getList()) {
			filters.add(JenaExpressionConverter.jenaExpressionToSqlExpression(jenaExpression));
		}
	}

	Filter(final Operation subOperation, final List<Expr> jenaFilterOperations) throws Exception {
		super(subOperation);
		for (final Expr jenaExpression : jenaFilterOperations) {
			filters.add(JenaExpressionConverter.jenaExpressionToSqlExpression(jenaExpression));
		}
	}

	public Dataset<Row> computeOperation(final SQLContext sqlContext) {
		Dataset<Row> result = this.computeSubOperation(sqlContext);
		for (final String filter : filters) {
			result = result.filter(filter);
		}
		return result;
	}

	@Override
	public String toString() {
		final StringBuilder stringBuilder = new StringBuilder();
		stringBuilder.append("FILTER{");
		for (final String filter : this.filters) {
			stringBuilder.append(System.lineSeparator());
			stringBuilder.append(filter);
		}
		stringBuilder.append(System.lineSeparator()).append("}");
		return stringBuilder.toString();
	}
}
