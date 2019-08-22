package translator.algebraTree;

import static utils.Utils.removeQuestionMark;

import java.util.List;

import com.hp.hpl.jena.shared.PrefixMapping;
import com.hp.hpl.jena.sparql.algebra.op.OpProject;
import com.hp.hpl.jena.sparql.core.Var;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import statistics.DatabaseStatistics;
import utils.Settings;

/**
 * A projection operation on a sub-operation.
 */
class Projection extends SimpleOperation {
	private Column[] projectionColumns;

	Projection(final OpProject jenaAlgebraTree, final DatabaseStatistics statistics, final Settings settings,
			   final PrefixMapping prefixes) throws Exception {
		super(jenaAlgebraTree.getSubOp(), statistics, settings, prefixes);
		final List<Var> jenaProjectionVariables = jenaAlgebraTree.getVars();
		this.projectionColumns = new Column[jenaProjectionVariables.size()];
		for (int i = 0; i < projectionColumns.length; i++) {
			projectionColumns[i] = new Column(removeQuestionMark(jenaProjectionVariables.get(i).toString()));
		}
	}

	public Dataset<Row> computeOperation(final SQLContext sqlContext) {
		return this.computeSubOperation(sqlContext).select(projectionColumns);
	}

	@Override
	public String toString() {
		final StringBuilder stringBuilder = new StringBuilder();
		stringBuilder.append("PROJECT");
		for (final Column column : projectionColumns) {
			stringBuilder.append(" ").append(column.toString());
		}
		stringBuilder.append("{").append(System.lineSeparator());
		stringBuilder.append(this.getSubOperation().toString()).append(System.lineSeparator()).append("}");
		return stringBuilder.toString();
	}
}
