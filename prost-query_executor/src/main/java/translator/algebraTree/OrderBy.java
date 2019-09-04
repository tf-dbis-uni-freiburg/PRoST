package translator.algebraTree;

import java.util.ArrayList;

import com.hp.hpl.jena.query.SortCondition;
import com.hp.hpl.jena.shared.PrefixMapping;
import com.hp.hpl.jena.sparql.algebra.op.OpOrder;
import javafx.util.Pair;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import statistics.DatabaseStatistics;
import utils.Settings;

/**
 * Implementation of a sorting operation.
 */
public class OrderBy extends SimpleOperation {
	private final ArrayList<Pair<String, Direction>> sortVariables = new ArrayList<>();

	OrderBy(final OpOrder jenaAlgebraTree, final DatabaseStatistics statistics, final Settings settings,
			final PrefixMapping prefixes) throws Exception {
		super(jenaAlgebraTree.getSubOp(), statistics, settings, prefixes);

		for (final SortCondition sortCondition : jenaAlgebraTree.getConditions()) {
			final String sortExpression = JenaExpressionConverter.jenaExpressionToSqlExpression(sortCondition.getExpression());
			final int jenaDirection = sortCondition.getDirection();
			final Direction direction;
			if (jenaDirection == -2) {
				direction = Direction.ASC;
			} else {
				direction = Direction.DESC;
			}
			final Pair<String, Direction> sortVariable = new Pair<>(sortExpression, direction);
			sortVariables.add(sortVariable);
		}
	}

	public Dataset<Row> computeOperation(final SQLContext sqlContext) {
		final ArrayList<Column> sortColumns = new ArrayList<>();
		for (final Pair<String, Direction> sortVariable : sortVariables) {
			final String variable = sortVariable.getKey();
			final Direction direction = sortVariable.getValue();
			if (direction == Direction.ASC) {
				sortColumns.add(org.apache.spark.sql.functions.asc(variable));
			} else {
				sortColumns.add(org.apache.spark.sql.functions.desc(variable));
			}
		}
		return this.computeSubOperation(sqlContext).orderBy(scala.collection.JavaConverters.asScalaIteratorConverter(sortColumns.iterator()).asScala().toSeq());
	}

	private enum Direction {
		ASC,
		DESC
	}
}