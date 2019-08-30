package translator.algebraTree;

import java.util.List;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import utils.Utils;

/**
 * A leftouter join between two sub-operations.
 */
public class LeftJoin extends CompoundOperation {
	LeftJoin(final Operation leftOperation, final Operation rightOperation) {
		super(leftOperation, rightOperation);
	}

	public Dataset<Row> computeOperation(final SQLContext sqlContext) {
		final Dataset<Row> left = this.computeLeftSubOperation(sqlContext);
		final Dataset<Row> right = this.computeRightSubOperation(sqlContext);

		final List<String> joinVariables = Utils.commonVariables(right.columns(),
				left.columns());
		return left.join(right, scala.collection.JavaConversions.asScalaBuffer(joinVariables).seq(), "leftouter");
	}

	@Override
	public String toString() {
		return "{" + this.getLeftSubOperation().toString() + "} LEFT_OUTER_JOIN {" + this.getRightSubOperation() + "}";
	}
}
