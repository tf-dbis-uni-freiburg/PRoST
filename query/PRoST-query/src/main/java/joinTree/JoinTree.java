package joinTree;

import java.util.List;

import org.apache.log4j.Logger;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

import executor.Utils;

/**
 * JoinTree definition.
 *
 * @author Matteo Cossu
 *
 */
public class JoinTree {
	
	private static final Logger logger = Logger.getLogger("PRoST");
	
	private final Node root;
	private final List<Node> optionalTreeRoots;
	private boolean selectDistinct = false;

	// identifier for the query, useful for debugging
	public String query_name;

	public JoinTree(final Node root, final List<Node> optionalTreeRoots, final String query_name) {
		this.query_name = query_name;
		this.root = root;
		this.optionalTreeRoots = optionalTreeRoots;
	}

	public Node getRoot() {
		return root;
	}

	public void computeSingularNodeData(final SQLContext sqlContext) {
		root.computeSubTreeData(sqlContext);
		for (int i = 0; i < optionalTreeRoots.size(); i++) {
			optionalTreeRoots.get(i).computeSubTreeData(sqlContext);
		}
	}

	public Dataset<Row> computeJoins(final SQLContext sqlContext) {
		// compute all the joins
		logger.info("computeJoins");
		Dataset<Row> results = root.computeJoinWithChildren(sqlContext);

		// select only the requested result
		final Column[] selectedColumns = new Column[root.projection.size()];
		for (int i = 0; i < selectedColumns.length; i++) {
			selectedColumns[i] = new Column(root.projection.get(i));
		}
		for (int i = 0; i < optionalTreeRoots.size(); i++) {
			// OPTIONAL
			final Node currentOptionalNode = optionalTreeRoots.get(i);
			// compute joins in the optional tree
			Dataset<Row> optionalResults = currentOptionalNode.computeJoinWithChildren(sqlContext);
			// add selection and filter in the optional tree
			// if there is a filter set, apply it
			if (currentOptionalNode.filter == null) {
				optionalResults = optionalResults.filter(currentOptionalNode.filter);
			}

			// add left join with the optional tree
			final List<String> joinVariables = Utils.commonVariables(results.columns(), optionalResults.columns());
			results = results.join(optionalResults, scala.collection.JavaConversions.asScalaBuffer(joinVariables).seq(),
					"left_outer");
		}

		// if there is a filter set, apply it
		results = root.filter == null ? results.select(selectedColumns)
				: results.filter(root.filter).select(selectedColumns);

		// if results are distinct
		if (selectDistinct) {
			results = results.distinct();
		}
		return results;

	}

	@Override
	public String toString() {
		return root.toString();
	}

	public void setDistinct(final boolean distinct) {
		selectDistinct = distinct;
	}
}
