package joinTree;

import java.util.Collections;
import java.util.List;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

/**
 * JoinTree definition. It represents a binary tree. The leaves are of type
 * either {@link VPNode} or {@link MVNode}. Inner nodes are of type
 * {@link JoinNode}.
 *
 * @author Matteo Cossu
 * @author Polina Koleva
 */
public class JoinTree {
	private final Node root;
	private String filter;
	private String queryName;
	private List<String> projection;
	private boolean selectDistinct = false;

	public JoinTree(final Node root, final String queryName) {
		this.queryName = queryName;
		this.root = root;

		// set the projections (if present)
		projection = Collections.emptyList();
	}

	/**
	 * Computes a join tree.
	 *
	 * @return the computed dataset.
	 */
	public Dataset<Row> compute(final SQLContext sqlContext) {
		// compute all the joins
		root.computeNodeData(sqlContext);
		Dataset<Row> results = root.getSparkNodeData();

		// select only the requested result
		final Column[] selectedColumns = new Column[this.projection.size()];
		for (int i = 0; i < selectedColumns.length; i++) {
			selectedColumns[i] = new Column(this.projection.get(i));
		}

		//TODO implement filter Node (filters are part of the execution plan and should be present in the join tree)
		// if there is a filter set, apply it
		results = this.filter == null ? results
				: results.filter(this.filter);

		// apply projection as defined in the SPARQL query
		if (selectedColumns.length > 0) {
			results = results.select(selectedColumns);
		}

		if (selectDistinct) {
			results = results.distinct();
		}
		return results;
	}

	public Node getRoot() {
		return root;
	}

	public String getQueryName() {
		return queryName;
	}

	public void setDistinct(final boolean distinct) {
		selectDistinct = distinct;
	}

	public void setProjectionList(final List<String> projections) {
		projection = projections;
	}

	public void setFilter(final String filter) {
		this.filter = filter;
	}

	@Override
	public String toString() {
		return root.toString();
	}
}
