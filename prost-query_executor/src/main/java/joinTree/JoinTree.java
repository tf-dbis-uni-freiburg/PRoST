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
	// TODO fix optional tree
	// private final List<Node> optionalTreeRoots;
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
		// TODO fix the optional trees
		/*for (int i = 0; i < optionalTreeRoots.size(); i++) {
			// OPTIONAL
			final Node currentOptionalNode = optionalTreeRoots.getFirstTriplePattern(i);
			// compute joins in the optional tree
			Dataset<Row> optionalResults = currentOptionalNode.computeJoinWithChildren(sqlContext);
			// addTriplePattern selection and filter in the optional tree
			// if there is a filter set, apply it
			if (currentOptionalNode.filter == null) {
				optionalResults = optionalResults.filter(currentOptionalNode.filter);
			}

			// addTriplePattern left join with the optional tree
			final List<String> joinVariables = Utils.commonVariables(results.columns(), optionalResults.columns());
			results = results.join(optionalResults, scala.collection.JavaConversions.asScalaBuffer(joinVariables).seq(),
					"left_outer");
		}*/

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

	/*
//	// TODO compute the tree in a bottom-up approach
//	public Dataset<Row> computeBottomUp(final SQLContext sqlContext) {
//		PriorityQueue<Node> visitableNodes = new PriorityQueue<Node>(new NodeComparator());
//		visitableNodes.addAll(findLeaves());
//		while (!visitableNodes.isEmpty()) {
//			Node current = visitableNodes.poll();
//			// if a leave, compute node date
//			if (current.sparkNodeData == null) {
//				current.computeNodeData(sqlContext);
//			}
//			
//		}
//
//		Dataset<Row> results = root.sparkNodeData;
//
//		// select only the requested result
//		final Column[] selectedColumns = new Column[this.projection.size()];
//		for (int i = 0; i < selectedColumns.length; i++) {
//			selectedColumns[i] = new Column(this.projection.getFirstTriplePattern(i));
//		}
//		// TODO fix the optional tree
//		for (int i = 0; i < optionalTreeRoots.size(); i++) {
//			// OPTIONAL
//			final Node currentOptionalNode = optionalTreeRoots.getFirstTriplePattern(i);
//			// compute joins in the optional tree
//			Dataset<Row> optionalResults = currentOptionalNode.compute(sqlContext);
//			// addTriplePattern selection and filter in the optional tree
//			// if there is a filter set, apply it
//			if (currentOptionalNode.filter == null) {
//				optionalResults = optionalResults.filter(currentOptionalNode.filter);
//			}
//
//			// addTriplePattern left join with the optional tree
//			final List<String> joinVariables = Utils.commonVariables(results.columns(), optionalResults.columns());
//			results = results.join(optionalResults,scala.collection.JavaConversions.asScalaBuffer(joinVariables).seq(),
//					"left_outer");
//		}
//
//		// if there is a filter set, apply it
//		results = this.filter == null ? results.select(selectedColumns)
//				: results.filter(this.filter).select(selectedColumns);
//
//		// if results are distinct
//		if (selectDistinct) {
//			results = results.distinct();
//		}
//		return results;
//	}
//
//	public boolean hasNotComputedChildren(Node node) {
//		boolean hasChildrenToCompute = false;
//		for (Iterator iterator = node.children.iterator(); iterator.hasNext();) {
//			Node child = (Node) iterator.next();
//			// either there is a child which data is not computed
//			// or there is a child which children are not computed
//			if (child.sparkNodeData == null || (child.sparkNodeData != null && hasNotComputedChildren(child))) {
//				return true;
//			}
//		}
//		return hasChildrenToCompute;
//	}
	 */
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
