package joinTree;

import java.util.Collections;
import java.util.List;

import org.apache.log4j.Logger;
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
	private static final Logger logger = Logger.getLogger("PRoST");
	public String filter;
	// identifier for the query, useful for debugging
	public String query_name;
	public List<String> projection;
	private final Node root;
	// TODO fix optional tree
	// private final List<Node> optionalTreeRoots;
	private boolean selectDistinct = false;
	// number of VpNodes and MVNodes
	private int vpLeavesCount = 0;
	private int wptLeavesCount = 0;


	public JoinTree(final Node root, final List<Node> optionalTreeRoots, final String queryName) {
		this.query_name = queryName;
		this.root = root;

		// calculate number of vp and wpt nodes for the tree
		// findLeaves();

		// TODO fix optional tree
		// this.optionalTreeRoots = optionalTreeRoots;

		// set the projections (if present)
		projection = Collections.emptyList();
	}

	/**
	 * Compute the result of a join tree.
	 *
	 * @return
	 */
	public Dataset<Row> compute(final SQLContext sqlContext) {
		// compute all the joins
		root.computeNodeData(sqlContext);
		Dataset<Row> results = root.sparkNodeData;

		// select only the requested result
		final Column[] selectedColumns = new Column[this.projection.size()];
		for (int i = 0; i < selectedColumns.length; i++) {
			selectedColumns[i] = new Column(this.projection.get(i));
		}
		// TODO fix the optional trees
		/*for (int i = 0; i < optionalTreeRoots.size(); i++) {
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
		}*/

		// if there is a filter set, apply it
		results = this.filter == null ? results
				: results.filter(this.filter);

		// apply projection as defined in the SPARQL query
		if (selectedColumns.length>0){
			results = results.select(selectedColumns);
		}

		// if results are distinct
		if (selectDistinct) {
			results = results.distinct();
		}
		return results;
	}

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
//			selectedColumns[i] = new Column(this.projection.get(i));
//		}
//		// TODO fix the optional tree
////		for (int i = 0; i < optionalTreeRoots.size(); i++) {
////			// OPTIONAL
////			final Node currentOptionalNode = optionalTreeRoots.get(i);
////			// compute joins in the optional tree
////			Dataset<Row> optionalResults = currentOptionalNode.compute(sqlContext);
////			// add selection and filter in the optional tree
////			// if there is a filter set, apply it
////			if (currentOptionalNode.filter == null) {
////				optionalResults = optionalResults.filter(currentOptionalNode.filter);
////			}
////
////			// add left join with the optional tree
////			final List<String> joinVariables = Utils.commonVariables(results.columns(), optionalResults.columns());
////			results = results.join(optionalResults,scala.collection.JavaConversions.asScalaBuffer(joinVariables).seq(),
////					"left_outer");
////		}
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

//	/**
//	 * Return a list containing the leaves of the tree.
//	 */
//	private void findLeaves() {
//		LinkedList<Node> toVisit = new LinkedList<>();
//		toVisit.add(root);
//		while (!toVisit.isEmpty()) {
//			Node current = toVisit.poll();
//			if (current instanceof MVNode) {
//				this.wptLeavesCount++;
//			} else if (current instanceof VPNode) {
//				this.vpLeavesCount++;
//			} else {
//				// add its children for visitation
//				toVisit.add(((JoinNode) current).getLeftChild());
//				toVisit.add(((JoinNode) current).getRightChild());
//			}
//			toVisit.remove(current);
//		}
//	}

	public Node getRoot() {
		return root;
	}

	public int getVpLeavesCount() {
		return vpLeavesCount;
	}

	public int getWptLeavesCount() {
		return wptLeavesCount;
	}

	public void setDistinct(final boolean distinct) {
		selectDistinct = distinct;
	}

	public void setProjectionList(final List<String> projections) {
		projection = projections;
	}

	@Override
	public String toString() {
		return root.toString();
	}
}
