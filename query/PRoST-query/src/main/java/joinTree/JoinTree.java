package joinTree;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.PriorityQueue;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

import executor.Utils;
import translator.NodeComparator2;

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
	public String filter;
	public List<String> projection;
	
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
//		logger.info("computeSingularNodeData");
//		logger.info("Start from the root. Compute subtrees!");
//		List<Node> childen = root.children;
//		for (Iterator iterator = childen.iterator(); iterator.hasNext();) {
//			Node node = (Node) iterator.next();
//			logger.info(node.triplePattern.toString());
//		}
		root.computeSubTreeData(sqlContext);
		for (int i = 0; i < optionalTreeRoots.size(); i++) {
			optionalTreeRoots.get(i).computeSubTreeData(sqlContext);
		}
	}

	public Dataset<Row> computeJoins(final SQLContext sqlContext) {
		// compute all the joins
		// logger.info("Inside computeJoins. Starting from the root, compute joins with
		// children on the root!");
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

	public Dataset<Row> compute(final SQLContext sqlContext) {
		PriorityQueue<Node> visitableNodes = new PriorityQueue<Node>(new NodeComparator2());
		visitableNodes.addAll(findLeaves());
		while (!visitableNodes.isEmpty()) {
			Node current = visitableNodes.poll();
			// if a leave, compute node date
			if (current.sparkNodeData == null) {
				current.computeNodeData(sqlContext);
			}
			if (current.parent != null) {
				// computes parent node date
				if(current.parent.sparkNodeData == null) {
					current.parent.computeNodeData(sqlContext);
				}
				final List<String> joinVariables = Utils.commonVariables(current.sparkNodeData.columns(),
						current.parent.sparkNodeData.columns());
				Dataset<Row> intermediateResult = current.sparkNodeData.join(current.parent.sparkNodeData,
						scala.collection.JavaConversions.asScalaBuffer(joinVariables).seq());
				// store join data into the parent node
				current.parent.sparkNodeData = intermediateResult;

				// remove current from the queue
				visitableNodes.remove(current);
				if(!hasNotComputedChildren(current.parent)) {
					visitableNodes.add(current.parent);
				}
			} else {
				break;
			}
		}
		Dataset<Row> results = root.sparkNodeData;
		// select only the requested result
		final Column[] selectedColumns = new Column[root.projection.size()];
		for (int i = 0; i < selectedColumns.length; i++) {
			selectedColumns[i] = new Column(root.projection.get(i));
		}
//		for (int i = 0; i < optionalTreeRoots.size(); i++) {
//			// OPTIONAL
//			final Node currentOptionalNode = optionalTreeRoots.get(i);
//			// compute joins in the optional tree
//			Dataset<Row> optionalResults = currentOptionalNode.compute(sqlContext);
//			// add selection and filter in the optional tree
//			// if there is a filter set, apply it
//			if (currentOptionalNode.filter == null) {
//				optionalResults = optionalResults.filter(currentOptionalNode.filter);
//			}
//
//			// add left join with the optional tree
//			final List<String> joinVariables = Utils.commonVariables(results.columns(), optionalResults.columns());
//			results = results.join(optionalResults, scala.collection.JavaConversions.asScalaBuffer(joinVariables).seq(),
//					"left_outer");
//		}

		// if there is a filter set, apply it
		results = root.filter == null ? results.select(selectedColumns)
				: results.filter(root.filter).select(selectedColumns);

		// if results are distinct
		if (selectDistinct) {
			results = results.distinct();
		}
		return results;
	}

	public boolean hasNotComputedChildren(Node node) {
		boolean hasChildrenToCompute = false;
		for (Iterator iterator = node.children.iterator(); iterator.hasNext();) {
			Node child = (Node) iterator.next();
			// either there is a child which data is not computed
			// or there is a child which children are not computed
			if(child.sparkNodeData == null || (child.sparkNodeData != null && hasNotComputedChildren(child))) {
				return true;
			} 
		}
		return hasChildrenToCompute;
	}
	public List<Node> findLeaves() {
		List<Node> leaves = new ArrayList<>();
		LinkedList<Node> toVisit = new LinkedList<>();
		toVisit.add(root);
		while (!toVisit.isEmpty()) {
			Node current = toVisit.poll();
			if (current.children.size() > 0) {
				toVisit.addAll(current.children);
			} else {
				leaves.add(current);
			}
			toVisit.remove(current);
		}
		return leaves;
	}

	@Override
	public String toString() {
		return root.toString();
	}

	public void setDistinct(final boolean distinct) {
		selectDistinct = distinct;
	}
	
	public void setProjectionList(final List<String> projections) {
		projection = projections;
	}
}
