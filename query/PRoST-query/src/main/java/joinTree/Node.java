package joinTree;

import java.util.Collections;
import java.util.List;
import java.util.PriorityQueue;

import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

import executor.Utils;
import translator.NodeComparator2;

/*
 * A single node of the JoinTree
 *
 */
public abstract class Node {

	private static final Logger logger = Logger.getLogger("PRoST");

	public TriplePattern triplePattern;
	public Node parent;
	public PriorityQueue<Node> children;
	public List<String> projection;
	public List<TriplePattern> tripleGroup;
	// the spark data set containing the data relative to this node
	public Dataset<Row> sparkNodeData;
	public String filter;

	public Node() {
		children = new PriorityQueue<>(new NodeComparator2());
		// set the projections (if present)
		projection = Collections.emptyList();
	}

	/**
	 * computeNodeData sets the Dataset<Row> to the data referring to this node.
	 */
	public abstract void computeNodeData(SQLContext sqlContext);

	public void setProjectionList(final List<String> projections) {
		projection = projections;
	}

	// call computeNodeData recursively on the whole subtree
	public void computeSubTreeData(final SQLContext sqlContext) {
		// logger.info("computeSubTreeData for a node " + triplePattern.toString() + ".
		// First compute node data for the current node.");
		computeNodeData(sqlContext);
		for (final Node child : children) {
			// logger.info("computeSubTreeData for a node's children");
			child.computeSubTreeData(sqlContext);
		}
	}

	// call computeNodeData recursively on the whole subtree
	public void compute(final SQLContext sqlContext) {
		// logger.info("computeSubTreeData for a node " + triplePattern.toString() + ".
		// First compute node data for the current node.");
		for (final Node child : children) {
			// logger.info("computeSubTreeData for a node's children");
			child.computeSubTreeData(sqlContext);
		}
	}

	// join tables between itself and all the children
	public Dataset<Row> computeJoinWithChildren(final SQLContext sqlContext) {
		// logger.info("Inside computeJoinWithChildren for node: " +
		// triplePattern.toString());
		if (sparkNodeData == null) {
			// logger.info("If the node data is not computed, compute it: ");
			computeNodeData(sqlContext);
		}
		Dataset<Row> currentResult = sparkNodeData;
		// logger.info("For node childen, do joins: ");
		for (final Node child : children) {
//			logger.info("Node:" + child.triplePattern.toString());
//			logger.info("Do compute join with childen! ");
			final Dataset<Row> childResult = child.computeJoinWithChildren(sqlContext);
			// logger.info("When childen data is computed, find common variables:");
			final List<String> joinVariables = Utils.commonVariables(currentResult.columns(), childResult.columns());
//			logger.info("JoinVars: " + joinVariables);
//			logger.info("Compute join result");
			currentResult = currentResult.join(childResult,
					scala.collection.JavaConversions.asScalaBuffer(joinVariables).seq());
//			logger.info("ChildResult: " + childResult.count());
			// final long startTime = System.currentTimeMillis();
//			logger.info("JoinVars: " + joinVariables);
//			logger.info("Compute join");
//			logger.info("Current result: " + currentResult.count());
//			long executionTime = System.currentTimeMillis() - startTime;
//			logger.info("Execution time: " + String.valueOf(executionTime));

		}
		// logger.info("Return after computing joins with all childrens");
		return currentResult;
	}

	@Override
	public String toString() {
		final StringBuilder str = new StringBuilder("{");
		if (this instanceof PtNode) {
			str.append("WPT node: ");
			for (final TriplePattern tpGroup : tripleGroup) {
				str.append(tpGroup.toString() + ", ");
			}
		} else if (this instanceof IptNode) {
			str.append("IWPT node: ");
			for (final TriplePattern tpGroup : tripleGroup) {
				str.append(tpGroup.toString() + ", ");
			}
		} else if (this instanceof JptNode) {
			str.append("JWPT node: ");
			for (final TriplePattern tpGroup : tripleGroup) {
				str.append(tpGroup.toString() + ", ");
			}
		} else {
			str.append("VP node: ");
			str.append(triplePattern.toString());
		}
		str.append(" }");
		str.append(" [");
		for (final Node child : children) {
			str.append("\n" + child.toString());
		}
		str.append("\n]");
		return str.toString();
	}

	public void addChildren(final Node newNode) {
		children.add(newNode);

	}

	public int getChildrenCount() {
		return children.size();
	}

}