package joinTree;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

import executor.Utils;

/*
 * A single node of the JoinTree
 *
 */
public abstract class Node {
	public TriplePattern triplePattern;
	public List<Node> children;
	public List<String> projection;
	public List<TriplePattern> tripleGroup;
	// the spark data set containing the data relative to this node
	public Dataset<Row> sparkNodeData;
	public String filter;

	public Node() {
		children = new ArrayList<>();

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
		computeNodeData(sqlContext);
		for (final Node child : children) {
			child.computeSubTreeData(sqlContext);
		}
	}

	// join tables between itself and all the children
	public Dataset<Row> computeJoinWithChildren(final SQLContext sqlContext) {
		if (sparkNodeData == null) {
			computeNodeData(sqlContext);
		}
		Dataset<Row> currentResult = sparkNodeData;
		for (final Node child : children) {
			final Dataset<Row> childResult = child.computeJoinWithChildren(sqlContext);
			final List<String> joinVariables = Utils.commonVariables(currentResult.columns(), childResult.columns());
			currentResult = currentResult.join(childResult,
					scala.collection.JavaConversions.asScalaBuffer(joinVariables).seq());
		}
		return currentResult;
	}

	@Override
	public String toString() {
		final StringBuilder str = new StringBuilder("{");
		if (this instanceof PtNode) {
			str.append("pt node: ");
			for (final TriplePattern tpGroup : tripleGroup) {
				str.append(tpGroup.toString() + ", ");
			}
		} else if (this instanceof IptNode) {
			str.append("ipt node: ");
			for (final TriplePattern tpGroup : tripleGroup) {
				str.append(tpGroup.toString() + ", ");
			}
		} else if (this instanceof JptNode) {
			str.append("jpt node: ");
			for (final TriplePattern tpGroup : tripleGroup) {
				str.append(tpGroup.toString() + ", ");
			}
		} else {
			str.append("vp node: ");
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