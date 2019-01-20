package joinTree;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import com.hp.hpl.jena.shared.PrefixMapping;
import org.apache.log4j.Logger;
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

	public static final Logger logger = Logger.getLogger("PRoST");
	// private static final Logger logger = Logger.getLogger("PRoST");

	public Node() {
		children = new ArrayList<>();
		tripleGroup = new ArrayList<>();

		// set the projections (if present)
		projection = Collections.emptyList();
	}

	/**
	 * computeNodeData sets the Dataset&ltRow> to the data referring to this node.
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

			for(String joinvar:joinVariables){
				logger.info("join vars: " + joinvar);
			}
			logger.info("currentResult " + currentResult.count());
			logger.info("childResult " + childResult.count());
			currentResult = currentResult.join(childResult, scala.collection.JavaConversions.asScalaBuffer(joinVariables).seq());
			logger.info("join " + currentResult.count());
		}
		return currentResult;
	}

	@Override
	public String toString() {
		final StringBuilder str = new StringBuilder("{");
		if (this instanceof PtNode) {
			str.append("WPT node: ");
			for (final TriplePattern tp_group : tripleGroup) {
				str.append(tp_group.toString() + ", ");
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
		} else if (this instanceof  VpJoinNode){
			str.append("JoinVP node: ");
			str.append(tripleGroup.get(0).toString());
			str.append(" join ");
			str.append(tripleGroup.get(1).toString());
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