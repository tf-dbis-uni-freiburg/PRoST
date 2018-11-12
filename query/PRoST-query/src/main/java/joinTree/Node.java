package joinTree;

import java.util.List;

import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

/*
 * A single node of the JoinTree
 *
 */
public abstract class Node {

	private static final Logger logger = Logger.getLogger("PRoST");

	public Node parent;
	// the spark data set containing the data relative to this node
	public Dataset<Row> sparkNodeData;

	public Node() {
		this.parent = null;
	}

	public Node(Node parent) {
		this.parent = parent;
	}

	/**
	 * computeNodeData sets the Dataset<Row> to the data referring to this node.
	 */
	public abstract void computeNodeData(SQLContext sqlContext);

	public abstract List<TriplePattern> collectTriples();
	
//TODO fix this
//	@Override
//	public String toString() {
//		final StringBuilder str = new StringBuilder("{");
//		if (this instanceof PtNode) {
//			str.append("WPT node: ");
//			for (final TriplePattern tpGroup : tripleGroup) {
//				str.append(tpGroup.toString() + ", ");
//			}
//		} else if (this instanceof IptNode) {
//			str.append("IWPT node: ");
//			for (final TriplePattern tpGroup : tripleGroup) {
//				str.append(tpGroup.toString() + ", ");
//			}
//		} else if (this instanceof JptNode) {
//			str.append("JWPT node: ");
//			for (final TriplePattern tpGroup : tripleGroup) {
//				str.append(tpGroup.toString() + ", ");
//			}
//		} else {
//			str.append("VP node: ");
//			str.append(triplePattern.toString());
//		}
//		str.append(" }");
//		str.append(" [");
//		for (final Node child : children) {
//			str.append("\n" + child.toString());
//		}
//		str.append("\n]");
//		return str.toString();
//	}
}