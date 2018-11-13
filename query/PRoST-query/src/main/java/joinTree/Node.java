package joinTree;

import java.util.List;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

/**
 * An abstract class that each node of the JoinTree has to extend. Each node has
 * a parent and data frame that contains the data of the node.
 * 
 * @author Polina Koleva
 */
public abstract class Node {

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
	 * Compute the Dataset<Row> to the data referring to this node.
	 */
	public abstract void computeNodeData(SQLContext sqlContext);

	/**
	 * Get a list of triples that each node contains. For example, {@link VpNode}
	 * represents only one triple. On the other side, {@link PtNode} contains a list
	 * of triples with the same subject.
	 */
	public abstract List<TriplePattern> collectTriples();
}