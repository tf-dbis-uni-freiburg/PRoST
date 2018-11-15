package joinTree;

import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;
import org.apache.spark.sql.SQLContext;

import executor.Utils;

/**
 * A node of a join tree that contains the result of a join between two other
 * nodes. It has two children. Its child can be another join node, if it is not
 * a leaf in the tree. Its list of triples is a union of its children's triples.
 * 
 * To compute the data in a join node, the data for its children is first
 * computed. Then, a join between them is executed, based on a common variables.
 * 
 * @author Polina Koleva
 *
 */
public class JoinNode extends MVNode {

	private static final Logger logger = Logger.getLogger("PRoST");
	
	private Node leftChild;
	private Node rightChild;

	public JoinNode(Node parent, Node leftChild, Node rightChild) {
		this.parent = parent;
		this.leftChild = leftChild;
		this.rightChild = rightChild;
		this.tripleGroup = getTriples();
	}

	@Override
	public void computeNodeData(SQLContext sqlContext) {
		// compute children's data
		if (getRightChild().sparkNodeData == null) {
			getRightChild().computeNodeData(sqlContext);
		}
		if (getLeftChild().sparkNodeData == null) {
			getLeftChild().computeNodeData(sqlContext);
		}
		// execute a join between the children
		final List<String> joinVariables = Utils.commonVariables(getRightChild().sparkNodeData.columns(),
				getLeftChild().sparkNodeData.columns());
		this.sparkNodeData = getLeftChild().sparkNodeData.join(getRightChild().sparkNodeData,
				scala.collection.JavaConversions.asScalaBuffer(joinVariables).seq());
	}

	public Node getLeftChild() {
		return leftChild;
	}

	public void setLeftChild(Node leftChild) {
		this.leftChild = leftChild;
	}

	public Node getRightChild() {
		return rightChild;
	}

	public void setRightChild(Node rightChild) {
		this.rightChild = rightChild;
	}

	/**
	 * To compute the triples that a join node represents, union the triples each of
	 * its children contains.
	 * 
	 * @return a list of triples
	 */
	private List<TriplePattern> getTriples() {
		ArrayList<TriplePattern> triples = new ArrayList<>();
		triples.addAll(leftChild.collectTriples());
		triples.addAll(rightChild.collectTriples());
		return triples;
	}

	/**
	 * Calculate heuristically a score for a join node. It is a multiplication of
	 * its children's scores. If a child's score is zero (when a constant is
	 * involved), it is ignored from the multiplication.
	 */
	@Override
	public float heuristicNodePriority() {
		float priority = 0;
		float leftChildPriority = this.leftChild.heuristicNodePriority();
		float rightChildPriority = this.rightChild.heuristicNodePriority();
		if (leftChildPriority == 0.0) {
			return rightChildPriority;
		} else if (rightChildPriority == 0.0) {
			return leftChildPriority;
		} else {
			priority = rightChildPriority * leftChildPriority;
		}
		return priority;
	}

	@Override
	public String toString() {
		final StringBuilder str = new StringBuilder("{");
		str.append("Join node: " + tripleGroup.size());
		str.append(" }");
		str.append(" [");
		str.append("\n Left child: " + leftChild.toString());
		str.append("\n Right child: " + rightChild.toString());
		str.append("\n]");
		return str.toString();
	}
}
