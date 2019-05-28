package joinTree;

import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;
import org.apache.spark.sql.SQLContext;
import stats.DatabaseStatistics;
import utils.Utils;

/**
 * A node of a join tree that contains the result of a join between two other
 * nodes. It has two children. Its child can be another join node, if it is not
 * a leaf in the tree. Its list of triples is a union of its children's triples.
 * <p>
 * To compute the data in a join node, the data for its children is first
 * computed. Then, a join between them is executed, based on a common variables.
 *
 * @author Polina Koleva
 */
public class JoinNode extends MVNode {

	private static final Logger logger = Logger.getLogger("PRoST");

	private Node leftChild;
	private Node rightChild;

	public JoinNode(final Node leftChild, final Node rightChild,
					final DatabaseStatistics statistics) {
		super(statistics);
		this.leftChild = leftChild;
		this.rightChild = rightChild;
		this.tripleGroup = getTriples();
	}

	@Override
	public void computeNodeData(final SQLContext sqlContext) {
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

	public void setLeftChild(final Node leftChild) {
		this.leftChild = leftChild;
	}

	public Node getRightChild() {
		return rightChild;
	}

	public void setRightChild(final Node rightChild) {
		this.rightChild = rightChild;
	}

	/**
	 * To compute the triples that a join node represents, union the triples each of
	 * its children contains.
	 *
	 * @return a list of triples
	 */
	private List<TriplePattern> getTriples() {
		final ArrayList<TriplePattern> triples = new ArrayList<>();
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
	public double heuristicNodePriority() {
		double priority = 0;
		final double leftChildPriority = this.leftChild.heuristicNodePriority();
		final double rightChildPriority = this.rightChild.heuristicNodePriority();
		/*if (leftChildPriority == 0.0) {
			return rightChildPriority;
		} else if (rightChildPriority == 0.0) {
			return leftChildPriority;
		} else {*/
			priority = rightChildPriority * leftChildPriority;
		//}
		return priority;
	}

	@Override
	public String toString() {
		return "{" + "Join node (" + this.getPriority() + "(: " + tripleGroup.size()
				+ " }"
				+ " ["
				+ "\n Left child: " + leftChild.toString()
				+ "\n Right child: " + rightChild.toString()
				+ "\n]";
	}
}
