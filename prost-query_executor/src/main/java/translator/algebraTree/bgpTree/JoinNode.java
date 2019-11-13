package translator.algebraTree.bgpTree;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.sql.SQLContext;
import statistics.DatabaseStatistics;
import utils.Settings;
import utils.Utils;

/**
 * A node of a bgp tree that contains the result of a join between two other nodes. It has two children. Its child can
 * be another join node, if it is not a leaf in the tree. Its list of triples is a union of its children's triples.
 * <p>
 * To compute the data in a join node, the data for its children is first computed. Then, a join between them is
 * executed, based on a common variables.
 *
 * @author Polina Koleva
 */
public class JoinNode extends MVNode {
	private final BgpNode leftChild;
	private final BgpNode rightChild;

	public JoinNode(final BgpNode leftChild, final BgpNode rightChild,
					final DatabaseStatistics statistics, final Settings settings) {
		super(statistics, settings);
		this.leftChild = leftChild;
		this.rightChild = rightChild;
		this.setTripleGroup(getTriples());
	}

	@Override
	public void computeNodeData(final SQLContext sqlContext) {
		this.getLeftChild().computeNodeData(sqlContext);
		this.getRightChild().computeNodeData(sqlContext);

		// execute a join between the children
		final List<String> joinVariables = Utils.commonVariables(getRightChild().getSparkNodeData().columns(),
				getLeftChild().getSparkNodeData().columns());

		this.setSparkNodeData(getLeftChild().getSparkNodeData().join(getRightChild().getSparkNodeData(),
				scala.collection.JavaConversions.asScalaBuffer(joinVariables).seq()));
	}

	public BgpNode getLeftChild() {
		return leftChild;
	}

	public BgpNode getRightChild() {
		return rightChild;
	}

	/**
	 * To compute the triples that a join node represents, union the triples each of its children contains.
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
	 * Calculate heuristically a score for a join node. It is a multiplication of its children's scores. If a child's
	 * score is zero (when a constant is involved), it is ignored from the multiplication.
	 */
	@Override
	public double heuristicNodePriority() {
		final double leftChildPriority = this.leftChild.heuristicNodePriority();
		final double rightChildPriority = this.rightChild.heuristicNodePriority();
		/*if (leftChildPriority == 0.0) {
			return rightChildPriority;
		} else if (rightChildPriority == 0.0) {
			return leftChildPriority;
		} else {*/

		return rightChildPriority * leftChildPriority;
	}

	@Override
	public String toString() {
		return "{" + "Join node (" + this.getPriority() + "): " + this.size()
				+ " }"
				+ " ["
				+ "\n Left child: " + leftChild.toString()
				+ "\n Right child: " + rightChild.toString()
				+ "\n]";
	}
}
