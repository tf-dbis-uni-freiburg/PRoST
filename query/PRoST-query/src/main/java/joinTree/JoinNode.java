package joinTree;

import java.util.ArrayList;
import java.util.List;
import org.apache.spark.sql.SQLContext;

import executor.Utils;

public class JoinNode extends MVNode {

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
		if (getRightChild().sparkNodeData == null) {
			getRightChild().computeNodeData(sqlContext);
		}
		if (getLeftChild().sparkNodeData == null) {
			getLeftChild().computeNodeData(sqlContext);
		}
		// compute join between both children
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
	
	private List<TriplePattern> getTriples(){
		ArrayList<TriplePattern> triples = new ArrayList<>();
		triples.addAll(leftChild.collectTriples());
		triples.addAll(rightChild.collectTriples());
		return triples;
	}
}
