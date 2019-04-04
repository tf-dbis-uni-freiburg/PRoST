package translator;

import java.util.Comparator;

import joinTree.Node;
import org.apache.log4j.Logger;

/**
 * Comparator used to sort nodes when building a join tree. A score for each
 * node is based on a heuristic function. Nodes are sorted in ascending order.
 *
 * @author Polina Koleva
 */
public class NodeComparator implements Comparator<Node> {

	private static final Logger logger = Logger.getLogger("PRoST");

	@Override
	public int compare(final Node node1, final Node node2) {
		final float priorityNode1 = node1.getPriority();
		final float priorityNode2 = node2.getPriority();

		// for the smallest to the biggest
		return (int) Math.ceil(priorityNode1 - priorityNode2);
	}
}
