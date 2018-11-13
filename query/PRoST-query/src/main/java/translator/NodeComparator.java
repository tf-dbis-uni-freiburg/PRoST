package translator;

import java.util.Comparator;

import org.apache.log4j.Logger;

import joinTree.ElementType;
import joinTree.Node;
import joinTree.TriplePattern;

/**
 * Comparator used to sort nodes when building a join tree. A score for each
 * node is based on a heuristic function. Nodes are sorted in ascending order.
 * 
 * @author Polina Koleva
 *
 */
public class NodeComparator implements Comparator<Node> {

	private static final Logger logger = Logger.getLogger("PRoST");

	/**
	 * Calculate heuristically a score for each node. A numeric value for each
	 * triple based on its predicates is collected while the data is loaded. The
	 * value is equal to the number of triples that exist in the data for a
	 * predicate. Each node represents one or more triples. Therefore, to calculate
	 * a score of a node summation over values for their triples is calculated. An
	 * exception of the rule exists only if a triples contains a constant. In this
	 * case, the heuristic value of a node is 0. Therefore, such node is pushed down
	 * in a join tree.
	 */
	public float heuristicNodePriority(final Node node) {
		float priority = 0;
		for (final TriplePattern triplePattern : node.collectTriples()) {
			final String predicate = triplePattern.predicate;
			final boolean isObjectVariable = triplePattern.objectType == ElementType.VARIABLE;
			final boolean isSubjectVariable = triplePattern.subjectType == ElementType.VARIABLE;
			if (!isObjectVariable || !isSubjectVariable) {
				priority = 0;
				break;
			} else {
				final int size = Stats.getInstance().getTableSize(predicate);
				priority += size;
			}
		}
		return priority;
	}

	@Override
	public int compare(final Node node1, final Node node2) {

		final float priorityNode1 = heuristicNodePriority(node1);
		final float priorityNode2 = heuristicNodePriority(node2);

		// for the smallest to the biggest
		return (int) Math.ceil(priorityNode1 - priorityNode2);
	}
}
