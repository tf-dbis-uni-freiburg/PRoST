package translator;

import java.util.Comparator;

import org.apache.log4j.Logger;

import joinTree.ElementType;
import joinTree.Node;
import joinTree.TriplePattern;
import joinTree.VpNode;

public class NodeComparator implements Comparator<Node> {

	private static final Logger logger = Logger.getLogger("PRoST");

	// used to sort nodes when building a join tree
	public float heuristicNodePriority(final Node node) {
		float priority = 0;
		logger.info(node.collectTriples());
		for (final TriplePattern triplePattern : node.collectTriples()) {
			final String predicate = triplePattern.predicate;
			logger.info(triplePattern.predicate);
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
		logger.info(priority);
//		if (node instanceof PtNode || node instanceof IptNode || node instanceof JptNode) {
//			for (final TriplePattern t : ((MVNode) node).tripleGroup) {
//				final boolean isObjectVariable = t.objectType == ElementType.VARIABLE;
//				final boolean isSubjectVariable = t.subjectType == ElementType.VARIABLE;
//				if (!isObjectVariable || !isSubjectVariable) {
//					priority = 0;
//					break;
//				}
//				final String predicate = t.predicate;
//				final int size = Stats.getInstance().getTableSize(predicate);
//				priority += size;
//			}
//		} else { // Vertical Partitioning NODE
//			final String predicate = ((VpNode) node).triplePattern.predicate;
//			final boolean isObjectVariable = ((VpNode) node).triplePattern.objectType == ElementType.VARIABLE;
//			final boolean isSubjectVariable = ((VpNode) node).triplePattern.subjectType == ElementType.VARIABLE;
//			if (!isObjectVariable || !isSubjectVariable) {
//				priority = 0;
//			} else {
//				final int size = Stats.getInstance().getTableSize(predicate);
//				priority = size;
//			}
//		}
		return priority;
	}

	@Override
	public int compare(final Node node1, final Node node2) {

		final float priorityNode1 = heuristicNodePriority(node1);
		final float priorityNode2 = heuristicNodePriority(node2);

		// smallest to biggest
		return (int) Math.ceil(priorityNode1 - priorityNode2);
	}
}
