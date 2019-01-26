package translator;

import java.util.Comparator;

import joinTree.*;

public class NodeComparator implements Comparator<Node> {

	// used to sort nodes when building a join tree
	public float heuristicNodePriority(final Node node) {
		float priority = 0;

		if (node instanceof PtNode || node instanceof IptNode || node instanceof JptNode || node instanceof VpJoinNode) {
			for (final TriplePattern t : node.tripleGroup) {
				final boolean isObjectVariable = t.objectType == ElementType.VARIABLE;
				final boolean isSubjectVariable = t.subjectType == ElementType.VARIABLE;
				if (!isObjectVariable || !isSubjectVariable) {
					priority = 0;
					break;
				}
				final String predicate = t.predicate;
				final int size;
				if (node instanceof PtNode) {
					size = Stats.getInstance().getTableSize(predicate, ((PtNode) node).prefixes);
				}
				else if (node instanceof IptNode){
						size = Stats.getInstance().getTableSize(predicate,((IptNode) node).prefixes);
				}
				else if (node instanceof JptNode){
						size = Stats.getInstance().getTableSize(predicate,((JptNode) node).prefixes);
				} else {
					size = Stats.getInstance().getTableSize(predicate);
				}
				priority += size;
			}
		} else { // Vertical Partitioning NODE
			final String predicate = node.triplePattern.predicate;
			final boolean isObjectVariable = node.triplePattern.objectType == ElementType.VARIABLE;
			final boolean isSubjectVariable = node.triplePattern.subjectType == ElementType.VARIABLE;
			if (!isObjectVariable || !isSubjectVariable) {
				priority = 0;
			} else {
				final int size;
				if (node instanceof VpNode){
					size = Stats.getInstance().getTableSize(predicate,((VpNode) node).prefixes);
				} else {
					size = Stats.getInstance().getTableSize(predicate);
				}
				priority = size;
			}
		}
		return priority;
	}

	@Override
	public int compare(final Node node1, final Node node2) {

		final float priorityNode1 = heuristicNodePriority(node1);
		final float priorityNode2 = heuristicNodePriority(node2);

		return (int) Math.ceil(priorityNode2 - priorityNode1);
	}

}
