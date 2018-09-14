package translator;

import java.util.Comparator;

import joinTree.*;

public class NodeComparator implements Comparator<Node> {

	// used to sort nodes when building a join tree
	public float heuristicNodePriority(Node node){
		
		float priority = 0;

        if (node.isPropertyTable) { // Property Table NODE

			for(TriplePattern t : node.tripleGroup){
				boolean isObjectVariable = t.objectType == ElementType.VARIABLE;
				boolean isSubjectVariable = t.subjectType == ElementType.VARIABLE;
				if (!isObjectVariable || !isSubjectVariable){
					priority = 0;
					break;
				}
				String predicate = t.predicate;
                int size = Stats.getInstance().getTableSize(predicate);
				priority += (float) size; 
			}
        } else { // Vertical Partitioning NODE
			String predicate = node.triplePattern.predicate;
			boolean isObjectVariable = node.triplePattern.objectType == ElementType.VARIABLE;
			boolean isSubjectVariable = node.triplePattern.subjectType == ElementType.VARIABLE;
			if (!isObjectVariable || !isSubjectVariable){
				priority = 0;
			} else {
                int size = Stats.getInstance().getTableSize(predicate);
                priority = (float) size;
			}
		}
		return priority;
	}
	
	
	
	@Override
	public int compare(Node node1, Node node2) {
		
		float priorityNode1 = heuristicNodePriority(node1);
		float priorityNode2 = heuristicNodePriority(node2);
	
		return (int) Math.ceil(priorityNode2 - priorityNode1);
	}

}
