package joinTree;

import java.util.List;

import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import stats.DatabaseStatistics;

/**
 * An abstract class that each node of the JoinTree has to extend. Each node has
 * a parent and data frame that contains the data of the node.
 *
 * @author Polina Koleva
 */
public abstract class Node {
	private static final Logger logger = Logger.getLogger("PRoST");

	public Node parent;
	// the spark data set containing the data relative to this node
	public Dataset<Row> sparkNodeData;
	private Float priority;
	final DatabaseStatistics statistics;

	public Node(final DatabaseStatistics statistics) {
		this.parent = null;
		this.statistics = statistics;
	}

	public Node(final Node parent, final DatabaseStatistics statistics) {
		this.parent = parent;
		this.statistics = statistics;
	}

	/**
	 * Compute the Dataset<Row> to the data referring to this node.
	 */
	public abstract void computeNodeData(SQLContext sqlContext);

	/**
	 * Get a list of triples that each node contains. For example, {@link VPNode}
	 * represents only one triple. On the other side, {@link PTNode} contains a list
	 * of triples with the same subject.
	 */
	public abstract List<TriplePattern> collectTriples();

	/**
	 * Calculate a score for each node. Based on it, the position of the node in the
	 * join tree is determined.
	 *
	 * @return heuristic score of a node
	 */
	public float getPriority() {
		// compute for first time
		if (this.priority == null) {
			this.priority = heuristicNodePriority();
			return this.heuristicNodePriority();
		}
		return this.priority;
	}

	/**
	 * Calculate heuristically a score for each node. A numeric value for each
	 * triple based on its predicates is collected while the data is loaded. The
	 * value is equal to the number of triples that exist in the data for a
	 * predicate. Each node represents one or more triples. Therefore, to calculate
	 * a score of a node summation over values for their triples is calculated. An
	 * exception of the rule exists only if a triples contains a constant. In this
	 * case, the heuristic value of a node is 0. Therefore, such node is pushed down
	 * in a join tree. Note: This heuristic function is valid only for leaves in the
	 * tree node. For {@link JoinNode}, see the overridden method.
	 */
	public float heuristicNodePriority() {
		// TODO add usage of characteristic set
		float priority = 0;
		for (final TriplePattern triplePattern : this.collectTriples()) {
			final String predicate = triplePattern.predicate;
			final boolean isObjectVariable = triplePattern.objectType == ElementType.VARIABLE;
			final boolean isSubjectVariable = triplePattern.subjectType == ElementType.VARIABLE;
			if (!isObjectVariable || !isSubjectVariable) {
				priority = 0;
				break;
			} else {
				final int size = statistics.getPropertyStatistics().get(predicate).getTuplesNumber();
				priority += size;
			}
		}
		return priority;
	}
}