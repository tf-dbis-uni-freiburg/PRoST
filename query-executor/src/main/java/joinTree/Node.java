package joinTree;

import static java.lang.Math.min;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import stats.CharacteristicSetStatistics;
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
	final DatabaseStatistics statistics;
	private Double priority;

	public Node(final DatabaseStatistics statistics) {
		this.parent = null;
		this.statistics = statistics;
	}

	public Node(final Node parent, final DatabaseStatistics statistics) {
		this.parent = parent;
		this.statistics = statistics;
	}

	/**
	 * Compute the Dataset&lt;Row> to the data referring to this node.
	 */
	public abstract void computeNodeData(SQLContext sqlContext);

	/**
	 * Get a list of triples that each node contains. For example, {@link VPNode}
	 * represents only one triple. On the other side, {@link WPTNode} contains a list
	 * of triples with the same subject.
	 */
	public abstract List<TriplePattern> collectTriples();

	/**
	 * Calculate a score for each node. Based on it, the position of the node in the
	 * join tree is determined.
	 *
	 * @return heuristic score of a node
	 */
	public double getPriority() {
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
	double heuristicNodePriority() {
		if (!statistics.getCharacteristicSets().isEmpty()) {
			return computeStarJoinCardinality();
		}
		float priority = 0;
		for (final TriplePattern triplePattern : this.collectTriples()) {
			final String predicate = triplePattern.predicate;
			final boolean isObjectVariable = triplePattern.objectType == ElementType.VARIABLE;
			final boolean isSubjectVariable = triplePattern.subjectType == ElementType.VARIABLE;
			if (!isObjectVariable || !isSubjectVariable) {
				//TODO number of distinct subjects|predicates / number of tuples for the given property is a better
				// estimation
				priority = 0;
				break;
			} else {
				final int size = statistics.getProperties().get(predicate).getTuplesNumber();
				priority += size;
			}
		}
		return priority;
	}

	private HashSet<String> computeCharacteristicSet() {
		final HashSet<String> characteristicSet = new HashSet<>();
		for (final TriplePattern pattern : this.collectTriples()) {
			characteristicSet.add(pattern.predicate);
		}
		return characteristicSet;
	}

	private ArrayList<CharacteristicSetStatistics> computeCharacteristicSupersets(
			final HashSet<String> baseCharacteristicSet, final DatabaseStatistics statistics) {
		final ArrayList<CharacteristicSetStatistics> superSets = new ArrayList<>();
		for (final CharacteristicSetStatistics characteristicSetStatistics : statistics.getCharacteristicSets()) {
			if (characteristicSetStatistics.containsSubset(baseCharacteristicSet)) {
				superSets.add(characteristicSetStatistics);
			}
		}
		return superSets;
	}

	/*
	See Neumann, Thomas, and Guido Moerkotte.
	"Characteristic sets: Accurate cardinality estimation for RDF queries with multiple joins."
	2011 IEEE 27th International Conference on Data Engineering. IEEE, 2011.
	 */
	private double computeStarJoinCardinality() {
		final ArrayList<CharacteristicSetStatistics> superSets = computeCharacteristicSupersets(
				this.computeCharacteristicSet(), statistics);
		double cardinality = 0;
		for (final CharacteristicSetStatistics superSet : superSets) {
			double m = 1;
			double o = 1;
			for (final TriplePattern triple : this.collectTriples()) {
				//m = m * (superSet.getTuplesPerPredicate().get(triple.predicate) / superSet.getDistinctSubjects());
				if (triple.objectType == ElementType.CONSTANT) {
					// o is min(o,sel(?o=o|?p=p)
					// min(o,sel(?o=o|?p=p) is sel(?o=o && ?p=p)/sel(?p=p)
					o = min(o, statistics.getProperties().get(triple.predicate).getBoundObjectEstimatedSelectivity());
				} else {
					m = m * ((double) superSet.getTuplesPerPredicate().get(triple.predicate)
							/ (double) superSet.getDistinctSubjects());
				}
			}
			cardinality = cardinality + superSet.getDistinctSubjects() * m * o;
		}
		return cardinality;
	}
}