package joinTree;

import static java.lang.Math.min;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import statistics.CharacteristicSetStatistics;
import statistics.DatabaseStatistics;
import utils.Settings;

/**
 * An abstract class that each node of the JoinTree has to extend. Each node has
 * a parent and data frame that contains the data of the node.
 *
 * @author Polina Koleva
 */
public abstract class Node {
	private final DatabaseStatistics statistics;
	private final Settings settings;
	private Dataset<Row> sparkNodeData;
	private Double priority;

	public Node(final DatabaseStatistics statistics, final Settings settings) {
		this.statistics = statistics;
		this.settings = settings;
	}

	/**
	 * Compute the Dataset&lt;Row> to the data referring to this node.
	 */
	abstract void computeNodeData(SQLContext sqlContext);

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
		if (settings.isUsingCharacteristicSets()) {
			return computeStarJoinCardinality();
		} else {
			float priority = 0;
			for (final TriplePattern triplePattern : this.collectTriples()) {
				final String predicate = triplePattern.getPredicate();
				final boolean isObjectVariable = triplePattern.getObjectType() == ElementType.VARIABLE;
				final boolean isSubjectVariable = triplePattern.getSubjectType() == ElementType.VARIABLE;
				if (!isObjectVariable || !isSubjectVariable) {
					//TODO number of distinct subjects|predicates / number of tuples for the given property is a better
					// estimation
					priority = 0;
					break;
				} else if (triplePattern.getPredicateType() == ElementType.VARIABLE) {
					priority += statistics.getTuplesNumber();
				} else {
					final int size = statistics.getProperties().get(predicate).getTuplesNumber();
					priority += size;
				}
			}
			return priority;
		}
	}

	private HashSet<String> computeCharacteristicSet() {
		final HashSet<String> characteristicSet = new HashSet<>();
		for (final TriplePattern pattern : this.collectTriples()) {
			characteristicSet.add(pattern.getPredicate());
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
				if (triple.getObjectType() == ElementType.CONSTANT) {
					// o is min(o,sel(?o=o|?p=p)
					// min(o,sel(?o=o|?p=p) is sel(?o=o && ?p=p)/sel(?p=p)
					o = min(o,
							statistics.getProperties().get(triple.getPredicate()).getBoundObjectEstimatedSelectivity());
				} else {
					m =
							m * ((double) superSet.getPropertyTuplesNumber(triple.getPredicate())
							/ (double) superSet.getDistinctSubjects());
				}
			}
			cardinality = cardinality + superSet.getDistinctSubjects() * m * o;
		}
		return cardinality;
	}

	Dataset<Row> getSparkNodeData() {
		return sparkNodeData;
	}

	void setSparkNodeData(final Dataset<Row> sparkNodeData) {
		this.sparkNodeData = sparkNodeData;
	}

	DatabaseStatistics getStatistics() {
		return this.statistics;
	}

	public enum DataModel {
		TT("tripletable"),
		WPT("wide_property_table"),
		IWPT("inverse_wide_property_table"),
		JWPT_OUTER("joined_wide_property_table_outer"),
		JWPT_LEFTOUTER("joined_wide_property_table_leftouter"),
		JWPT_INNER("joined_wide_property_table_inner");

		private String tableName;

		DataModel(final String tableName) {
			this.tableName = tableName;
		}

		public String getTableName() {
			return this.tableName;
		}

	}
}
