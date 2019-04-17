package joinTree;

import static java.lang.Math.min;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

import stats.CharacteristicSetStatistics;
import stats.DatabaseStatistics;

/**
 * Abstract class that represent a node which contains a list of triples. For
 * example, if there exist multiple triples with the same subject they are
 * unioned in one node and property table is queried.
 *
 * @author Polina Koleva
 */
public abstract class MVNode extends Node {

	public List<TriplePattern> tripleGroup;

	public MVNode(final DatabaseStatistics statistics) {
		super(statistics);
	}

	@Override
	public List<TriplePattern> collectTriples() {
		return tripleGroup;
	}

	private HashSet<String> computeCharacteristicSet() {
		final HashSet<String> characteristicSet = new HashSet<>();
		for (final TriplePattern pattern : tripleGroup) {
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
	private long computeStarJoinCardinality() {
		final ArrayList<CharacteristicSetStatistics> superSets = computeCharacteristicSupersets(
				this.computeCharacteristicSet(), statistics);
		long cardinality = 0;
		for (final CharacteristicSetStatistics superSet : superSets) {
			long m = 1;
			long o = 1;
			for (final TriplePattern triple : this.tripleGroup) {
				//m = m * (superSet.getTuplesPerPredicate().get(triple.predicate) / superSet.getDistinctSubjects());
				if (triple.objectType == ElementType.CONSTANT) {
					// o is min(o,sel(?o=o|?p=p)
					// min(o,sel(?o=o|?p=p) is sel(?o=o && ?p=p)/sel(?p=p)
					o = min(o, statistics.getProperties().get(triple.predicate).getBoundObjectEstimatedSelectivity());
				} else {
					m = m * (superSet.getTuplesPerPredicate().get(triple.predicate) / superSet.getDistinctSubjects());
				}
			}
			cardinality = cardinality + superSet.getDistinctSubjects() * m * o;
		}
		return cardinality;
	}

	public float heuristicNodePriority() {
		return computeStarJoinCardinality();
	}
}
