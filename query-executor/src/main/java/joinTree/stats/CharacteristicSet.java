package joinTree.stats;

import java.util.Map;
import java.util.Set;

/**
 * POJO object representing a characteristic set. Each characteristic set
 * contains a list of unique predicates. For each predicate, it stored the
 * number of occurrences of this predicates in entities belonging to the
 * characteristic set. Moreover, we store the number of unique subjects that are
 * part of the characteristic set.
 *
 * @author Polina Koleva
 */
public class CharacteristicSet {

	private int distinctSubjectsCount;
	// predicate, number of triples for this predicate
	private Map<String, Integer> triplesPerPredicate;

	public CharacteristicSet(final Map<String, Integer> triplesPerPredicate, final int distinctSubjectsCount) {
		this.triplesPerPredicate = triplesPerPredicate;
		this.distinctSubjectsCount = distinctSubjectsCount;
	}

	public Set<String> getPredicates() {
		return this.triplesPerPredicate.keySet();
	}

	public int getDistinctSubjectsCount() {
		return distinctSubjectsCount;
	}

	public void setDistinctSubjectsCount(final int distinctSubjectsCount) {
		this.distinctSubjectsCount = distinctSubjectsCount;
	}

	public Map<String, Integer> getTriplesPerPredicate() {
		return triplesPerPredicate;
	}

	public void setTriplesPerPredicate(final Map<String, Integer> triplesPerPredicate) {
		this.triplesPerPredicate = triplesPerPredicate;
	}

}
