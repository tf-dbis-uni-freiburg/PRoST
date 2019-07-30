package statistics;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Annotations regarding a single characteristic set. Stores the number of distinct subjects with this
 * characteristic set, and the number of tuples with each property of the set.
 **/
public class CharacteristicSetStatistics {
	private final HashMap<String, Long> tuplesPerPredicate;
	private Long distinctSubjects;

	public CharacteristicSetStatistics() {
		tuplesPerPredicate = new HashMap<>();
	}

	public HashMap<String, Long> getTuplesPerPredicate() {
		return tuplesPerPredicate;
	}

	public Long getDistinctSubjects() {
		return distinctSubjects;
	}

	public void setDistinctSubjects(final Long distinctSubjects) {
		this.distinctSubjects = distinctSubjects;
	}

	@Override
	public boolean equals(final Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}

		if (!this.distinctSubjects.equals(((CharacteristicSetStatistics) o).distinctSubjects)) {
			return false;
		}

		if (this.getTuplesPerPredicate().size() != ((CharacteristicSetStatistics) o).getTuplesPerPredicate().size()) {
			return false;
		}

		for (final String key : this.getTuplesPerPredicate().keySet()) {
			if (!this.getTuplesPerPredicate().get(key).equals(
					((CharacteristicSetStatistics) o).getTuplesPerPredicate().get(key))) {
				return false;
			}
		}
		return true;
	}

	/**
	 * Checks if argument is a subset of this characteristic set
	 *
	 * @param queryCharacteristicSet HashSet of predicates
	 * @return true if the characteristic set contains all predicates from queryCharacteristicSet
	 */
	public boolean containsSubset(final Set<String> queryCharacteristicSet) {
		if (this.tuplesPerPredicate.size() < queryCharacteristicSet.size()) {
			return false;
		}

		for (final String key : queryCharacteristicSet) {
			if (!tuplesPerPredicate.containsKey(key)) {
				return false;
			}
		}
		return true;
	}

	boolean hasCommonProperties(final CharacteristicSetStatistics otherSet) {
		for (final String property : otherSet.getProperties()) {
			if (this.getProperties().contains(property)) {
				return true;
			}
		}
		return false;
	}

	public Set<String> getProperties() {
		return this.tuplesPerPredicate.keySet();
	}

	void merge(final CharacteristicSetStatistics newCharset) {
		for (final Map.Entry<String, Long> tuple : newCharset.getTuplesPerPredicate().entrySet()) {
			final String newProperty = tuple.getKey();
			final Long valueToAdd = tuple.getValue();

			if (this.getTuplesPerPredicate().containsKey(newProperty)) {
				final Long oldValue = this.getTuplesPerPredicate().get(newProperty);
				this.getTuplesPerPredicate().put(newProperty, oldValue + valueToAdd);
			} else {
				this.getTuplesPerPredicate().put(newProperty, valueToAdd);
			}
		}
	}
}