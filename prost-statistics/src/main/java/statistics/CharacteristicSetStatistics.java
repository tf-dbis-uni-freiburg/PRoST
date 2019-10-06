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

	public void addProperty(final String property, final long count) {
		this.tuplesPerPredicate.put(property, count);
	}

	public Long getDistinctSubjects() {
		return distinctSubjects;
	}

	public void setDistinctSubjects(final Long distinctSubjects) {
		this.distinctSubjects = distinctSubjects;
	}

	public long getPropertyTuplesNumber(final String key) {
		return this.tuplesPerPredicate.get(key);
	}

	@Override
	public boolean equals(final Object object) {
		if (this == object) {
			return true;
		}
		if (object == null || getClass() != object.getClass()) {
			return false;
		}

		if (!this.distinctSubjects.equals(((CharacteristicSetStatistics) object).distinctSubjects)) {
			return false;
		}

		if (this.tuplesPerPredicate.size() != ((CharacteristicSetStatistics) object).tuplesPerPredicate.size()) {
			return false;
		}

		for (final String property : this.getProperties()) {
			if (!this.tuplesPerPredicate.get(property).equals(
					((CharacteristicSetStatistics) object).tuplesPerPredicate.get(property))) {
				return false;
			}
		}
		return true;
	}

	/**
	 * Checks if argument is a subset of this characteristic set.
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
		for (final Map.Entry<String, Long> tuple : newCharset.tuplesPerPredicate.entrySet()) {
			final String newProperty = tuple.getKey();
			final Long valueToAdd = tuple.getValue();

			if (this.tuplesPerPredicate.containsKey(newProperty)) {
				final Long oldValue = this.tuplesPerPredicate.get(newProperty);
				this.tuplesPerPredicate.put(newProperty, oldValue + valueToAdd);
			} else {
				this.tuplesPerPredicate.put(newProperty, valueToAdd);
			}
		}
	}
}