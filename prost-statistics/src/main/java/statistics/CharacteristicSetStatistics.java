package statistics;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Annotations regarding a single characteristic set. Stores the number of distinct subjects with this characteristic
 * set, and the number of tuples with each property of the set.
 **/
public class CharacteristicSetStatistics {
	private final HashMap<String, Long> tuplesPerPredicate;
	private final HashMap<String, Long> tuplesPerInversePredicate;
	private Long distinctResources;

	public CharacteristicSetStatistics() {
		tuplesPerPredicate = new HashMap<>();
		tuplesPerInversePredicate = new HashMap<>();
	}

	public void addProperty(final String property, final long count) {
		this.tuplesPerPredicate.put(property, count);
	}

	public void addInverseProperty(final String inverseProperty, final long count) {
		this.tuplesPerInversePredicate.put(inverseProperty, count);
	}

	public Long getDistinctResources() {
		return distinctResources;
	}

	public void setDistinctResources(final Long distinctResources) {
		this.distinctResources = distinctResources;
	}

	public long getPropertyTuplesNumber(final String key) {
		return this.tuplesPerPredicate.get(key);
	}

	public long getInversePropertyTuplesNumber(final String key) {
		return this.tuplesPerInversePredicate.get(key);
	}

	@Override
	public boolean equals(final Object object) {
		if (this == object) {
			return true;
		}
		if (object == null || getClass() != object.getClass()) {
			return false;
		}

		if (!this.distinctResources.equals(((CharacteristicSetStatistics) object).distinctResources)) {
			return false;
		}

		if (this.tuplesPerPredicate.size() != ((CharacteristicSetStatistics) object).tuplesPerPredicate.size()) {
			return false;
		}

		if (this.tuplesPerInversePredicate.size()
				!= ((CharacteristicSetStatistics) object).tuplesPerInversePredicate.size()) {
			return false;
		}

		for (final String property : this.getProperties()) {
			if (!this.tuplesPerPredicate.get(property).equals(
					((CharacteristicSetStatistics) object).tuplesPerPredicate.get(property))) {
				return false;
			}

			if (!this.tuplesPerInversePredicate.get(property).equals(
					((CharacteristicSetStatistics) object).tuplesPerInversePredicate.get(property))) {
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
	public boolean containsSubset(final Set<String> queryCharacteristicSet,
								  final Set<String> queryInverseCharacteristicSet) {

		assert (queryCharacteristicSet != null || queryInverseCharacteristicSet != null)
				: "Trying to compute subset of an empty characteristic set";

		if (queryCharacteristicSet != null) {
			if (this.tuplesPerPredicate.size() < queryCharacteristicSet.size()) {
				return false;
			}
			for (final String key : queryCharacteristicSet) {
				if (!tuplesPerPredicate.containsKey(key)) {
					return false;
				}
			}
		}

		if (queryInverseCharacteristicSet != null) {
			if (this.tuplesPerInversePredicate.size() < queryInverseCharacteristicSet.size()) {
				return false;
			}
			for (final String key : queryInverseCharacteristicSet) {
				if (!tuplesPerInversePredicate.containsKey(key)) {
					return false;
				}
			}
		}

		return true;
	}

	boolean hasCommonProperties(final CharacteristicSetStatistics otherSet) {
		for (final String property : otherSet.getProperties()) {
			if (this.getProperties().contains(property)) {
				return true;
			}

			if (this.getInverseProperties().contains(property)) {
				return true;
			}
		}
		return false;
	}

	public Set<String> getProperties() {
		return this.tuplesPerPredicate.keySet();
	}

	public Set<String> getInverseProperties() {
		return this.tuplesPerInversePredicate.keySet();
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