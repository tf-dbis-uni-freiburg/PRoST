package stats;

import java.util.HashMap;
import java.util.HashSet;

/**
	Annotations regarding a single characteristic set. Stores the number of distinct subjects with this
	characteristic set, and the number of tuples with each property of the set.
**/
public class CharacteristicSetStatistics {
	private Long distinctSubjects;
	private final HashMap<String, Long> tuplesPerPredicate;

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

	public boolean containsSubset(final HashSet<String> queryCharacteristicSet) {
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

}
