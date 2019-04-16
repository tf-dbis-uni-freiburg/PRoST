package stats;

import java.util.HashMap;
import java.util.Set;

public class CharacteristicSetStatistics {
	private Long distinctSubjects;
	private HashMap<String, Long> tuplesPerPredicate;

	public CharacteristicSetStatistics() {
		tuplesPerPredicate = new HashMap<>();
	}

	public Set<String> getPredicates() {
		return tuplesPerPredicate.keySet();
	}

	public HashMap<String, Long> getTuplesPerPredicate() {
		return tuplesPerPredicate;
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

		if (!this.distinctSubjects.equals(((CharacteristicSetStatistics)o).distinctSubjects)){
			return false;
		}

		if (this.getTuplesPerPredicate().size() != ((CharacteristicSetStatistics)o).getTuplesPerPredicate().size()) {
			return false;
		}

		for (String key:this.getTuplesPerPredicate().keySet()) {
			if (!this.getTuplesPerPredicate().get(key).equals(((CharacteristicSetStatistics)o).getTuplesPerPredicate().get(key))) {
				return false;
			}
		}
		return true;
	}

}
