package stats;

import java.util.HashMap;
import java.util.Set;

public class CharacteristicSetStatistics {
	private String tableName;
	private Long distinctSubjects;
	private HashMap<String, Long> tuplesPerPredicate;

	CharacteristicSetStatistics() {
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
}
