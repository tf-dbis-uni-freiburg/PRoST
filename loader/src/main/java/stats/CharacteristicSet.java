package stats;

import java.util.List;
import java.util.Map;

// TODO add comments

public class CharacteristicSet {

	private List<String> predicates;
	private int distinctSubjectsCount;
	// predicate, number of subjects that contain objects for the predicate
	private Map<String, Integer> subjectsPerPredicate;
	
}
