package translator;

import java.util.HashSet;

import com.hp.hpl.jena.graph.Triple;

/**
 * A group of triples that have their common variable at either their subject or object.
 */
public class JoinedTriplesGroup {
	private final HashSet<Triple> wptGroup;
	private final HashSet<Triple> iwptGroup;

	JoinedTriplesGroup() {
		wptGroup = new HashSet<>();
		iwptGroup = new HashSet<>();
	}

	/**
	 * @return returns the triples whose common variable is a subject
	 */
	public HashSet<Triple> getWptGroup() {
		return wptGroup;
	}

	/**
	 * @return returns the triples whose common variable is an object
	 */
	public HashSet<Triple> getIwptGroup() {
		return iwptGroup;
	}

	/**
	 * @return returns the number of elements in this group.
	 */
	public int size() {
		return wptGroup.size() + iwptGroup.size();
	}
}
