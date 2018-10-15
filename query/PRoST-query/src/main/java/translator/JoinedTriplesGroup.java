package translator;

import java.util.HashSet;

import com.hp.hpl.jena.graph.Triple;

/**
 * A group of triples that have their common variable at either their subject or object.
 */
class JoinedTriplesGroup {
	private final HashSet<Triple> wptGroup;
	private final HashSet<Triple> iwptGroup;

	JoinedTriplesGroup() {
		wptGroup = new HashSet<>();
		iwptGroup = new HashSet<>();
	}

	/**
	 * @return returns the triples whose common variable is a subject
	 */
	HashSet<Triple> getWptGroup() {
		return wptGroup;
	}

	/**
	 *
	 * @return returns the triples whose common variable is an object
	 */
	HashSet<Triple> getIwptGroup() {
		return iwptGroup;
	}
}
