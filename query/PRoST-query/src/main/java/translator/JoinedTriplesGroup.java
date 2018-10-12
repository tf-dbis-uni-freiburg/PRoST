package translator;

import java.util.HashSet;

import com.hp.hpl.jena.graph.Triple;

public class JoinedTriplesGroup {
	private final HashSet<Triple> wptGroup;
	private final HashSet<Triple> iwptGroup;

	JoinedTriplesGroup() {
		wptGroup = new HashSet<>();
		iwptGroup = new HashSet<>();
	}

	public HashSet<Triple> getWptGroup() {
		return wptGroup;
	}

	public HashSet<Triple> getIwptGroup() {
		return iwptGroup;
	}
}
