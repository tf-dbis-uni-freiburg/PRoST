package translator;

import java.util.HashSet;

import com.hp.hpl.jena.graph.Triple;

public class JoinedTriplesGroup {
	private final HashSet<Triple> wptGroup;
	private final HashSet<Triple> iwptGroup;

	public JoinedTriplesGroup() {
		wptGroup = new HashSet<>();
		iwptGroup = new HashSet<>();
	}

	public HashSet<Triple> getWptGroup() {
		return wptGroup;
	}

	public HashSet<Triple> getIwptGroup() {
		return iwptGroup;
	}

	public void addWptTriple(final Triple triple) {
		wptGroup.add(triple);
	}

	public void addIwptTriple(final Triple triple) {
		iwptGroup.add(triple);
	}

	public void removeWptTriple(final Triple triple) {
		wptGroup.remove(triple);
	}

	public void removeIwptTriple(final Triple triple) {
		iwptGroup.remove(triple);
	}
}
