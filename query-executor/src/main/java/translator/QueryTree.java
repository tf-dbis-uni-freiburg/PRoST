package translator;

import java.util.List;

import com.hp.hpl.jena.graph.Triple;

public class QueryTree {
	private List<Triple> triples;
	private String filter;

	public QueryTree(final List<Triple> triples, final String filter) {
		this.setTriples(triples);
		this.setFilter(filter);
	}

	public QueryTree(final List<Triple> triples) {
		this.setTriples(triples);
		this.setFilter(null);
	}

	public List<Triple> getTriples() {
		return triples;
	}

	public void setTriples(final List<Triple> triples) {
		this.triples = triples;
	}

	public String getFilter() {
		return filter;
	}

	public void setFilter(final String filter) {
		this.filter = filter;
	}
}
