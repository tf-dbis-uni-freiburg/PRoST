package Translator;

import java.util.List;

import com.hp.hpl.jena.graph.Triple;

public class QueryTree {
	private List<Triple> triples;
	private String filter;
	
	public QueryTree(List<Triple> triples, String filter) {
		this.setTriples(triples);
		this.setFilter(filter);
	}
	
	public QueryTree(List<Triple> triples) {
		this.setTriples(triples);
		this.setFilter(null);
	}

	public List<Triple> getTriples() {
		return triples;
	}

	public void setTriples(List<Triple> triples) {
		this.triples = triples;
	}

	public String getFilter() {
		return filter;
	}

	public void setFilter(String filter) {
		this.filter = filter;
	}
}
