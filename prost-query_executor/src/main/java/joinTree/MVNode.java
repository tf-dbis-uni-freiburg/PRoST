package joinTree;

import java.util.ArrayList;
import java.util.List;

import statistics.DatabaseStatistics;
import utils.Settings;

/**
 * Abstract class that represent a node which contains a list of triples. For
 * example, if there exist multiple triples with the same subject they are
 * unioned in one node and property table is queried.
 *
 * @author Polina Koleva
 */
public abstract class MVNode extends Node {

	private List<TriplePattern> tripleGroup;

	MVNode(final DatabaseStatistics statistics, final Settings settings) {
		super(statistics, settings);
		tripleGroup = new ArrayList<>();
	}

	@Override
	public List<TriplePattern> collectTriples() {
		return tripleGroup;
	}

	void addTriplePattern(final TriplePattern triplePattern) {
		this.tripleGroup.add(triplePattern);
	}

	List<TriplePattern> getTripleGroup() {
		return this.tripleGroup;
	}

	void setTripleGroup(final List<TriplePattern> tripleGroup) {
		this.tripleGroup = tripleGroup;
	}

	TriplePattern getFirstTriplePattern() {
		return this.tripleGroup.get(0);
	}

	int size() {
		return this.tripleGroup.size();
	}
}
