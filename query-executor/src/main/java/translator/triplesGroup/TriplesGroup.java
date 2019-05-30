package translator.triplesGroup;

import java.util.List;

import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.shared.PrefixMapping;
import joinTree.Node;
import stats.DatabaseStatistics;
import utils.Settings;

public abstract class TriplesGroup {

	public abstract List<Triple> getTriples();

	public abstract void removeTriples(final TriplesGroup triplesToRemove);

	public abstract void addTriple(final Triple triple);

	public abstract int size();

	abstract String getCommonResource();

	public abstract List<Node> createNodes(final Settings settings, final DatabaseStatistics statistics,
									final PrefixMapping prefixes);

	public abstract List<Triple> getForwardTriples();

	public abstract List<Triple> getInverseTriples();

}
