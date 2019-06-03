package translator.triplesGroup;

import java.util.List;

import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.shared.PrefixMapping;
import joinTree.Node;
import stats.DatabaseStatistics;
import utils.Settings;

/**
 * An abstract group of triple patterns.
 */
public abstract class TriplesGroup {
	String commonResource;

	public abstract List<Triple> getTriples();

	/**
	 * Removes the triples present in <code>triplesToRemove</code> from this group.
	 */
	public abstract void removeTriples(final TriplesGroup triplesToRemove);

	/**
	 * Adds the given triple pattern to this group.
	 */
	public abstract void addTriple(final Triple triple);

	/**
	 * Returns the number of triple patterns present in this group.
	 */
	public abstract int size();

	abstract String getCommonResource();

	/**
	 * Creates a {@link Node} object for this group of triples.
	 * @param settings {@link Settings} object containing information about available data models, and whether nodes
	 *                                    with grouped triple patterns is enabled.
	 * @return The list of created nodes. Returns null if no group could be created.
	 */
	public abstract List<Node> createNodes(final Settings settings, final DatabaseStatistics statistics,
										   final PrefixMapping prefixes);

	/**
	 * Returns a list containing only the triple patterns whose subject resource is equal to
	 * {@link this.commonResource}.
	 */
	public abstract List<Triple> getForwardTriples();

	/**
	 * Returns a list containing only the triple patterns whose object resource is equal to
	 * {@link this.commonResource}.
	 */
	public abstract List<Triple> getInverseTriples();

}
