package translator.triplesGroup;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.shared.PrefixMapping;
import translator.algebraTree.bgpTree.JWPTNode;
import translator.algebraTree.bgpTree.BgpNode;
import translator.algebraTree.bgpTree.WPTNode;
import statistics.DatabaseStatistics;
import utils.Settings;

/**
 * A group of triple patterns with a common subject resource.
 */
public class ForwardTriplesGroup extends TriplesGroup {
	private final List<Triple> triplesGroup = new ArrayList<>();

	ForwardTriplesGroup(final Triple triple) {
		triplesGroup.add(triple);
		commonResource = triple.getSubject().toString();
	}

	public List<Triple> getTriples() {
		return this.triplesGroup;
	}

	public void addTriple(final Triple triple) {
		triplesGroup.add(triple);
	}

	public void removeTriples(final TriplesGroup triplesGroupToRemove) {
		this.triplesGroup.removeAll(triplesGroupToRemove.getTriples());
	}

	public int size() {
		return this.triplesGroup.size();
	}

	String getCommonResource() {
		return commonResource;
	}

	public List<Triple> getForwardTriples() {
		return this.triplesGroup;
	}

	public List<Triple> getInverseTriples() {
		return new ArrayList<>();
	}

	public List<BgpNode> createNodes(final Settings settings, final DatabaseStatistics statistics,
									 final PrefixMapping prefixes) {
		final List<BgpNode> createdNodes = new ArrayList<>();
		//TODO add existing logic for emergentSchema

		if (settings.isGroupingTriples()) {
			if (settings.isUsingWPT()) {
				createdNodes.add(new WPTNode(this.triplesGroup, prefixes, statistics, settings));
			} else if (settings.isUsingJWPTOuter() || settings.isUsingJWPTLeftouter()) {
				createdNodes.add(new JWPTNode(this, prefixes, statistics, settings));
			}
			return createdNodes;
		} else {
			for (final Triple triple : this.triplesGroup) {
				if (settings.isUsingWPT()) {
					createdNodes.add(new WPTNode(Collections.singletonList(triple), prefixes, statistics, settings));
				} else if (settings.isUsingJWPTOuter() || settings.isUsingJWPTLeftouter()) {
					createdNodes.add(new JWPTNode(triple, prefixes, statistics, true, settings));
				}
			}
			return createdNodes;
		}
	}
}