package translator.triplesGroup;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.shared.PrefixMapping;
import translator.algebraTree.bgpTree.IWPTNode;
import translator.algebraTree.bgpTree.JWPTNode;
import translator.algebraTree.bgpTree.BgpNode;
import statistics.DatabaseStatistics;
import utils.Settings;

/**
 * A group of triple patterns with a common object resource.
 */
public class InverseTriplesGroup extends TriplesGroup {
	private final List<Triple> triplesGroup = new ArrayList<>();

	InverseTriplesGroup(final Triple triple) {
		this.triplesGroup.add(triple);
		this.commonResource = triple.getObject().toString();
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
		return new ArrayList<>();
	}

	public List<Triple> getInverseTriples() {
		return this.triplesGroup;
	}

	public List<BgpNode> createNodes(final Settings settings, final DatabaseStatistics statistics,
									 final PrefixMapping prefixes) {
		final List<BgpNode> createdNodes = new ArrayList<>();
		if (settings.isGroupingTriples()) {
			if (settings.isUsingIWPT()) {
				createdNodes.add(new IWPTNode(this.triplesGroup, prefixes, statistics, settings));
			} else if (settings.isUsingJWPTOuter()) {
				createdNodes.add(new JWPTNode(this, prefixes, statistics, settings));
			}
			return createdNodes;
		} else {
			for (final Triple triple : this.triplesGroup) {
				if (settings.isUsingIWPT()) {
					createdNodes.add(new IWPTNode(Collections.singletonList(triple), prefixes, statistics, settings));
				} else if (settings.isUsingJWPTOuter()) {
					createdNodes.add(new JWPTNode(triple, prefixes, statistics, false, settings));
				}
			}
			return createdNodes;
		}
	}
}
