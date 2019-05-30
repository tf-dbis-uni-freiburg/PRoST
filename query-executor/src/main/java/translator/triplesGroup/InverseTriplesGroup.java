package translator.triplesGroup;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.shared.PrefixMapping;
import joinTree.IWPTNode;
import joinTree.JWPTNode;
import joinTree.Node;
import stats.DatabaseStatistics;
import utils.Settings;

public class InverseTriplesGroup extends TriplesGroup {
	private List<Triple> triplesGroup = new ArrayList<>();
	private String commonResource;

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
		return null;
	}

	public List<Triple> getInverseTriples() {
		return this.triplesGroup;
	}

	public List<Node> createNodes(final Settings settings, final DatabaseStatistics statistics,
								  final PrefixMapping prefixes) {
		final List<Node> createdNodes = new ArrayList<>();
		if (settings.isGroupingTriples()) {
			if (settings.isUsingIWPT()) {
				createdNodes.add(new IWPTNode(this.triplesGroup, prefixes, statistics));
			} else if (settings.isUsingJWPTOuter()) {
				createdNodes.add(new JWPTNode(this, prefixes, statistics));
			}
			return createdNodes;
		} else {
			for (final Triple triple : this.triplesGroup) {
				if (settings.isUsingIWPT()) {
					createdNodes.add(new IWPTNode(Collections.singletonList(triple), prefixes, statistics));
				} else if (settings.isUsingJWPTOuter()) {
					createdNodes.add(new JWPTNode(triple, prefixes, statistics, false));
				}
			}
			return createdNodes;
		}
	}
}
