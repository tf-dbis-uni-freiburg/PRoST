package translator.triplesGroup;

import java.util.ArrayList;
import java.util.List;

import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.shared.PrefixMapping;
import translator.algebraTree.bgpTree.JWPTNode;
import translator.algebraTree.bgpTree.BgpNode;
import statistics.DatabaseStatistics;
import utils.Settings;

/**
 * A group of triples that have their common variable at either their subject or object positions.
 */
public class JoinedTriplesGroup extends TriplesGroup {
	private final List<Triple> forwardTriples = new ArrayList<>();
	private final List<Triple> inverseTriples = new ArrayList<>();

	JoinedTriplesGroup(final String commonResource) {
		this.commonResource = commonResource;
	}

	void addTriplesGroup(final TriplesGroup group) {

		if (group instanceof ForwardTriplesGroup) {
			forwardTriples.addAll(group.getTriples());
		} else {
			assert (group instanceof InverseTriplesGroup) : "Expected a "
					+ "ForwardTriplesGroup or an InverseTriplesGroup, but received something else";
			inverseTriples.addAll(group.getTriples());
		}
	}

	public void addTriple(final Triple triple) {
		if (commonResource.equals(triple.getSubject().toString())) {
			forwardTriples.add(triple);
		} else if (commonResource.equals(triple.getObject().toString())) {
			inverseTriples.add(triple);
		} else {
			assert false : "Tried to add a triple to a JoinedTriplesGroup without a common resource";
		}
	}


	public List<Triple> getTriples() {
		final List<Triple> fullList = new ArrayList<>();
		fullList.addAll(this.forwardTriples);
		fullList.addAll(this.inverseTriples);
		return fullList;
	}

	public void removeTriples(final TriplesGroup triplesGroupToRemove) {
		final List<Triple> triplesToRemove = triplesGroupToRemove.getTriples();
		forwardTriples.removeAll(triplesToRemove);
		inverseTriples.removeAll(triplesToRemove);
	}

	public int size() {
		return forwardTriples.size() + inverseTriples.size();
	}

	String getCommonResource() {
		return this.commonResource;
	}

	public List<Triple> getForwardTriples() {
		return this.forwardTriples;
	}

	public List<Triple> getInverseTriples() {
		return this.inverseTriples;
	}

	public List<BgpNode> createNodes(final Settings settings, final DatabaseStatistics statistics,
									 final PrefixMapping prefixes) {
		assert settings.isUsingJWPTLeftouter() || settings.isUsingJWPTOuter() || settings.isUsingJWPTInner() : "Tried"
				+ " to created nodes for a JoinedTriplesGroup when no joined data model is enabled";

		final List<BgpNode> createdNodes = new ArrayList<>();
		if (settings.isGroupingTriples()) {
			createdNodes.add(new JWPTNode(this, prefixes, statistics, settings));
			return createdNodes;
		} else {
			for (final Triple triple : this.forwardTriples) {
				createdNodes.add(new JWPTNode(triple, prefixes, statistics, true, settings));
			}

			for (final Triple triple : this.inverseTriples) {
				createdNodes.add(new JWPTNode(triple, prefixes, statistics, false, settings));
			}
			return createdNodes;
		}
	}

}
