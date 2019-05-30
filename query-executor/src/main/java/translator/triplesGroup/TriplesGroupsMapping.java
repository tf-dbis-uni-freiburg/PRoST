package translator.triplesGroup;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import com.hp.hpl.jena.graph.Triple;
import org.apache.log4j.Logger;
import utils.Settings;

public class TriplesGroupsMapping {
	private static final Logger logger = Logger.getLogger("PRoST");
	private Multimap<String, TriplesGroup> triplesGroups = ArrayListMultimap.create();

	public TriplesGroupsMapping(final List<Triple> triples, final Settings settings) {
		if (settings.isUsingWPT()
				|| settings.isUsingJWPTOuter() || settings.isUsingJWPTLeftouter() || settings.isUsingJWPTInner()) {
			createForwardGroups(triples);
		}

		if (settings.isUsingIWPT()
				|| settings.isUsingJWPTOuter() || settings.isUsingJWPTLeftouter() || settings.isUsingJWPTInner()) {
			createInverseGroups(triples);
		}

		if (settings.isUsingJWPTOuter() || settings.isUsingJWPTLeftouter() || settings.isUsingJWPTInner()) {
			createJoinedGroups();
		}
	}

	private void createForwardGroups(final List<Triple> triples) {
		for (final Triple triple : triples) {
			TriplesGroup forwardGroup = null;
			final Collection<TriplesGroup> groups = triplesGroups.get(triple.getSubject().toString());
			for (final TriplesGroup group : groups) {
				if (group instanceof ForwardTriplesGroup) {
					forwardGroup = group;
					forwardGroup.addTriple(triple);
					break;
				}
			}
			if (forwardGroup == null) { //either not grouping triples or there is no existing group for the given triple
				forwardGroup = new ForwardTriplesGroup(triple);
				triplesGroups.put(triple.getSubject().toString(), forwardGroup);
			}
		}
	}

	private void createInverseGroups(final List<Triple> triples) {
		for (final Triple triple : triples) {
			TriplesGroup inverseGroup = null;

			final Collection<TriplesGroup> groups = triplesGroups.get(triple.getObject().toString());
			for (final TriplesGroup group : groups) {
				if (group instanceof InverseTriplesGroup) {
					inverseGroup = group;
					inverseGroup.addTriple(triple);
					break;
				}
			}

			if (inverseGroup == null) {
				inverseGroup = new InverseTriplesGroup(triple);
				triplesGroups.put(triple.getObject().toString(), inverseGroup);
			}
		}
	}

	private void createJoinedGroups() {
		for (final String key : triplesGroups.keySet()) {
			final Collection<TriplesGroup> groups = triplesGroups.get(key);
			if (groups.size() > 1) {
				final JoinedTriplesGroup joinedTriplesGroup = new JoinedTriplesGroup(key);
				for (final TriplesGroup group : groups) {
					joinedTriplesGroup.addTriplesGroup(group);
				}
				this.triplesGroups.put(key, joinedTriplesGroup);
			}
		}
	}

	/**
	 * Heuristically chooses the best group, according to the enabled data models in the database.
	 * The largest group is returned.
	 * When multiple groups have the same size, the following rules are checked (first valid rule is used; the first
	 * found group takes priority):
	 * 1. If an inner joined wide property table is enabled, joined groups are chosen (inner jwpt have the fewest
	 * tuples)
	 * 2. Forward groups are chosen if wpt is enabled
	 * 3. Joined groups are chosen if jwpt_leftouter is enabled (jwpt_outer have potentially the
	 * most tuples)
	 * 4. The current group is chosen if no group was chosen in the previous cases.
	 *
	 * @param settings The enabled data model in the settings file are used in the heuristic.
	 * @return The selected group.
	 */
	public TriplesGroup extractBestTriplesGroup(final Settings settings) {
		TriplesGroup largestGroup = null;
		int size = 0;
		for (final TriplesGroup group : this.triplesGroups.values()) {
			final int groupSize = group.size();
			if (groupSize > size) {
				largestGroup = group;
				size = groupSize;
			} else if (settings.isUsingJWPTInner()) {
				if (!(largestGroup instanceof JoinedTriplesGroup) && (group instanceof JoinedTriplesGroup)) {
					largestGroup = group;
				}
			} else if (settings.isUsingWPT()) {
				if (!(largestGroup instanceof ForwardTriplesGroup) && (group instanceof ForwardTriplesGroup)) {
					largestGroup = group;
				}
			} else if (settings.isUsingJWPTLeftouter()) {
				if (!(largestGroup instanceof JoinedTriplesGroup) && (group instanceof JoinedTriplesGroup)) {
					largestGroup = group;
				}
			}
		}

		assert (largestGroup != null && size > 0) : "No best group was extracted";

		this.triplesGroups.remove(largestGroup.getCommonResource(), largestGroup);

		return largestGroup;
	}

	public void removeTriples(final TriplesGroup triplesToRemove) {
		final Iterator<TriplesGroup> i = this.triplesGroups.values().iterator();
		while (i.hasNext()) {
			final TriplesGroup group = i.next();
			group.removeTriples(triplesToRemove);
			if (group.size() == 0) {
				i.remove();
			}
		}
	}

	public int size() {
		return this.triplesGroups.size();
	}
}