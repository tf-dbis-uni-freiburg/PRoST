package translator.triplesGroup;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import com.hp.hpl.jena.graph.Triple;
import utils.Settings;

/**
 * Methods to manage groups of triple patterns.
 */
public class TriplesGroupsMapping {
	private Multimap<String, TriplesGroup> triplesGroups = ArrayListMultimap.create();

	/**
	 * Takes a list of triple patterns and creates the appropriate groups according to the enabled data models.
	 *
	 * @param triples  triple patterns to be grouped.
	 * @param settings settings file with information about the enabled data models.
	 */
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

	/**
	 * Creates triple groups with a common subject resource.
	 */
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

	/**
	 * Creates triple groups with a common object resource.
	 */
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

	/**
	 * Create joined groups (common resource in either subject or object position) from the current computed groups.
	 */
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

	/**
	 * Removes the triples present in <code>triplesToRemove</code> from the existing groups. Removes empty groups.
	 *
	 * @param triplesToRemove group containing the triples that need to be removed.
	 */
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

	/**
	 * Returns the total number of triples in the groups.
	 */
	public int size() {
		return this.triplesGroups.size();
	}
}