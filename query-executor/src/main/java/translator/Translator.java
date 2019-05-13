package translator;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.PriorityQueue;

import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.query.Query;
import com.hp.hpl.jena.query.QueryFactory;
import com.hp.hpl.jena.shared.PrefixMapping;
import com.hp.hpl.jena.sparql.algebra.Algebra;
import com.hp.hpl.jena.sparql.algebra.Op;
import com.hp.hpl.jena.sparql.algebra.OpWalker;
import com.hp.hpl.jena.sparql.core.Var;
import joinTree.ElementType;
import joinTree.IWPTNode;
import joinTree.JWPTNode;
import joinTree.JoinNode;
import joinTree.JoinTree;
import joinTree.Node;
import joinTree.TTNode;
import joinTree.TriplePattern;
import joinTree.VPNode;
import joinTree.WPTNode;
import org.apache.log4j.Logger;
import stats.DatabaseStatistics;
import utils.EmergentSchema;
import utils.Settings;

/**
 * This class parses the SPARQL query, build a {@link JoinTree} and save its
 * serialization in a file.
 *
 * @author Matteo Cossu
 * @author Polina Koleva
 */
public class Translator {

	private static final Logger logger = Logger.getLogger("PRoST");
	private final DatabaseStatistics statistics;
	private final Settings settings;
	private final String queryPath;
	// minimum number of triple patterns with the same subject to form a group
	// (property table)
	private PrefixMapping prefixes;

	// TODO check this, if you do not specify the treeWidth in the input parameters
	// when you are running the jar, its default value is -1.

	// TODO Move this logic to the translator
	public Translator(final Settings settings, final DatabaseStatistics statistics, final String queryPath) {
		this.settings = settings;
		this.statistics = statistics;
		this.queryPath = queryPath;
	}

	public JoinTree translateQuery() {
		// parse the query and extract prefixes
		final Query query = QueryFactory.read("file:" + queryPath);
		prefixes = query.getPrefixMapping();

		logger.info("** SPARQL QUERY **\n" + query + "\n****************");

		// extract variables, list of triples and filter
		final Op opQuery = Algebra.compile(query);
		final QueryVisitor queryVisitor = new QueryVisitor(prefixes);
		OpWalker.walk(opQuery, queryVisitor);

		final QueryTree mainTree = queryVisitor.getMainQueryTree();
		final List<Var> projectionVariables = queryVisitor.getProjectionVariables();

		// build main tree
		final Node rootNode = buildTree(mainTree.getTriples());

		// TODO fix the optional
		//		final List<Node> optionalTreeRoots = new ArrayList<>();
		//		for (int i = 0; i < queryVisitor.getOptionalQueryTrees().size(); i++) {
		//			final QueryTree currentOptionalTree = queryVisitor.getOptionalQueryTrees().get(i);
		//			// build optional tree
		//			final Node optionalTreeRoot = buildTree(currentOptionalTree.getTriples());
		//			// optionalTreeRoot.filter = currentOptionalTree.getFilter();
		//			optionalTreeRoots.add(optionalTreeRoot);
		//		}
		// final JoinTree tree = new JoinTree(rootNode, optionalTreeRoots, inputFile);

		final JoinTree tree = new JoinTree(rootNode, null, queryPath);

		// set filter
		tree.filter = mainTree.getFilter();

		// set projections
		if (projectionVariables != null) {
			// set the root node with the variables that need to be projected
			// only for the main tree
			final ArrayList<String> projectionList = new ArrayList<>();
			for (final Var projectionVariable : projectionVariables) {
				projectionList.add(projectionVariable.getVarName());
			}
			tree.setProjectionList(projectionList);
		}

		// if distinct keyword is present
		tree.setDistinct(query.isDistinct());

		logger.info("** Spark JoinTree **\n" + tree + "\n****************");
		return tree;
	}

	/*
	 * Constructs the join tree.
	 */
	private Node buildTree(final List<Triple> triples) {
		final PriorityQueue<Node> nodesQueue = getNodesQueue(triples);
		Node currentNode = null;

		// visit the hypergraph to build the tree
		while (!nodesQueue.isEmpty()) {
			currentNode = nodesQueue.poll();
			final Node relatedNode = findRelateNode(currentNode, nodesQueue);
			if (relatedNode != null) {
				// append join node to the queue
				final JoinNode joinNode = new JoinNode(null, currentNode, relatedNode, statistics);
				nodesQueue.add(joinNode);
				// add join node as a parent
				currentNode.parent = joinNode;
				relatedNode.parent = joinNode;

				// remove consumed nodes
				nodesQueue.remove(currentNode);
				nodesQueue.remove(relatedNode);
			}
		}
		return currentNode;
	}

	/**
	 * Removes an entry with the given key from <code>joinedGroups</code> if its
	 * element size is 0.
	 *
	 * @param joinedGroups a mapping of <code>JoinedTriplesGroup</code>
	 * @param key          a key from <code>joinedGroups</code>
	 */
	private void removeJoinedTriplesGroupIfEmpty(final HashMap<String, JoinedTriplesGroup> joinedGroups,
												 final String key) {
		if (joinedGroups.containsKey(key)) {
			if (joinedGroups.get(key).size() == 0) {
				joinedGroups.remove(key);
			}
		}
	}

	private PriorityQueue<Node> getNodesQueue(final List<Triple> triples) {
		final PriorityQueue<Node> nodesQueue = new PriorityQueue<>(triples.size(), new NodeComparator());
		final List<Triple> unassignedTriples = new ArrayList<>(triples);

		logger.info("Triple patterns without nodes: " + unassignedTriples.size());
		if (settings.isUsingJWPT()) {
			logger.info("Creating JWPT nodes... ");
			addJWPTNodes(unassignedTriples, nodesQueue);
			logger.info("Done! Triple patterns without nodes: " + unassignedTriples.size());
		}
		if (settings.isUsingWPT() && settings.isUsingIWPT() && settings.isGroupingTriples()) {
			logger.info("Creating WPT and IWPT nodes...");
			addMixedPTNodes(unassignedTriples, nodesQueue);
			logger.info("Done! Triple patterns without nodes: " + unassignedTriples.size());
		} else if (settings.isUsingWPT()) {
			logger.info("Creating WPT nodes...");
			addWPTNodes(unassignedTriples, nodesQueue);
			logger.info("Done! Triple patterns without nodes: " + unassignedTriples.size());
		} else if (settings.isUsingIWPT()) {
			logger.info("Creating IWPT nodes...");
			addIWPTNodes(unassignedTriples, nodesQueue);
			logger.info("Done! Triple patterns without nodes: " + unassignedTriples.size());
		}
		if (settings.isUsingVP()) {
			// VP only
			logger.info("Creating VP nodes...");
			createVpNodes(unassignedTriples, nodesQueue);
			logger.info("Done! Triple patterns without nodes: " + unassignedTriples.size());
		}
		if (settings.isUsingTT()) {
			logger.info("Creating TT nodes...");
			createTTNodes(unassignedTriples, nodesQueue);
			logger.info("Done! Triple patterns without nodes: " + unassignedTriples.size());
		}
		if (unassignedTriples.size() > 0) {
			throw new RuntimeException("Cannot generate nodes queue. Some triple patterns are not assigned to a node.");
		} else {
			return nodesQueue;
		}

	}

	/**
	 * Creates JWPT nodes given the triple patterns.
	 *
	 * @param triples    List of triple patterns. Patterns added to a node are removed from the list.
	 * @param nodesQueue Queue where the nodes are added to.
	 */
	private void addJWPTNodes(final List<Triple> triples, final PriorityQueue<Node> nodesQueue) {
		final HashMap<String, JoinedTriplesGroup> joinedGroups = getJoinedTriplesGroups(triples);
		while (!joinedGroups.isEmpty()) {
			if (settings.isGroupingTriples()) {
				// get largest group
				final String largestGroupKey = getLargestGroupKey(joinedGroups);
				final JoinedTriplesGroup largestJoinedTriplesGroup = joinedGroups.get(largestGroupKey);

				if (largestJoinedTriplesGroup.size() >= settings.getMinGroupSize()) {
					nodesQueue.add(new JWPTNode(largestJoinedTriplesGroup, prefixes, statistics));

					triples.removeAll(largestJoinedTriplesGroup.getWptGroup());
					triples.removeAll(largestJoinedTriplesGroup.getIwptGroup());

					// remove triples from smaller groups
					for (final Triple triple : largestJoinedTriplesGroup.getWptGroup()) {
						final String object = triple.getObject().toString();
						if (joinedGroups.get(object) != null) {
							joinedGroups.get(object).getIwptGroup().remove(triple);
							removeJoinedTriplesGroupIfEmpty(joinedGroups, object);
						}
					}
					for (final Triple triple : largestJoinedTriplesGroup.getIwptGroup()) {
						final String subject = triple.getSubject().toString();
						if (joinedGroups.get(subject) != null) {
							joinedGroups.get(subject).getWptGroup().remove(triple);
							removeJoinedTriplesGroupIfEmpty(joinedGroups, subject);
						}
					}
					joinedGroups.remove(largestGroupKey);
				} else {
					joinedGroups.clear(); //all available groups are smaller than the minimum group size
				}
			} else { // if grouping is disabled, all JoinedGroups contain only one triple, and each triple is in at
				// most one groups
				for (final JoinedTriplesGroup group : joinedGroups.values()) {
					nodesQueue.add(new JWPTNode(group, prefixes, statistics));
					triples.removeAll(group.getWptGroup());
					triples.removeAll(group.getIwptGroup());
				}
				joinedGroups.clear(); // avoid concurrent modifications
			}
		}
	}

	/**
	 * Given a list of triples, adds valid WPT and IWPT nodes to the queue.
	 *
	 * @param triples    The list of available triple patterns. Patterns assigned to nodes are removed.
	 * @param nodesQueue Priority queue where the nodes are added to.
	 */
	private void addMixedPTNodes(final List<Triple> triples, final PriorityQueue<Node> nodesQueue) {
		final HashMap<String, List<Triple>> objectGroups = getObjectGroups(triples);
		final HashMap<String, List<Triple>> subjectGroups = getSubjectGroups(triples);

		// repeats until there are no unassigned triple patterns left
		while (objectGroups.size() != 0 && subjectGroups.size() != 0) {
			// Calculate largest group by object
			final String largestObjectGroupKey = getLargestGroupKey(objectGroups);
			final int largestObjectGroupSize = objectGroups.get(largestObjectGroupKey).size();
			// calculate biggest group by subject
			final String largestSubjectGroupKey = getLargestGroupKey(subjectGroups);
			final int largestSubjectGroupSize = subjectGroups.get(largestSubjectGroupKey).size();

			// create nodes
			if (largestObjectGroupSize > largestSubjectGroupSize
					&& largestObjectGroupSize > settings.getMinGroupSize()) {
				// create and add the iwpt
				final List<Triple> largestObjectGroupTriples = objectGroups.get(largestObjectGroupKey);
				nodesQueue.add(new IWPTNode(largestObjectGroupTriples, prefixes, statistics));
				triples.removeAll(largestObjectGroupTriples);

				// remove triples from subject group
				for (final Triple triple : largestObjectGroupTriples) {
					final String key = triple.getObject().toString();
					if (subjectGroups.containsKey(key)) {
						subjectGroups.get(key).remove(triple);
						if (subjectGroups.get(key).size() == 0) {
							subjectGroups.remove(key);
						}
					}
				}
				objectGroups.remove(largestObjectGroupKey);
			} else if (largestSubjectGroupSize > settings.getMinGroupSize()) {
				/// create and add the wpt
				final List<Triple> largestSubjectGroupTriples = subjectGroups.get(largestSubjectGroupKey);
				nodesQueue.add(new WPTNode(largestSubjectGroupTriples, prefixes, statistics));
				triples.removeAll(largestSubjectGroupTriples);
				// remove triples from object group
				for (final Triple triple : largestSubjectGroupTriples) {
					final String key = triple.getSubject().toString();
					if (objectGroups.containsKey(key)) {
						objectGroups.get(key).remove(triple);
						if (objectGroups.get(key).size() == 0) {
							objectGroups.remove(key);
						}
					}
				}
				subjectGroups.remove(largestSubjectGroupKey);
			} else {
				break;
			}
		}
	}

	/**
	 * Given a list of triples, adds valid WPT nodes to the queue.
	 *
	 * @param triples    The list of available triple patterns. Patterns assigned to nodes are removed.
	 * @param nodesQueue Priority queue where the nodes are added to.
	 */
	private void addWPTNodes(final List<Triple> triples, final PriorityQueue<Node> nodesQueue) {
		if (EmergentSchema.isUsed()) {
			logger.info("Emergent schema is used, group triples based on subject and emergent schema.");
			final HashMap<String, HashMap<String, List<Triple>>> emergentSchemaSubjectGroups =
					getEmergentSchemaSubjectGroups(triples);
			for (final String tableName : emergentSchemaSubjectGroups.keySet()) {
				final HashMap<String, List<Triple>> emergentSubjectGroups =
						emergentSchemaSubjectGroups.get(tableName);
				for (final String subject : emergentSubjectGroups.keySet()) {
					final List<Triple> subjectTriples = emergentSubjectGroups.get(subject);
					nodesQueue.add(new WPTNode(subjectTriples, prefixes, tableName, statistics));
					triples.removeAll(subjectTriples);
				}
			}
		} else {
			final HashMap<String, List<Triple>> subjectGroups = getSubjectGroups(triples);
			for (final List<Triple> triplesGroup : subjectGroups.values()) {
				if (triplesGroup.size() > settings.getMinGroupSize() || !settings.isGroupingTriples()) {
					nodesQueue.add(new WPTNode(triplesGroup, prefixes, statistics));
					triples.removeAll(triplesGroup);
				} else {
					break;
				}
			}
		}
	}

	/**
	 * Given a list of triples, adds valid IWPT nodes to the queue.
	 *
	 * @param triples    The list of available triple patterns. Patterns assigned to nodes are removed.
	 * @param nodesQueue Priority queue where the nodes are added to.
	 */
	private void addIWPTNodes(final List<Triple> triples, final PriorityQueue<Node> nodesQueue) {
		final HashMap<String, List<Triple>> objectGroups = getObjectGroups(triples);
		for (final List<Triple> triplesGroup : objectGroups.values()) {
			if (triplesGroup.size() > settings.getMinGroupSize() || !settings.isGroupingTriples()) {
				nodesQueue.add(new IWPTNode(triplesGroup, prefixes, statistics));
				triples.removeAll(triplesGroup);
			} else {
				break;
			}
		}
	}

	/**
	 * Creates VP nodes from a list of triples.
	 *
	 * @param unassignedTriples Triples for which VP nodes will be created
	 * @param nodesQueue        PriorityQueue where created nodes are added to
	 */
	private void createVpNodes(final List<Triple> unassignedTriples, final PriorityQueue<Node> nodesQueue) {
		final List<Triple> triples = new ArrayList<>(unassignedTriples);
		for (final Triple t : triples) {
			final Node newNode = new VPNode(new TriplePattern(t, prefixes), statistics);
			nodesQueue.add(newNode);
			unassignedTriples.remove(t);
		}
	}

	/**
	 * Creates TT nodes from a list of triples.
	 *
	 * @param triples    Triples for which TT nodes will be created
	 * @param nodesQueue PriorityQueue where created nodes are added to
	 */
	private void createTTNodes(final List<Triple> triples, final PriorityQueue<Node> nodesQueue) {
		for (final Triple t : triples) {
			nodesQueue.add(new TTNode(new TriplePattern(t, prefixes), statistics));
		}
		triples.clear();
	}

	/**
	 * Groups the input triples by subject. If grouping is disabled, create one list
	 * with one element for each triple.
	 *
	 * @param triples triples to be grouped
	 * @return HashMap of triples grouped by the subject
	 */
	private HashMap<String, List<Triple>> getSubjectGroups(final List<Triple> triples) {
		final HashMap<String, List<Triple>> subjectGroups = new HashMap<>();
		int key = 0; // creates a unique key to be used when grouping is disabled, to avoid
		// overwriting values
		for (final Triple triple : triples) {
			if (!triple.getPredicate().isVariable()) {
				if (settings.isGroupingTriples()) {
					final String subject = triple.getSubject().toString(prefixes);
					if (subjectGroups.containsKey(subject)) {
						subjectGroups.get(subject).add(triple);
					} else { // new entry in the HashMap
						final List<Triple> subjTriples = new ArrayList<>();
						subjTriples.add(triple);
						subjectGroups.put(subject, subjTriples);
					}
				} else {
					final List<Triple> subjTriples = new ArrayList<>();
					subjTriples.add(triple);
					subjectGroups.put(String.valueOf(key), subjTriples);
					key++;
				}
			}
		}
		return subjectGroups;
	}

	/**
	 * Groups the input triples by subject considering the emergent schema. If two
	 * triples have the same subject, but they are not part of the same property
	 * table, they won't be grouped.
	 *
	 * @param triples triples to be grouped
	 * @return hash map of triples grouped by the subject considering the emergent schema
	 */
	private HashMap<String, HashMap<String, List<Triple>>> getEmergentSchemaSubjectGroups(final List<Triple> triples) {
		// key - table names, (subject, triples)
		final HashMap<String, HashMap<String, List<Triple>>> subjectGroups = new HashMap<>();
		for (final Triple triple : triples) {
			if (!triple.getPredicate().isVariable()) {
				final String subject = triple.getSubject().toString(prefixes);
				// find in which table this triple is stored, based on the predicate
				final String subjectTableName = EmergentSchema.getInstance()
						.getTable(statistics.getProperties().get(triple.getPredicate().toString()).getInternalName());
				// if we already have a triple for the table
				if (subjectGroups.containsKey(subjectTableName)) {
					final HashMap<String, List<Triple>> subjects = subjectGroups.get(subjectTableName);
					// if we have a triple with the same subject
					if (subjects.containsKey(subject)) {
						subjectGroups.get(subjectTableName).get(subject).add(triple);
					} else {
						final List<Triple> subjTriples = new ArrayList<>();
						subjTriples.add(triple);
						subjectGroups.get(subjectTableName).put(triple.getSubject().toString(prefixes), subjTriples);
					}
				} else {
					// add new table and a new subject to it
					final HashMap<String, List<Triple>> subjectGroup = new HashMap<>();
					final List<Triple> subjTriples = new ArrayList<>();
					subjTriples.add(triple);
					subjectGroup.put(triple.getSubject().toString(prefixes), subjTriples);
					subjectGroups.put(subjectTableName, subjectGroup);
				}
			}
		}
		return subjectGroups;
	}

	/**
	 * Groups the input triples by object.
	 *
	 * @param triples triples to be grouped
	 * @return HashMap of triples grouped by the object
	 */
	private HashMap<String, List<Triple>> getObjectGroups(final List<Triple> triples) {
		final HashMap<String, List<Triple>> objectGroups = new HashMap<>();
		int key = 0; // creates a unique key to be used when grouping is disabled, to avoid
		// overwriting values
		for (final Triple triple : triples) {
			if (!triple.getPredicate().isVariable()) {
				if (settings.isGroupingTriples()) {
					final String object = triple.getObject().toString(prefixes);
					if (objectGroups.containsKey(object)) {
						objectGroups.get(object).add(triple);
					} else { // new entry in the HashMap
						final List<Triple> objTriples = new ArrayList<>();
						objTriples.add(triple);
						objectGroups.put(object, objTriples);
					}
				} else {
					final List<Triple> objTriples = new ArrayList<>();
					objTriples.add(triple);
					objectGroups.put(String.valueOf(key), objTriples);
					key++;
				}
			}
		}
		return objectGroups;
	}

	private HashMap<String, JoinedTriplesGroup> getJoinedTriplesGroups(final List<Triple> triples) {
		final HashMap<String, JoinedTriplesGroup> joinedGroups = new HashMap<>();
		int key = 0; // creates a unique key to be used when grouping is disabled, to avoid
		// overwriting values
		for (final Triple triple : triples) {
			if (!triple.getPredicate().isVariable()) { //only patterns without a variable predicate can be grouped
				if (settings.isGroupingTriples()) {
					final String subject = triple.getSubject().toString(prefixes);
					final String object = triple.getObject().toString(prefixes);

					// group by subject value
					if (joinedGroups.containsKey(subject)) {
						joinedGroups.get(subject).getWptGroup().add(triple);
					} else {
						final JoinedTriplesGroup newGroup = new JoinedTriplesGroup();
						newGroup.getWptGroup().add(triple);
						joinedGroups.put(subject, newGroup);
					}

					// group by object value
					if (joinedGroups.containsKey(object)) {
						joinedGroups.get(object).getIwptGroup().add(triple);
					} else {
						final JoinedTriplesGroup newGroup = new JoinedTriplesGroup();
						newGroup.getIwptGroup().add(triple);
						joinedGroups.put(object, newGroup);
					}
				} else {
					final JoinedTriplesGroup newGroup = new JoinedTriplesGroup();
					newGroup.getWptGroup().add(triple);
					joinedGroups.put(String.valueOf(key), newGroup);
					key++;
				}
			}
		}

		return joinedGroups;
	}

	/**
	 * Given a Map with groups, return the key of the largest group.
	 *
	 * @param groupsMapping Map with the groups whose size are to be checked.
	 * @param <T>           Type of the groups. <code>JoinedTriplesGroup</code>
	 *                      or a list of <code>Triple</code>
	 * @return Returns the key of the biggest group
	 */
	private <T> String getLargestGroupKey(final HashMap<String, T> groupsMapping) {
		int biggestGroupSize = 0;
		String biggestGroupKey = null;

		for (final String key : groupsMapping.keySet()) {
			final T group = groupsMapping.get(key);
			final int groupSize;
			if (group instanceof JoinedTriplesGroup) {
				groupSize = ((JoinedTriplesGroup) group).size();
			} else {
				groupSize = ((List<Triple>) group).size();
			}
			if (groupSize >= biggestGroupSize) {
				biggestGroupKey = key;
				biggestGroupSize = groupSize;
			}
		}
		return biggestGroupKey;
	}

	/*
	 * Given a source node, finds another node with at least one variable in common,
	 * if there isn't return null.
	 */
	private Node findRelateNode(final Node sourceNode, final PriorityQueue<Node> availableNodes) {
		for (final TriplePattern tripleSource : sourceNode.collectTriples()) {
			for (final Node node : availableNodes) {
				for (final TriplePattern tripleDest : node.collectTriples()) {
					if (existsVariableInCommon(tripleSource, tripleDest)) {
						return node;
					}
				}
			}
		}
		return null;
	}

	/*
	 * check if two Triple Patterns share at least one variable.
	 */
	private boolean existsVariableInCommon(final TriplePattern tripleA, final TriplePattern tripleB) {
		if (tripleA.objectType == ElementType.VARIABLE
				&& (tripleA.object.equals(tripleB.subject) || tripleA.object.equals(tripleB.object))) {
			return true;
		}

		return tripleA.subjectType == ElementType.VARIABLE
				&& (tripleA.subject.equals(tripleB.subject) || tripleA.subject.equals(tripleB.object));

	}
}
