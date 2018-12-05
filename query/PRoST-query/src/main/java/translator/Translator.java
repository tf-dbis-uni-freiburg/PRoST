package translator;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.PriorityQueue;

import org.apache.log4j.Logger;

import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.query.Query;
import com.hp.hpl.jena.query.QueryFactory;
import com.hp.hpl.jena.shared.PrefixMapping;
import com.hp.hpl.jena.sparql.algebra.Algebra;
import com.hp.hpl.jena.sparql.algebra.Op;
import com.hp.hpl.jena.sparql.algebra.OpWalker;
import com.hp.hpl.jena.sparql.core.Var;

import joinTree.ElementType;
import joinTree.IptNode;
import joinTree.JoinNode;
import joinTree.JoinTree;
import joinTree.JptNode;
import joinTree.Node;
import joinTree.PtNode;
import joinTree.TriplePattern;
import joinTree.VpNode;
import utils.EmergentSchema;
import utils.Stats;

/**
 * This class parses the SPARQL query, build a {@link JoinTree} and save its
 * serialization in a file.
 *
 * @author Matteo Cossu
 * @author Polina Koleva
 */
public class Translator {

	private static final Logger logger = Logger.getLogger("PRoST");

	// minimum number of triple patterns with the same subject to form a group
	final int DEFAULT_MIN_GROUP_SIZE = 2;

	String inputFile;
	int treeWidth;

	PrefixMapping prefixes;
	private boolean useVerticalPartitioning = false;
	private boolean usePropertyTable = false;
	private boolean useInversePropertyTable = false;
	private boolean useJoinedPropertyTable = false;

	// if triples are grouped(by subject/object) when using property table
	private boolean groupingDisabled = false;

	// TODO check this, if you do not specify the treeWidth in the input parameters
	// when you are running the jar, its default value is -1.
	// TODO Move this logic to the translator
	public Translator(final String input, final int treeWidth) {
		inputFile = input;
		this.treeWidth = treeWidth;
	}

	public JoinTree translateQuery() {
		// parse the query and extract prefixes
		final Query query = QueryFactory.read("file:" + inputFile);
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

		final JoinTree tree = new JoinTree(rootNode, null, inputFile);

		// set filter
		tree.filter = mainTree.getFilter();

		// set projections
		if (projectionVariables != null) {
			// set the root node with the variables that need to be projected
			// only for the main tree
			final ArrayList<String> projectionList = new ArrayList<>();
			for (int i = 0; i < projectionVariables.size(); i++) {
				projectionList.add(projectionVariables.get(i).getVarName());
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
	public Node buildTree(final List<Triple> triples) {
		final PriorityQueue<Node> nodesQueue = getNodesQueue(triples);
		Node currentNode = null;

		// visit the hypergraph to build the tree
		while (!nodesQueue.isEmpty()) {
			currentNode = nodesQueue.poll();
			Node relatedNode = findRelateNode(currentNode, nodesQueue);
			if (relatedNode != null) {
				// append join node to the queue
				JoinNode joinNode = new JoinNode(null, currentNode, relatedNode);
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

	private PriorityQueue<Node> getNodesQueue(final List<Triple> triples) {
		final PriorityQueue<Node> nodesQueue = new PriorityQueue<>(triples.size(), new NodeComparator());

		if (useJoinedPropertyTable) {
			logger.info("JWPT model used.");
			final HashMap<String, JoinedTriplesGroup> joinedGroups = getJoinedGroups(triples);

			while (!joinedGroups.isEmpty()) {
				// get biggest group
				final String biggestGroupKey = getBiggestGroupKey(joinedGroups);

				// remove from smaller groups
				for (final Triple triple : joinedGroups.get(biggestGroupKey).getWptGroup()) {
					final String object = triple.getObject().toString();
					joinedGroups.get(object).removeIwptTriple(triple);
					if (joinedGroups.get(object).getIwptGroup().size()
							+ joinedGroups.get(object).getWptGroup().size() == 0) {
						joinedGroups.remove(object);
					}
				}

				for (final Triple triple : joinedGroups.get(biggestGroupKey).getIwptGroup()) {
					final String subject = triple.getSubject().toString();
					joinedGroups.get(subject).removeWptTriple(triple);
					if (joinedGroups.get(subject).getIwptGroup().size()
							+ joinedGroups.get(subject).getWptGroup().size() == 0) {
						joinedGroups.remove(subject);
					}
				}
				// the number of triples for a JWPT
				int joinedTriplesCount = joinedGroups.get(biggestGroupKey).getIwptGroup().size()
						+ joinedGroups.get(biggestGroupKey).getWptGroup().size();
				// if the number of grouped triples is less that the minimum and VP is enables
				// create VP nodes; otherwise create a JWPT node
				if (useVerticalPartitioning && joinedTriplesCount < DEFAULT_MIN_GROUP_SIZE) {
					for (final Triple t : joinedGroups.get(biggestGroupKey).getIwptGroup()) {
						final String tableName = Stats.getInstance().findTableName(t.getPredicate().toString());
						final Node newNode = new VpNode(new TriplePattern(t, prefixes), tableName);
						nodesQueue.add(newNode);
					}
					for (final Triple t : joinedGroups.get(biggestGroupKey).getWptGroup()) {
						final String tableName = Stats.getInstance().findTableName(t.getPredicate().toString());
						final Node newNode = new VpNode(new TriplePattern(t, prefixes), tableName);
						nodesQueue.add(newNode);
					}
				} else {
					nodesQueue.add(new JptNode(joinedGroups.get(biggestGroupKey), prefixes));
				}
				joinedGroups.remove(biggestGroupKey);
			}
		} else if (usePropertyTable && !useInversePropertyTable) {
			// IWPT disabled
			logger.info("WPT model is used. IWPRT disabled.");
			HashMap<String, List<Triple>> subjectGroups = new HashMap<>();
			HashMap<String, HashMap<String, List<Triple>>> emergentSchemaSubjectGroups = new HashMap<>();
			// group by subject, check if emergent schema option is set
			if (EmergentSchema.isUsed()) {
				logger.info("Emergent schema is used, group triples based on subject and emergent schema.");
				emergentSchemaSubjectGroups = getEmergentSchemaSubjectGroups(triples);
				for (final String tableName : emergentSchemaSubjectGroups.keySet()) {
					HashMap<String, List<Triple>> emergentSubjectGroups = emergentSchemaSubjectGroups.get(tableName);
					for (final String subject : emergentSubjectGroups.keySet()) {
						List<Triple> subjectTriples = emergentSubjectGroups.get(subject);
						nodesQueue.add(new PtNode(subjectTriples, prefixes, tableName));
					}
				}
			} else {
				logger.info("Group triples based on subject, emergent schema is not considered.");
				subjectGroups = getSubjectGroups(triples);
				for (final String subject : subjectGroups.keySet()) {
					List<Triple> subjectTriples = subjectGroups.get(subject);
					// if the number of grouped triples is less that the minimum and VP is enables
					// create VP nodes; otherwise create a WPT node
					if (useVerticalPartitioning && subjectTriples.size() < DEFAULT_MIN_GROUP_SIZE) {
						for (final Triple t : subjectTriples) {
							final String tableName = Stats.getInstance().findTableName(t.getPredicate().toString());
							final Node newNode = new VpNode(new TriplePattern(t, prefixes), tableName);
							nodesQueue.add(newNode);
						}
					} else {
						// create WPT nodes
						if (groupingDisabled) {
							// for each triple, create an individual WPT node, no grouping by subject is
							// done
							for (final Triple t : subjectTriples) {
								nodesQueue.add(new PtNode(Arrays.asList(t), prefixes));
							}
						} else {
							// create a WPT node for each group which contains triples with the same subject
							nodesQueue.add(new PtNode(subjectTriples, prefixes));
						}
					}
				}
			}
		} else if (!usePropertyTable && useInversePropertyTable) {
			logger.info("IWPT is used. WPT disbaled!");
			// PT disabled
			final HashMap<String, List<Triple>> objectGroups = getObjectGroups(triples);

			// for each group of triples
			for (final String object : objectGroups.keySet()) {
				List<Triple> objectTriples = objectGroups.get(object);
				// if the number of triples in the current group is less that the minimum
				// and VP is enabled, create VP node
				// otherwise create IPT node without considering the number of triples in the
				// group
				if (useVerticalPartitioning && objectTriples.size() < DEFAULT_MIN_GROUP_SIZE) {
					for (final Triple t : objectTriples) {
						final String tableName = Stats.getInstance().findTableName(t.getPredicate().toString());
						final Node newNode = new VpNode(new TriplePattern(t, prefixes), tableName);
						nodesQueue.add(newNode);
					}
				} else {
					nodesQueue.add(new IptNode(objectTriples, prefixes));
				}
			}
		} else if (usePropertyTable && useInversePropertyTable) {
			// RPT, PT enabled
			final HashMap<String, List<Triple>> objectGroups = getObjectGroups(triples);
			final HashMap<String, List<Triple>> subjectGroups = getSubjectGroups(triples);
			logger.info("WPT and IWPT are used.");

			// repeats until there are no unassigned triple patterns left
			while (objectGroups.size() != 0 && subjectGroups.size() != 0) {
				// Calculate biggest group by object
				String biggestObjectGroupIndex = "";
				int biggestObjectGroupSize = 0;
				List<Triple> biggestObjectGroupTriples = new ArrayList<>();
				for (final HashMap.Entry<String, List<Triple>> entry : objectGroups.entrySet()) {
					final int size = entry.getValue().size();
					if (size > biggestObjectGroupSize) {
						biggestObjectGroupIndex = entry.getKey();
						biggestObjectGroupSize = size;
						biggestObjectGroupTriples = entry.getValue();
					}
				}

				// calculate biggest group by subject
				String biggestSubjectGroupIndex = "";
				int biggestSubjectGroupSize = 0;
				List<Triple> biggestSubjectGroupTriples = new ArrayList<>();
				for (final HashMap.Entry<String, List<Triple>> entry : subjectGroups.entrySet()) {
					final int size = entry.getValue().size();
					if (size > biggestSubjectGroupSize) {
						biggestSubjectGroupIndex = entry.getKey();
						biggestSubjectGroupSize = size;
						biggestSubjectGroupTriples = entry.getValue();
					}
				}

				// create nodes
				if (biggestObjectGroupSize > biggestSubjectGroupSize) {
					// create and add either RPT or VP node
					if (useVerticalPartitioning && biggestObjectGroupSize < DEFAULT_MIN_GROUP_SIZE) {
						// create VP nodes
						for (final Triple t : biggestObjectGroupTriples) {
							final String tableName = Stats.getInstance().findTableName(t.getPredicate().toString());
							final Node newNode = new VpNode(new TriplePattern(t, prefixes), tableName);
							nodesQueue.add(newNode);
						}
					} else {
						nodesQueue.add(new IptNode(biggestObjectGroupTriples, prefixes));
					}
					removeTriplesFromGroups(biggestObjectGroupTriples, subjectGroups); // remove empty groups
					objectGroups.remove(biggestObjectGroupIndex); // remove group of created node
				} else {
					// create and add the PT or VP node
					if (useVerticalPartitioning && biggestSubjectGroupSize >= DEFAULT_MIN_GROUP_SIZE) {
						// create VP nodes
						for (final Triple t : biggestSubjectGroupTriples) {
							final String tableName = Stats.getInstance().findTableName(t.getPredicate().toString());
							final Node newNode = new VpNode(new TriplePattern(t, prefixes), tableName);
							nodesQueue.add(newNode);
						}
					} else {
						nodesQueue.add(new PtNode(biggestSubjectGroupTriples, prefixes));
					}
					removeTriplesFromGroups(biggestSubjectGroupTriples, objectGroups); // remove empty groups
					subjectGroups.remove(biggestSubjectGroupIndex); // remove group of created node
				}
			}
		} else if (useVerticalPartitioning) {
			// VP only
			logger.info("VP model only");
			for (final Triple t : triples) {
				final String tableName = Stats.getInstance().findTableName(t.getPredicate().toString());
				final Node newNode = new VpNode(new TriplePattern(t, prefixes), tableName);
				nodesQueue.add(newNode);
			}
		}
		return nodesQueue;
	}

	/**
	 * Remove every instance of a triple from input triples from the given groups
	 * and guarantees that there are no empty entries in groups.
	 *
	 * @param triples list of triples to be removed
	 * @param groups  HashMap containing a list of grouped triples
	 */
	private void removeTriplesFromGroups(final List<Triple> triples, final HashMap<String, List<Triple>> groups) {
		for (final HashMap.Entry<String, List<Triple>> entry : groups.entrySet()) {
			entry.getValue().removeAll(triples);
		}
		// remove empty groups
		final Iterator<Entry<String, List<Triple>>> it = groups.entrySet().iterator();
		while (it.hasNext()) {
			final HashMap.Entry<String, List<Triple>> pair = it.next();
			if (pair.getValue().size() == 0) {
				it.remove(); // avoids a ConcurrentModificationException
			}
		}
	}

	/**
	 * Groups the input triples by subject.
	 *
	 * @param triples triples to be grouped
	 * @return hashmap of triples grouped by the subject
	 */
	private HashMap<String, List<Triple>> getSubjectGroups(final List<Triple> triples) {
		final HashMap<String, List<Triple>> subjectGroups = new HashMap<>();
		for (final Triple triple : triples) {
			final String subject = triple.getSubject().toString(prefixes);

			if (subjectGroups.containsKey(subject)) {
				subjectGroups.get(subject).add(triple);
			} else { // new entry in the HashMap
				final List<Triple> subjTriples = new ArrayList<>();
				subjTriples.add(triple);
				subjectGroups.put(subject, subjTriples);
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
	 * @return hash map of triples grouped by the subject considering the emergent
	 *         schema
	 */
	private HashMap<String, HashMap<String, List<Triple>>> getEmergentSchemaSubjectGroups(final List<Triple> triples) {
		// key - table names, (subject, triples)
		final HashMap<String, HashMap<String, List<Triple>>> subjectGroups = new HashMap<String, HashMap<String, List<Triple>>>();
		for (final Triple triple : triples) {
			final String subject = triple.getSubject().toString(prefixes);
			// find in which table this triple is stored, based on the predicate
			String subjectTableName = EmergentSchema.getInstance()
					.getTable(Stats.getInstance().findTableName(triple.getPredicate().toString()));
			// if we already have a triple for the table
			if (subjectGroups.containsKey(subjectTableName)) {
				HashMap<String, List<Triple>> subjects = subjectGroups.get(subjectTableName);
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
				HashMap<String, List<Triple>> subjectGroup = new HashMap<>();
				final List<Triple> subjTriples = new ArrayList<>();
				subjTriples.add(triple);
				subjectGroup.put(triple.getSubject().toString(prefixes), subjTriples);
				subjectGroups.put(subjectTableName, subjectGroup);
			}
		}
		return subjectGroups;
	}

	/**
	 * Groups the input triples by object.
	 *
	 * @param triples triples to be grouped
	 * @return hashmap of triples grouped by the object
	 */
	private HashMap<String, List<Triple>> getObjectGroups(final List<Triple> triples) {
		final HashMap<String, List<Triple>> objectGroups = new HashMap<>();
		for (final Triple triple : triples) {
			final String object = triple.getObject().toString(prefixes);

			if (objectGroups.containsKey(object)) {
				objectGroups.get(object).add(triple);
			} else { // new entry in the HashMap
				final List<Triple> objTriples = new ArrayList<>();
				objTriples.add(triple);
				objectGroups.put(object, objTriples);
			}
		}
		return objectGroups;
	}

	private HashMap<String, JoinedTriplesGroup> getJoinedGroups(final List<Triple> triples) {
		final HashMap<String, JoinedTriplesGroup> joinedGroups = new HashMap<>();

		for (final Triple triple : triples) {
			final String subject = triple.getSubject().toString(prefixes);
			final String object = triple.getObject().toString(prefixes);

			// group by subject value
			if (joinedGroups.containsKey(subject)) {
				joinedGroups.get(subject).addWptTriple(triple);
			} else {
				final JoinedTriplesGroup newGroup = new JoinedTriplesGroup();
				newGroup.addWptTriple(triple);
				joinedGroups.put(subject, newGroup);
			}

			// group by object value
			if (joinedGroups.containsKey(object)) {
				joinedGroups.get(object).addIwptTriple(triple);
			} else {
				final JoinedTriplesGroup newGroup = new JoinedTriplesGroup();
				newGroup.addIwptTriple(triple);
				joinedGroups.put(object, newGroup);
			}
		}
		return joinedGroups;
	}

	private String getBiggestGroupKey(final HashMap<String, JoinedTriplesGroup> joinedGroups) {
		int biggestGroupSize = 0;
		String biggestGroupKey = null;

		for (final String key : joinedGroups.keySet()) {
			final JoinedTriplesGroup joinedTriplesGroup = joinedGroups.get(key);
			final int groupSize = joinedTriplesGroup.getIwptGroup().size() + joinedTriplesGroup.getWptGroup().size();
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
	private boolean existsVariableInCommon(final TriplePattern triple_a, final TriplePattern triple_b) {
		if (triple_a.objectType == ElementType.VARIABLE
				&& (triple_a.object.equals(triple_b.subject) || triple_a.object.equals(triple_b.object))) {
			return true;
		}

		if (triple_a.subjectType == ElementType.VARIABLE
				&& (triple_a.subject.equals(triple_b.subject) || triple_a.subject.equals(triple_b.object))) {
			return true;
		}

		return false;
	}

	// TODO remove if we do not use width
	/*
	 * heuristicWidth decides a width based on the proportion between the number of
	 * elements in a table and the unique subjects.
	 */
	private int heuristicWidth(final Node node) {
		if (node instanceof PtNode || node instanceof IptNode || node instanceof JptNode) {
			return 5;
		}
		final String predicate = ((VpNode) node).triplePattern.predicate;
		final int tableSize = Stats.getInstance().getTableSize(predicate);
		final int numberUniqueSubjects = Stats.getInstance().getTableDistinctSubjects(predicate);
		final float proportion = tableSize / numberUniqueSubjects;
		if (proportion > 1) {
			return 3;
		}
		return 2;
	}

	public void setUsePropertyTable(final boolean usePropertyTable) {
		this.usePropertyTable = usePropertyTable;
	}

	public void setUseInversePropertyTable(final boolean useInversePropertyTable) {
		this.useInversePropertyTable = useInversePropertyTable;
	}

	public void setUseJoinedPropertyTable(final boolean useJoinedPropertyTable) {
		this.useJoinedPropertyTable = useJoinedPropertyTable;
		if (this.useJoinedPropertyTable) {
			setUsePropertyTable(false);
			setUseInversePropertyTable(false);
		}
	}

	public void setUseVerticalPartitioning(boolean useVerticalPartitioning) {
		this.useVerticalPartitioning = useVerticalPartitioning;
	}

	public boolean isGroupingDisabled() {
		return groupingDisabled;
	}

	public void setGroupingDisabled(boolean groupingDisabled) {
		this.groupingDisabled = groupingDisabled;
	}
}
