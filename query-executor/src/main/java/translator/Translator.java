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
import translator.triplesGroup.TriplesGroup;
import translator.triplesGroup.TriplesGroupsMapping;
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
	private PrefixMapping prefixes;

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

		final JoinTree tree = new JoinTree(rootNode, queryPath);

		// set filter
		tree.setFilter(mainTree.getFilter());

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

	/**
	 * Constructs the join tree.
	 */
	private Node buildTree(final List<Triple> triples) {
		final PriorityQueue<Node> nodesQueue = getNodesQueue(triples);
		Node currentNode = null;

		while (!nodesQueue.isEmpty()) {
			currentNode = nodesQueue.poll();
			final Node relatedNode = findRelateNode(currentNode, nodesQueue);
			if (relatedNode != null) {
				final JoinNode joinNode = new JoinNode(currentNode, relatedNode, statistics);
				nodesQueue.add(joinNode);
				nodesQueue.remove(currentNode);
				nodesQueue.remove(relatedNode);
			}
		}
		return currentNode;
	}

	private PriorityQueue<Node> getNodesQueue(final List<Triple> triples) {
		final PriorityQueue<Node> nodesQueue = new PriorityQueue<>(triples.size(), new NodeComparator());
		final List<Triple> unassignedTriples = new ArrayList<>();
		final List<Triple> unassignedTriplesWithVariablePredicate = new ArrayList<>();

		for (final Triple triple : triples) {
			if (triple.getPredicate().isVariable()) {
				unassignedTriplesWithVariablePredicate.add(triple);
			} else {
				unassignedTriples.add(triple);
			}
		}

		if (settings.isUsingWPT() || settings.isUsingIWPT() || settings.isUsingJWPTInner()
				|| settings.isUsingJWPTOuter() || settings.isUsingJWPTLeftouter()) {
			logger.info("Creating grouped nodes...");
			final TriplesGroupsMapping groupsMapping = new TriplesGroupsMapping(unassignedTriples, settings);
			while (groupsMapping.size() > 0) {
				final TriplesGroup largestGroup = groupsMapping.extractBestTriplesGroup(settings);
				if (largestGroup.size() < settings.getMinGroupSize()) {
					break;
				}
				final List<Node> createdNodes = largestGroup.createNodes(settings, statistics, prefixes);
				if (!createdNodes.isEmpty()) {
					nodesQueue.addAll(createdNodes);
					groupsMapping.removeTriples(largestGroup);
					unassignedTriples.removeAll(largestGroup.getTriples());
				}
			}
			logger.info("Done! Triple patterns without nodes: " + (unassignedTriples.size()
					+ unassignedTriplesWithVariablePredicate.size()));
		}

		if (settings.isUsingVP()) {
			// VP only
			logger.info("Creating VP nodes...");
			createVpNodes(unassignedTriples, nodesQueue);
			logger.info("Done! Triple patterns without nodes: " + (unassignedTriples.size()
					+ unassignedTriplesWithVariablePredicate.size()));
		}
		if (settings.isUsingTT()) {
			logger.info("Creating TT nodes...");
			createTTNodes(unassignedTriples, nodesQueue);
			logger.info("Done! Triple patterns without nodes: " + (unassignedTriples.size()
					+ unassignedTriplesWithVariablePredicate.size()));
		}

		//create nodes for patterns with variable predicates
		for (final Triple triple : new ArrayList<>(unassignedTriplesWithVariablePredicate)) {
			final ArrayList<Triple> tripleAsList = new ArrayList<>(); // list is needed as argument to node creation
			// methods
			tripleAsList.add(triple);
			//first try to find best PT node type for the given pattern
			if (settings.isUsingWPT() && triple.getSubject().isConcrete()) {
				nodesQueue.add(new WPTNode(tripleAsList, prefixes, statistics));
				unassignedTriplesWithVariablePredicate.remove(triple);
			} else if (settings.isUsingIWPT() && triple.getObject().isConcrete()) {
				nodesQueue.add(new IWPTNode(tripleAsList, prefixes, statistics));
				unassignedTriplesWithVariablePredicate.remove(triple);
			} else if (settings.isUsingJWPTOuter()
					&& (triple.getSubject().isConcrete() || triple.getObject().isConcrete())) {
				nodesQueue.add(new JWPTNode(triple, prefixes, statistics, true, settings));
				unassignedTriplesWithVariablePredicate.remove(triple);
			} else {
				//no best pt node type, uses general best option
				if (settings.isUsingTT()) {
					createTTNodes(tripleAsList, nodesQueue);
					unassignedTriplesWithVariablePredicate.remove(triple);
				} else if (settings.isUsingVP()) {
					createVpNodes(tripleAsList, nodesQueue);
					unassignedTriplesWithVariablePredicate.remove(triple);
				} else if (settings.isUsingWPT()) {
					nodesQueue.add(new WPTNode(tripleAsList, prefixes, statistics));
					unassignedTriplesWithVariablePredicate.remove(triple);
				} else if (settings.isUsingJWPTOuter()) {
					nodesQueue.add(new JWPTNode(triple, prefixes, statistics, true, settings));
					unassignedTriplesWithVariablePredicate.remove(triple);
				} else if (settings.isUsingIWPT()) {
					nodesQueue.add(new IWPTNode(tripleAsList, prefixes, statistics));
					unassignedTriplesWithVariablePredicate.remove(triple);
				}
			}
		}

		if (unassignedTriples.size() > 0 || unassignedTriplesWithVariablePredicate.size() > 0) {
			throw new RuntimeException("Cannot generate nodes queue. Some triple patterns are not assigned to a node.");
		} else {
			return nodesQueue;
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

	/**
	 * Check if two Triple Patterns share at least one variable.
	 */
	private boolean existsVariableInCommon(final TriplePattern tripleA, final TriplePattern tripleB) {
		return (tripleA.getObjectType() == ElementType.VARIABLE
				&& (tripleA.getObject().equals(tripleB.getSubject())
				|| tripleA.getObject().equals(tripleB.getObject())));
	}
}
