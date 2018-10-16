package translator;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
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
import joinTree.JoinTree;
import joinTree.JptNode;
import joinTree.Node;
import joinTree.PtNode;
import joinTree.TriplePattern;
import joinTree.VpNode;

/**
 * This class parses the SPARQL query, build the Tree and save its serialization in a
 * file.
 *
 * @author Matteo Cossu
 */
public class Translator {
	enum NODE_TYPE{
		VP, WPT, IWPT, JWPT
	}

	private static final Logger logger = Logger.getLogger("PRoST");
	// minimum number of triple patterns with the same subject to form a group
	// (property table)
	private final int DEFAULT_MIN_GROUP_SIZE = 2;
	private final String inputFile;
	private int treeWidth;
	private int minimumGroupSize = DEFAULT_MIN_GROUP_SIZE;
	private PrefixMapping prefixes;

	// if false, only virtual partitioning tables will be queried
	private boolean usePropertyTable = false;
	private boolean useInversePropertyTable = false;
	private boolean useJoinedPropertyTable = false;

	// TODO check this, if you do not specify the treeWidth in the input parameters
	// when
	// you are running the jar, its default value is -1.
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
		final Node rootNode = buildTree(mainTree.getTriples(), projectionVariables);
		rootNode.filter = mainTree.getFilter();

		final List<Node> optionalTreeRoots = new ArrayList<>();
		// order is important TODO use ordered list
		for (int i = 0; i < queryVisitor.getOptionalQueryTrees().size(); i++) {
			final QueryTree currentOptionalTree = queryVisitor.getOptionalQueryTrees().get(i);
			// build optional tree
			final Node optionalTreeRoot = buildTree(currentOptionalTree.getTriples(), null);
			optionalTreeRoot.filter = currentOptionalTree.getFilter();
			optionalTreeRoots.add(optionalTreeRoot);
		}

		final JoinTree tree = new JoinTree(rootNode, optionalTreeRoots, inputFile);

		// if distinct keyword is present
		tree.setDistinct(query.isDistinct());

		logger.info("** Spark JoinTree **\n" + tree + "\n****************");

		return tree;
	}

	/*
	 * buildTree constructs the JoinTree
	 */
	private Node buildTree(final List<Triple> triples, final List<Var> projectionVars) {
		// sort the triples before adding them
		// this.sortTriples();

		final PriorityQueue<Node> nodesQueue = getNodesQueue(triples);

		final Node tree = nodesQueue.poll();

		if (projectionVars != null) {
			// set the root node with the variables that need to be projected
			// only for the main tree
			final ArrayList<String> projectionList = new ArrayList<>();
            for (Var projectionVar : projectionVars) {
                projectionList.add(projectionVar.getVarName());
            }
			tree.setProjectionList(projectionList);
		}

		// visit the hypergraph to build the tree
		Node currentNode = tree;
		final ArrayDeque<Node> visitableNodes = new ArrayDeque<>();
		while (!nodesQueue.isEmpty()) {

			int limitWidth = 0;
			// if a limit not set, a heuristic decides the width
			if (treeWidth == -1) {
				treeWidth = heuristicWidth(currentNode);
			}

			Node newNode = findRelateNode(currentNode, nodesQueue);

			// there are nodes that are impossible to join with the current tree width
			if (newNode == null && visitableNodes.isEmpty()) {
				// set the limit to infinite and execute again
				treeWidth = Integer.MAX_VALUE;
				return buildTree(triples, projectionVars);
			}

			// add every possible children (wide tree) or limit to a custom width
			// stop if a width limit exists and is reached
			while (newNode != null && !(treeWidth > 0 && limitWidth == treeWidth)) {

				// append it to the current node and to the queue
				currentNode.addChildren(newNode);

				// visit again the new child
				visitableNodes.add(newNode);

				// remove consumed node and look for another one
				nodesQueue.remove(newNode);
				newNode = findRelateNode(currentNode, nodesQueue);

				limitWidth++;
			}

			// next Node is one of the children
			if (!visitableNodes.isEmpty() && !nodesQueue.isEmpty()) {
				currentNode = visitableNodes.pop();
			}
		}
		return tree;
	}

	/**
	 * Removes an entry with the given key from <code>joinedGroups</code> if its element size is 0.
	 * @param joinedGroups a mapping of <code>JoinedTriplesGroup</code>
	 * @param key a key from <code>joinedGroups</code>
	 */
	private void removeJoinedTriplesGroupIfEmpty(final HashMap<String, JoinedTriplesGroup> joinedGroups, String key){
		if (joinedGroups.containsKey(key)){
			if (joinedGroups.get(key).size() == 0) {
				joinedGroups.remove(key);
			}
		}
	}

	private PriorityQueue<Node> getNodesQueue(final List<Triple> triples) {
		final PriorityQueue<Node> nodesQueue = new PriorityQueue<>(triples.size(), new NodeComparator());

		if (useJoinedPropertyTable) {
			final HashMap<String, JoinedTriplesGroup> joinedGroups = getJoinedGroups(triples);
			logger.info("JWPT and VP models only");

			while (!joinedGroups.isEmpty()) {
				// get largest group
				final String largestGroupKey = getLargestGroupKey(joinedGroups);
				final JoinedTriplesGroup largestJoinedTriplesGroup = joinedGroups.get(largestGroupKey);

				// remove triples from smaller groups
				for (final Triple triple : largestJoinedTriplesGroup.getWptGroup()) {
					final String object = triple.getObject().toString();
					joinedGroups.get(object).getIwptGroup().remove(triple);
					removeJoinedTriplesGroupIfEmpty(joinedGroups, object);
				}
				for (final Triple triple : largestJoinedTriplesGroup.getIwptGroup()) {
					final String subject = triple.getSubject().toString();
					joinedGroups.get(subject).getWptGroup().remove(triple);
					removeJoinedTriplesGroupIfEmpty(joinedGroups, subject);
				}
				createNodes(largestJoinedTriplesGroup, nodesQueue);
				joinedGroups.remove(largestGroupKey);
			}
		} else if (usePropertyTable && !useInversePropertyTable) {
			logger.info("WPT and VP models only");
			final HashMap<String, List<Triple>> subjectGroups = getSubjectGroups(triples);
			for (final List<Triple> triplesGroup : subjectGroups.values()) {
				createNodes(triplesGroup, nodesQueue, NODE_TYPE.WPT);
			}
		} else if (!usePropertyTable && useInversePropertyTable) {
			logger.info("IWPT and VP only");
			final HashMap<String, List<Triple>> objectGroups = getObjectGroups(triples);
			for (final List<Triple> triplesGroup : objectGroups.values()) {
				createNodes(triplesGroup, nodesQueue, NODE_TYPE.IWPT);
			}
		} else if (usePropertyTable && useInversePropertyTable) {
			logger.info("WPT, IWPT, and VP models only");

			final HashMap<String, List<Triple>> objectGroups = getObjectGroups(triples);
			final HashMap<String, List<Triple>> subjectGroups = getSubjectGroups(triples);

			// repeats until there are no unassigned triple patterns left
			while (objectGroups.size() != 0 && subjectGroups.size() != 0) {
				// Calculate largest group by object
				String largestObjectGroupKey = getLargestGroupKey(objectGroups);
				int largestObjectGroupSize = objectGroups.get(largestObjectGroupKey).size();
				// calculate biggest group by subject
				String largestSubjectGroupKey = getLargestGroupKey(subjectGroups);
				int largestSubjectGroupSize = subjectGroups.get(largestSubjectGroupKey).size();

				// create nodes
				if (largestObjectGroupSize > largestSubjectGroupSize) {
					// create and add the iwpt or vp nodes
					List<Triple> largestObjectGroupTriples = objectGroups.get(largestObjectGroupKey);
					createNodes(largestObjectGroupTriples, nodesQueue, NODE_TYPE.IWPT);

					// remove triples from subject group
					for (Triple triple : largestObjectGroupTriples){
						String key = triple.getObject().toString();
						if (subjectGroups.containsKey(key)){
							subjectGroups.get(key).remove(triple);
							if (subjectGroups.get(key).size()==0){
								subjectGroups.remove(key);
							}
						}
					}
					objectGroups.remove(largestObjectGroupKey);
				} else {
					/// create and add the wpt or vp nodes
					List<Triple> largestSubjectGroupTriples = subjectGroups.get(largestSubjectGroupKey);
					createNodes(largestSubjectGroupTriples, nodesQueue, NODE_TYPE.WPT);
					// remove triples from object group
					for (Triple triple : largestSubjectGroupTriples){
						String key = triple.getSubject().toString();
						if (objectGroups.containsKey(key)){
							objectGroups.get(key).remove(triple);
							if (objectGroups.get(key).size()==0){
								objectGroups.remove(key);
							}
						}
					}
					subjectGroups.remove(largestSubjectGroupKey);
				}
			}
		} else {
			// VP only
			logger.info("VP model only");
			createVpNodes(triples, nodesQueue);
		}
		return nodesQueue;
	}

	/**
	 * Creates nodes for the given triples and add them to the <code>nodesQueue</code>.
	 *
	 * @param triples Triples for which nodes are to be created
	 * @param nodesQueue <Code>PriorityQueue</code> to add created nodes to.
	 * @param nodeType Type of the node to be created. Defaults to a VP nodes if not possible create the given node type.
	 */
	private void createNodes(final List<Triple> triples, final PriorityQueue<Node> nodesQueue, final NODE_TYPE nodeType){
		if (triples.size()>=minimumGroupSize){
			switch (nodeType){
				case WPT:
					nodesQueue.add(new PtNode(triples, prefixes));
					break;
				case IWPT:
					nodesQueue.add(new IptNode(triples, prefixes));
					break;
				case JWPT:
					throw new RuntimeException("Tried to create a JWPT, but no JoinedTriplesGroup was given");
				default:
					createVpNodes(triples,nodesQueue);
			}
		}
		else {
			createVpNodes(triples, nodesQueue);
		}
	}

	/**
	 * Creates JWPT nodes for the given <code>JoinedTriplesGroup</code> and add them to the <code>nodesQueue</code>. If not possible to
	 * create a JWPT node, creates VP nodes instead.
	 *
	 * @param joinedGroup <code>JoinedTriplesGroup</code> for which nodes are to be created
	 * @param nodesQueue <Code>PriorityQueue</code> to add created nodes to.
	 */
	private void createNodes(final JoinedTriplesGroup joinedGroup, final PriorityQueue<Node> nodesQueue){
		if (joinedGroup.size()>=minimumGroupSize){
			nodesQueue.add(new JptNode(joinedGroup, prefixes));
		} else {
			createVpNodes(new ArrayList<>(joinedGroup.getWptGroup()), nodesQueue);
			// avoids repeating vp nodes for patterns with same subject and object, i.e ?v fof ?v.
			joinedGroup.getIwptGroup().removeAll(joinedGroup.getWptGroup());
			createVpNodes(new ArrayList<>(joinedGroup.getIwptGroup()), nodesQueue);
		}
	}

	/**
	 * Creates VP nodes from a list of triples.
	 *
	 * @param triples
	 *            Triples for which VP nodes will be created
	 * @param nodesQueue
	 *            PriorityQueue where created nodes are added to
	 */
	private void createVpNodes(final List<Triple> triples, final PriorityQueue<Node> nodesQueue) {
		for (final Triple t : triples) {
			final String tableName = Stats.getInstance().findTableName(t.getPredicate().toString());
			final Node newNode = new VpNode(new TriplePattern(t, prefixes), tableName);
			nodesQueue.add(newNode);
		}
	}

	/**
	 * Groups the input triples by subject.
	 *
	 * @param triples
	 *            triples to be grouped
	 * @return HashMap of triples grouped by the subject
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
	 * Groups the input triples by object.
	 *
	 * @param triples
	 *            triples to be grouped
	 * @return HashMap of triples grouped by the object
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
		}
		return joinedGroups;
	}

	/**
	 * Given a Map with groups, return the key of the largest group.
	 *
	 * @param groupsMapping Map with the groups whose size are to be checked.
	 * @param <T> Type of the groups. <code>JoinedTriplesGroup</code> or a list of <code>Triple</code>
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
			} else{
				groupSize = ((List<Triple>)group).size();
			}
			if (groupSize >= biggestGroupSize) {
				biggestGroupKey = key;
				biggestGroupSize = groupSize;
			}
		}
		return biggestGroupKey;
	}

	/*
	 * findRelateNode, given a source node, finds another node with at least one variable in
	 * common, if there isn't return null.
	 */
	private Node findRelateNode(final Node sourceNode, final PriorityQueue<Node> availableNodes) {

		if (sourceNode instanceof PtNode || sourceNode instanceof IptNode || sourceNode instanceof JptNode) {
			// sourceNode is a group
			for (final TriplePattern tripleSource : sourceNode.tripleGroup) {
				for (final Node node : availableNodes) {
					if (node instanceof PtNode || node instanceof IptNode || node instanceof JptNode) {
						for (final TriplePattern tripleDest : node.tripleGroup) {
							if (existsVariableInCommon(tripleSource, tripleDest)) {
								return node;
							}
						}
					} else {
						if (existsVariableInCommon(tripleSource, node.triplePattern)) {
							return node;
						}
					}
				}
			}
		} else {
			// source node is not a group
			for (final Node node : availableNodes) {
				if (node instanceof PtNode || node instanceof IptNode || node instanceof JptNode) {
					for (final TriplePattern tripleDest : node.tripleGroup) {
						if (existsVariableInCommon(tripleDest, sourceNode.triplePattern)) {
							return node;
						}
					}
				} else {
					if (existsVariableInCommon(sourceNode.triplePattern, node.triplePattern)) {
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

	/*
	 * heuristicWidth decides a width based on the proportion between the number of elements
	 * in a table and the unique subjects.
	 */
	private int heuristicWidth(final Node node) {
		if (node instanceof PtNode || node instanceof IptNode || node instanceof JptNode) {
			return 5;
		}
		final String predicate = node.triplePattern.predicate;
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

	public void setMinimumGroupSize(final int size) {
		minimumGroupSize = size;
	}

	public void setUseJoinedPropertyTable(final boolean useJoinedPropertyTable) {
		this.useJoinedPropertyTable = useJoinedPropertyTable;
		if (this.useJoinedPropertyTable) {
			setUsePropertyTable(false);
			setUseInversePropertyTable(false);
		}
	}
}
