package translator;

import java.util.*;

import javafx.geometry.VPos;
import joinTree.*;
import org.apache.log4j.Logger;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;

import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.query.Query;
import com.hp.hpl.jena.query.QueryFactory;
import com.hp.hpl.jena.shared.PrefixMapping;
import com.hp.hpl.jena.sparql.algebra.Algebra;
import com.hp.hpl.jena.sparql.algebra.Op;
import com.hp.hpl.jena.sparql.algebra.OpWalker;
import com.hp.hpl.jena.sparql.core.Var;

import extVp.DatabaseStatistics;
import extVp.ExtVpCreator;
import extVp.TableStatistic;

/**
 * This class parses the SPARQL query, build the Tree and save its serialization in a
 * file.
 *
 * @author Matteo Cossu
 */
public class Translator {
	private static final Logger logger = Logger.getLogger("PRoST");

	enum NODE_TYPE{
		VP, WPT, IWPT, JWPT, EXTVP
	}
	// minimum number of triple patterns with the same subject to form a group
	// (property table)
	static final int DEFAULT_MIN_GROUP_SIZE = 2;

	private final SparkSession spark;
	private final SQLContext sqlContext;
	private final String inputFile;
	private boolean isGrouping = true;
	private int treeWidth;
	private int minimumGroupSize = DEFAULT_MIN_GROUP_SIZE;
	private PrefixMapping prefixes;

	// if false, only virtual partitioning tables will be queried
	private boolean usePropertyTable;
	private boolean useInversePropertyTable = false;
	private boolean useJoinedPropertyTable = false;
	private boolean useExtVP = false;
	private boolean useVerticalPartitioning = false;
	private boolean useVpToExtVp = false;
	private boolean partitionExtVP = false;
	private boolean createJoinVpNodes = false;

	private final String databaseName;
	private final String extVPDatabaseName;
	private final DatabaseStatistics extVPDatabaseStatistic;
	// private Map<String, TableStatistic> extVpStatistics;

	// TODO check this, if you do not specify the treeWidth in the input parameters when you
	// are running the jar, its
	// default value is -1.
	// TODO Move this logic to the translator
	public Translator(final String input, final int treeWidth, final String databaseName,
			final String extVPDatabaseName, final DatabaseStatistics extVPDatabaseStatistic) {
		inputFile = input;
		this.treeWidth = treeWidth;
		this.databaseName = databaseName;
		this.extVPDatabaseName = extVPDatabaseName;
		this.extVPDatabaseStatistic = extVPDatabaseStatistic;

		// initialize the Spark environment used by extVP tables creation
		spark = SparkSession.builder().appName("PRoST-Translator").enableHiveSupport().getOrCreate();
		sqlContext = spark.sqlContext();
		sqlContext.sql("USE " + this.databaseName);
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

		// create new extVP nodes if possible. Compares a child node with its parent to create the
		// ExtVP node.
		// This creates EXTVP nodes after tree is created

		// TODO is creating extVP nodes before, but might want to use this at some point again
		if (useVpToExtVp) {
			changeVpNodesToExtVPNodes(tree.getRoot(), null);
			logger.info("** Spark JoinTree with ExtVP nodes**\n" + tree + "\n****************");
		}

		return tree;
	}

	private void changeVpNodesToExtVPNodes(final Node node, final TriplePattern parentPattern) {
		final List<Node> children = node.children;

		// if (node.isVPNode) {
		// logger.info(node.triplePattern.toString());
		// }

		final List<Node> nodesToTemove = new ArrayList<>();
		final List<Node> nodesToAdd = new ArrayList<>();

		for (final Node child : children) {
			changeVpNodesToExtVPNodes(child, node.triplePattern);

			// TODO if current node is an EXTVP node, it should still create EXTVP nodes
			if (child instanceof VpNode && node instanceof VpNode) {
				final ExtVpCreator extVPcreator = new ExtVpCreator();
				final String createdTable = extVPcreator.createExtVPTable(child.triplePattern, node.triplePattern,
						spark, extVPDatabaseStatistic, extVPDatabaseName, prefixes, partitionExtVP);

				if (createdTable != "") {
					final ExtVpNode newNode = new ExtVpNode(child.triplePattern, createdTable, extVPDatabaseName);
					newNode.children = child.children;

					nodesToAdd.add(newNode);
					nodesToTemove.add(child);
				}
			}
		}
		// TODO create extVP node for the parent(current node) if possible

		node.children.removeAll(nodesToTemove);
		node.children.addAll(nodesToAdd);

		// OLD EXTVP CREATION CALLL => created all possible combinations of extvp tables
		// ExtVpCreator extVPcreator = new ExtVpCreator();
		// extVPcreator.createExtVPFromTriples(triples, prefixes, spark, extVPDatabaseStatistic,
		// extVPDatabaseName);
		// logger.info("ExtVP: all tables created!");
		// logger.info("Database size: " + extVPDatabaseStatistic.getSize());
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
		List<Triple> unassignedTriples = new ArrayList<>(triples);

		//TODO only creates one, if none available, when looking for a table at the later stage
		//Creating all possible extvp tables
		if (useExtVP){
			ExtVpCreator extVPcreator = new ExtVpCreator();

			// TODO add option to force creation of EXTVP tables
			//extVPcreator.createExtVPFromTriples(triples, prefixes, spark, extVPDatabaseStatistic, extVPDatabaseName, partitionExtVP);
			logger.info("ExtVP: all tables created!");

			logger.info("Database size: " + extVPDatabaseStatistic.getSize());
		}

		if (useJoinedPropertyTable) {
			final HashMap<String, JoinedTriplesGroup> joinedGroups = getJoinedGroups(triples);
			logger.info("JWPT model");
			while (!joinedGroups.isEmpty()) {
				if (isGrouping) {
					// get largest group
					final String largestGroupKey = getLargestGroupKey(joinedGroups);
					final JoinedTriplesGroup largestJoinedTriplesGroup = joinedGroups.get(largestGroupKey);

					createNodes(largestJoinedTriplesGroup, nodesQueue, unassignedTriples);

					// remove triples from smaller groups
					for (final Triple triple : largestJoinedTriplesGroup.getWptGroup()) {
						final String object = triple.getObject().toString();
						if (joinedGroups.get(object)!= null){
							joinedGroups.get(object).getIwptGroup().remove(triple);
							removeJoinedTriplesGroupIfEmpty(joinedGroups, object);
						}
					}
					for (final Triple triple : largestJoinedTriplesGroup.getIwptGroup()) {
						final String subject = triple.getSubject().toString();
						if (joinedGroups.get(subject)!= null) {
							joinedGroups.get(subject).getWptGroup().remove(triple);
							removeJoinedTriplesGroupIfEmpty(joinedGroups, subject);
						}
					}

					joinedGroups.remove(largestGroupKey);
				} else {//if grouping is disabled, there will be no repeated triples. Works as a normal WPT.
					for (String key : joinedGroups.keySet()){
						createNodes(joinedGroups.get(key), nodesQueue, unassignedTriples);
						// joinedGroups.remove(key);
					}
					joinedGroups.clear(); //avoid concurrent modifications
				}
			}
		} else if (usePropertyTable && useInversePropertyTable && isGrouping) {
			logger.info("WPT and IWPT models");

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
					createNodes(largestObjectGroupTriples, nodesQueue, NODE_TYPE.IWPT, unassignedTriples);

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
					createNodes(largestSubjectGroupTriples, nodesQueue, NODE_TYPE.WPT, unassignedTriples);
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
		} else if (usePropertyTable && !useInversePropertyTable) {
			logger.info("WPT model");
			final HashMap<String, List<Triple>> subjectGroups = getSubjectGroups(triples);
			for (final List<Triple> triplesGroup : subjectGroups.values()) {
				createNodes(triplesGroup, nodesQueue, NODE_TYPE.WPT, unassignedTriples);
			}
		} else if (useInversePropertyTable && !usePropertyTable) {
			logger.info("IWPT model");
			final HashMap<String, List<Triple>> objectGroups = getObjectGroups(triples);
			for (final List<Triple> triplesGroup : objectGroups.values()) {
				createNodes(triplesGroup, nodesQueue, NODE_TYPE.IWPT, unassignedTriples);
			}
		}
		if (useExtVP){
			logger.info("ExtVP model");
			List<Triple> triplesToRemove = new ArrayList<Triple>();
			for (Triple currentTriple : unassignedTriples) {
				String tableName = TableStatistic.selectExtVPTable(currentTriple, triples, extVPDatabaseStatistic.getTables(), prefixes);
				if (!tableName.equals("")){
					Node newNode = new ExtVpNode(new TriplePattern(currentTriple, prefixes), tableName, extVPDatabaseName);
					nodesQueue.add(newNode);
					logger.info("added ExtVpNode for triple " + currentTriple.toString());
					triplesToRemove.add(currentTriple);
				} else {
					logger.info("no suitable ExtVP table found for triple " + currentTriple.toString());
				}
			}
			unassignedTriples.removeAll(triplesToRemove);
		}

		if (useVerticalPartitioning){
			// VP only
			logger.info("VP model");
			createVpNodes(unassignedTriples, nodesQueue);
			unassignedTriples.clear();
		}
		if (unassignedTriples.size()>0){
			throw new RuntimeException("Triples without nodes");
		}


		// create VPJoinNodes from VP Nodes
		if (createJoinVpNodes){
			logger.info("Converting VP nodes into JoinVPNodes");
			final PriorityQueue<VpNode> vpNodesList = new PriorityQueue<>(new NodeComparator());
			for (Node node:nodesQueue){
				if (node instanceof VpNode){
					vpNodesList.add((VpNode) node);
				}
			}

			while (!vpNodesList.isEmpty()){
				logger.info("Remaining VP to check: " + vpNodesList.size());
				VpNode bestVpNode = vpNodesList.poll();
				VpNode vpNodeToJoin = null;

				Iterator<VpNode> vpNodesListIterator = vpNodesList.iterator();
				while (vpNodeToJoin == null && vpNodesListIterator.hasNext()){
					VpNode element = vpNodesListIterator.next();
					if ((element.triplePattern.subject.equals(bestVpNode.triplePattern.subject)) || (element.triplePattern.subject.equals(bestVpNode.triplePattern.object))||(element.triplePattern.object.equals(bestVpNode.triplePattern.subject)) || (element.triplePattern.object.equals(bestVpNode.triplePattern.object))){
						vpNodeToJoin = element;
					}
				}
				if (vpNodeToJoin!=null){
					logger.info("Match found. Removing VP nodes and creating JoinVP node");
					vpNodesList.remove(vpNodeToJoin);
					nodesQueue.remove((vpNodeToJoin));
					nodesQueue.remove(bestVpNode);
					nodesQueue.add(new VpJoinNode(bestVpNode.triplePattern, vpNodeToJoin.triplePattern, bestVpNode.getTableName(),
							vpNodeToJoin.getTableName(), spark, extVPDatabaseStatistic, extVPDatabaseName, prefixes));
				} else {
					logger.info("No match found. Keeping VP node");
				}
			}
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
	private void createNodes(final List<Triple> triples, final PriorityQueue<Node> nodesQueue, final NODE_TYPE nodeType,
							 List<Triple> unassignedTriples){
		if (triples.size()>=minimumGroupSize){
			switch (nodeType){
				case WPT:
					nodesQueue.add(new PtNode(triples, prefixes));
					unassignedTriples.removeAll(triples);
					break;
				case IWPT:
					nodesQueue.add(new IptNode(triples, prefixes));
					unassignedTriples.removeAll(triples);
					break;
				case JWPT:
					throw new RuntimeException("Tried to create a JWPT, but no JoinedTriplesGroup was given");
				default:
					throw new RuntimeException("Cannot create node. No valid partitioning enabled");
			}
		} else {
			logger.info("Group too small. Node not created");
		}

	}

	/**
	 * Creates JWPT nodes for the given <code>JoinedTriplesGroup</code> and add them to the <code>nodesQueue</code>. If not possible to
	 * create a JWPT node, creates VP nodes instead.
	 *
	 * @param joinedGroup <code>JoinedTriplesGroup</code> for which nodes are to be created
	 * @param nodesQueue <Code>PriorityQueue</code> to add created nodes to.
	 */
	private void createNodes(final JoinedTriplesGroup joinedGroup, final PriorityQueue<Node> nodesQueue, List<Triple> unassignedTriples){
		if (joinedGroup.size()>=minimumGroupSize){
			nodesQueue.add(new JptNode(joinedGroup, prefixes));
			unassignedTriples.removeAll(joinedGroup.getWptGroup());
			unassignedTriples.removeAll(joinedGroup.getIwptGroup());
		} else {
			logger.info("Group too small. Node not created");
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
			final String tableName = Stats.getInstance().findCorrectTableName(t.getPredicate().toString(), prefixes);
			final Node newNode = new VpNode(new TriplePattern(t, prefixes), tableName, prefixes);
			nodesQueue.add(newNode);
		}
	}

	/**
	 * Groups the input triples by subject. If grouping is disabled, create one list with one element for each triple.
	 *
	 * @param triples
	 *            triples to be grouped
	 * @return HashMap of triples grouped by the subject
	 */
	private HashMap<String, List<Triple>> getSubjectGroups(final List<Triple> triples) {
		final HashMap<String, List<Triple>> subjectGroups = new HashMap<>();
		int key = 0; //creates a unique key to be used when grouping is disabled, to avoid overwriting values
		for (final Triple triple : triples) {
			if (isGrouping) {
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
		int key = 0; //creates a unique key to be used when grouping is disabled, to avoid overwriting values
		for (final Triple triple : triples) {
			if (isGrouping) {
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
		return objectGroups;
	}

	private HashMap<String, JoinedTriplesGroup> getJoinedGroups(final List<Triple> triples) {
		final HashMap<String, JoinedTriplesGroup> joinedGroups = new HashMap<>();
		int key = 0; //creates a unique key to be used when grouping is disabled, to avoid overwriting values
		for (final Triple triple : triples) {
			if (isGrouping){
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
	 * common, if there isn't return null
	 */
	private Node findRelateNode(final Node sourceNode, final PriorityQueue<Node> availableNodes) {
		if (sourceNode instanceof PtNode || sourceNode instanceof IptNode || sourceNode instanceof JptNode || sourceNode instanceof  VpJoinNode) {
			// sourceNode is a group
			for (final TriplePattern tripleSource : sourceNode.tripleGroup) {
				for (final Node node : availableNodes) {
					if (node instanceof PtNode || node instanceof IptNode || node instanceof JptNode || node instanceof VpJoinNode ) {
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
				if (node instanceof PtNode || node instanceof IptNode || node instanceof JptNode || node instanceof VpJoinNode) {
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

		if (node instanceof  VpJoinNode){
			final String predicate = node.tripleGroup.get(0).predicate;
			final int tableSize = Stats.getInstance().getTableSize(predicate);
			final int numberUniqueSubjects = Stats.getInstance().getTableDistinctSubjects(predicate);

			final String predicate2 = node.tripleGroup.get(1).predicate;
			final int tableSize2 = Stats.getInstance().getTableSize(predicate2);
			final int numberUniqueSubjects2 = Stats.getInstance().getTableDistinctSubjects(predicate2);

			final float proportion = (tableSize+tableSize2) / (numberUniqueSubjects + numberUniqueSubjects2);
			if (proportion > 1) {
				return 3;
			}
			return 2;
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

	public void setUseExtVP(final boolean b){
		useExtVP = b;
	}

	public void setUseVpToExtVp(final boolean b){
		useVpToExtVp = b;
	}

	public void setUseJoinedPropertyTable(final boolean useJoinedPropertyTable) {
		this.useJoinedPropertyTable = useJoinedPropertyTable;
		if (this.useJoinedPropertyTable) {
			setUsePropertyTable(false);
			setUseInversePropertyTable(false);
		}
	}

	public void setIsGrouping(boolean isGrouping){
		this.isGrouping = isGrouping;
	}

	public void setUseVerticalPartitioning(boolean useVerticalPartitioning){
		this.useVerticalPartitioning = useVerticalPartitioning;
	}

	public void setPartitionExtVP(boolean isPartitioned){
		this.partitionExtVP = isPartitioned;
	}

	public void setCreateJoinVpNodes(boolean createJoinVpNodes){
		this.createJoinVpNodes = createJoinVpNodes;
	}
}
