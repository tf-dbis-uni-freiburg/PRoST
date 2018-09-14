package translator;

import java.util.*;

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
import joinTree.JoinTree;
import joinTree.Node;
import joinTree.PtNode;
import joinTree.TriplePattern;
import joinTree.VpNode;

/**
 * This class parses the SPARQL query, build the Tree and save its serialization
 * in a file.
 *
 * @author Matteo Cossu
 */
public class Translator {

	// minimum number of triple patterns with the same subject to form a group
	// (property table)
	final int DEFAULT_MIN_GROUP_SIZE = 2;

	String inputFile;
	int treeWidth;
	int minimumGroupSize = DEFAULT_MIN_GROUP_SIZE;
	PrefixMapping prefixes;

	// if false, only virtual partitioning tables will be queried
	private boolean usePropertyTable;
	private static final Logger logger = Logger.getLogger("PRoST");

	// TODO check this, if you do not specify the treeWidth in the input parameters
	// when
	// you are running the jar, its default value is -1.
	// TODO Move this logic to the translator
	public Translator(String input, int treeWidth) {
		this.inputFile = input;
		this.treeWidth = treeWidth;
	}

	public JoinTree translateQuery() {
		// parse the query and extract prefixes
		Query query = QueryFactory.read("file:" + inputFile);
		this.prefixes = query.getPrefixMapping();

		logger.info("** SPARQL QUERY **\n" + query + "\n****************");

		// extract variables, list of triples and filter
		Op opQuery = Algebra.compile(query);
		QueryVisitor queryVisitor = new QueryVisitor(prefixes);
		OpWalker.walk(opQuery, queryVisitor);

		QueryTree mainTree = queryVisitor.getMainQueryTree();
		List<Var> projectionVariables = queryVisitor.getProjectionVariables();

		// build main tree
		Node rootNode = buildTree(mainTree.getTriples(), projectionVariables);
		rootNode.filter = mainTree.getFilter();
		
		List<Node> optionalTreeRoots = new ArrayList<Node>();
		// order is important TODO use ordered list
		for (int i = 0; i < queryVisitor.getOptionalQueryTrees().size(); i++) {
			QueryTree currentOptionalTree = queryVisitor.getOptionalQueryTrees().get(i);
			// build optional tree
			Node optionalTreeRoot = buildTree(currentOptionalTree.getTriples(), null);
			optionalTreeRoot.filter = currentOptionalTree.getFilter();
			optionalTreeRoots.add(optionalTreeRoot);
		}

		JoinTree tree = new JoinTree(rootNode, optionalTreeRoots, inputFile);

		// if distinct keyword is present
		tree.setDistinct(query.isDistinct());

		logger.info("** Spark JoinTree **\n" + tree + "\n****************");

		return tree;
	}

	/*
	 * buildTree constructs the JoinTree
	 */
	public Node buildTree(List<Triple> triples, List<Var> projectionVars) {
		// sort the triples before adding them
		// this.sortTriples();

		PriorityQueue<Node> nodesQueue = getNodesQueue(triples);

		Node tree = nodesQueue.poll();

		if (projectionVars != null) {
			// set the root node with the variables that need to be projected
			// only for the main tree
			ArrayList<String> projectionList = new ArrayList<String>();
			for (int i = 0; i < projectionVars.size(); i++)
				projectionList.add(projectionVars.get(i).getVarName());
			tree.setProjectionList(projectionList);
		}
		
		// visit the hypergraph to build the tree
		Node currentNode = tree;
		ArrayDeque<Node> visitableNodes = new ArrayDeque<Node>();
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

	private PriorityQueue<Node> getNodesQueue(List<Triple> triples) {
		PriorityQueue<Node> nodesQueue = new PriorityQueue<Node>(triples.size(), new NodeComparator());
		if (usePropertyTable) {
			HashMap<String, List<Triple>> subjectGroups = new HashMap<String, List<Triple>>();

			// group by subjects
			for (Triple triple : triples) {
				String subject = triple.getSubject().toString(prefixes);

				if (subjectGroups.containsKey(subject)) {
					subjectGroups.get(subject).add(triple);
				} else {
					List<Triple> subjTriples = new ArrayList<Triple>();
					subjTriples.add(triple);
					subjectGroups.put(subject, subjTriples);
				}
			}

			// create and add the proper nodes
			for (String subject : subjectGroups.keySet()) {
				if (minimumGroupSize == 0) {
					for (Triple t : subjectGroups.get(subject)) {
						// triple group with a single triple
						List<Triple> singleGroup = new ArrayList<Triple>();
						singleGroup.add(t);
						Node newNode = new PtNode(singleGroup, prefixes);
						nodesQueue.add(newNode);
					}
				} else if (subjectGroups.get(subject).size() >= minimumGroupSize) {
					nodesQueue.add(new PtNode(subjectGroups.get(subject), prefixes));
				} else {
					for (Triple t : subjectGroups.get(subject)) {
						String tableName = Stats.getInstance().findTableName(t.getPredicate().toString());
						Node newNode = new VpNode(new TriplePattern(t, prefixes), tableName);
						nodesQueue.add(newNode);
					}
				}
			}

		} else {
			for (Triple t : triples) {
				String tableName = Stats.getInstance().findTableName(t.getPredicate().toString());
				Node newNode = new VpNode(new TriplePattern(t, prefixes), tableName);
				nodesQueue.add(newNode);
			}
		}
		return nodesQueue;
	}

	/*
	 * findRelateNode, given a source node, finds another node with at least one
	 * variable in common, if there isn't return null
	 */
	private Node findRelateNode(Node sourceNode, PriorityQueue<Node> availableNodes) {

		if (sourceNode.isPropertyTable) {
			// sourceNode is a group
			for (TriplePattern tripleSource : sourceNode.tripleGroup) {
				for (Node node : availableNodes) {
					if (node.isPropertyTable) {
						for (TriplePattern tripleDest : node.tripleGroup)
							if (existsVariableInCommon(tripleSource, tripleDest))
								return node;
					} else {
						if (existsVariableInCommon(tripleSource, node.triplePattern))
							return node;
					}
				}
			}

		} else {
			// source node is not a group
			for (Node node : availableNodes) {
				if (node.isPropertyTable) {
					for (TriplePattern tripleDest : node.tripleGroup) {
						if (existsVariableInCommon(tripleDest, sourceNode.triplePattern))
							return node;
					}
				} else {
					if (existsVariableInCommon(sourceNode.triplePattern, node.triplePattern))
						return node;
				}
			}
		}
		return null;
	}

	/*
	 * check if two Triple Patterns share at least one variable
	 */
	private boolean existsVariableInCommon(TriplePattern triple_a, TriplePattern triple_b) {
		if (triple_a.objectType == ElementType.VARIABLE
				&& (triple_a.object.equals(triple_b.subject) || triple_a.object.equals(triple_b.object)))
			return true;

		if (triple_a.subjectType == ElementType.VARIABLE
				&& (triple_a.subject.equals(triple_b.subject) || triple_a.subject.equals(triple_b.object)))
			return true;

		return false;
	}

	/*
	 * heuristicWidth decides a width based on the proportion between the number of
	 * elements in a table and the unique subjects.
	 */
	private int heuristicWidth(Node node) {
		if (node.isPropertyTable)
			return 5;
		String predicate = node.triplePattern.predicate;
		int tableSize = Stats.getInstance().getTableSize(predicate);
		int numberUniqueSubjects = Stats.getInstance().getTableDistinctSubjects(predicate);
		float proportion = tableSize / numberUniqueSubjects;
		if (proportion > 1)
			return 3;
		return 2;
	}

	public void setPropertyTable(boolean b) {
		this.usePropertyTable = b;
	}

	public void setMinimumGroupSize(int size) {
		this.minimumGroupSize = size;
	}

}
