package translator;

import java.util.*;

import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;

import JoinTree.ElementType;
import JoinTree.ExtVpNode;
import JoinTree.JoinTree;
import JoinTree.Node;
import JoinTree.PtNode;
import JoinTree.TriplePattern;
import JoinTree.VpNode;
import extVp.DatabaseStatistics;
import extVp.ExtVpCreator;
import extVp.TableStatistic;
import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.query.Query;
import com.hp.hpl.jena.query.QueryFactory;
import com.hp.hpl.jena.shared.PrefixMapping;
import com.hp.hpl.jena.sparql.algebra.Algebra;
import com.hp.hpl.jena.sparql.algebra.Op;
import com.hp.hpl.jena.sparql.algebra.OpWalker;
import com.hp.hpl.jena.sparql.core.Var;
import Executor.Utils;

/**
 * This class parses the SPARQL query, build the Tree and save its serialization
 * in a file.
 *
 * @author Matteo Cossu
 */
public class Translator {
	SparkSession spark;
	SQLContext sqlContext;

	// minimum number of triple patterns with the same subject to form a group
	// (property table)
	final static int DEFAULT_MIN_GROUP_SIZE = 2;

	String inputFile;
	int treeWidth;
	int minimumGroupSize = DEFAULT_MIN_GROUP_SIZE;
	PrefixMapping prefixes;

	
	// if false, only virtual partitioning tables will be queried
	private boolean usePropertyTable;
	private boolean useExtVP = false;
	
	private static final Logger logger = Logger.getLogger("PRoST");
	private String databaseName;
	private String extVPDatabaseName;
	private DatabaseStatistics extVPDatabaseStatistic;
	//private Map<String, TableStatistic> extVpStatistics;

	// TODO check this, if you do not specify the treeWidth in the input parameters when you are running the jar, its default value is -1.
	// TODO Move this logic to the translator
	public Translator(String input, int treeWidth, String databaseName, String extVPDatabaseName,  DatabaseStatistics extVPDatabaseStatistic) {
		this.inputFile = input;
		this.treeWidth = treeWidth;
		this.databaseName = databaseName;
		this.extVPDatabaseName = extVPDatabaseName;
		this.extVPDatabaseStatistic = extVPDatabaseStatistic;
		
		// initialize the Spark environment used by extVP tables creation
		spark = SparkSession.builder().appName("PRoST-Translator").enableHiveSupport().getOrCreate();
		sqlContext = spark.sqlContext();
		sqlContext.sql("USE "+ this.databaseName);	
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
		
		if(useExtVP) {
			changeVpNodesToExtVPNodes(tree.getRoot(), null);
		}
		
		logger.info("** Spark JoinTree with ExtVP nodes**\n" + tree + "\n****************");

		return tree;
	}
	
	private void changeVpNodesToExtVPNodes(Node node, TriplePattern parentPattern) {
		
		
		List<Node> children = node.children;
		
		//if (node.isVPNode) {
		//	logger.info(node.triplePattern.toString());
		//}
		
		List<Node> nodesToTemove = new ArrayList<Node>();
		List<Node> nodesToAdd = new ArrayList<Node>();
		
		for (Node child : children) {
			changeVpNodesToExtVPNodes(child, node.triplePattern);
			
			if (child.isVPNode && node.isVPNode) {
				ExtVpCreator extVPcreator = new ExtVpCreator();
				String createdTable = extVPcreator.createExtVPTable(child.triplePattern, node.triplePattern, this.spark, this.extVPDatabaseStatistic, this.extVPDatabaseName, prefixes);
				
				if (createdTable!="") {
					ExtVpNode newNode = new ExtVpNode(child.triplePattern, createdTable, extVPDatabaseName);
					newNode.children = child.children;
					
					nodesToAdd.add(newNode);
					nodesToTemove.add(child);
				}
			}
		}
		
		node.children.removeAll(nodesToTemove);
		node.children.addAll(nodesToAdd);
		
		//TODO create extvp tables if possible, according to tree
		//1. check if there is a join between two VP NODES
		//1.1 check statistics if extvp node is in statistics, and should be recreated or not
		//1.2. create extvp tables for join
		//1.3 if created extvp, must translate query tree again
		
		
		//OLD EXTVP CREATION CALLL
		// ExtVpCreator extVPcreator = new ExtVpCreator();
		// extVPcreator.createExtVPFromTriples(triples, prefixes, spark, extVPDatabaseStatistic, extVPDatabaseName);
		// logger.info("ExtVP: all tables created!");
								
		// logger.info("Database size: " + extVPDatabaseStatistic.getSize());
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
		
		if (useExtVP) {
			List<Triple> remainingTriples = new ArrayList<Triple>();
			remainingTriples.addAll(triples);
			
			//uses property table when group is bigger than minimum group size
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
				
				// create and adds the proper nodes
				for (String subject : subjectGroups.keySet()) {
					if (subjectGroups.get(subject).size() >= minimumGroupSize) {
						List<Triple> groupedTriples = subjectGroups.get(subject);
						nodesQueue.add(new PtNode(groupedTriples, prefixes));
						logger.info("added PTNode with subject " + subject + ", group size: " + subjectGroups.get(subject).size());
						remainingTriples.removeAll(groupedTriples);
					}
				}
			}
			
			// remaining triples uses ExtVP or VP
			for (Triple currentTriple : remainingTriples) {
				String tableName = TableStatistic.selectExtVPTable(currentTriple, triples, extVPDatabaseStatistic.getTables(), prefixes);
				if (tableName != "") {
					//ExtVp
					Node newNode = new ExtVpNode(new TriplePattern(currentTriple, prefixes), tableName, extVPDatabaseName);
					nodesQueue.add(newNode);
					logger.info("added ExtVpNode for triple " + currentTriple.toString());
				} else {
					//VP
					tableName = Stats.getInstance().findTableName(currentTriple.getPredicate().toString());
					Node newNode = new VpNode(new TriplePattern(currentTriple, prefixes), tableName);
					nodesQueue.add(newNode);
					logger.info("added VpNode for triple " + currentTriple.toString());
				}
			}
		}
		else if (usePropertyTable) {
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
	
	public void setUseExtVP(boolean b) {
		this.useExtVP = b;
	}

}
