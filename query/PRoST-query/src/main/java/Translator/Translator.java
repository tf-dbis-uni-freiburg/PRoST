package Translator;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map.Entry;
import java.util.PriorityQueue;
import java.util.Iterator;

import org.apache.log4j.Logger;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;

import JoinTree.ElementType;
import JoinTree.JoinTree;
import JoinTree.Node;
import JoinTree.PtNode;
import JoinTree.RPtNode;
import JoinTree.TriplePattern;
import JoinTree.VpNode;

import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.query.Query;
import com.hp.hpl.jena.query.QueryFactory;
import com.hp.hpl.jena.shared.PrefixMapping;
import com.hp.hpl.jena.sparql.algebra.Algebra;
import com.hp.hpl.jena.sparql.algebra.Op;
import com.hp.hpl.jena.sparql.algebra.OpWalker;
import com.hp.hpl.jena.sparql.core.Var;
/**
 * This class parses the SPARQL query,
 * build the Tree and save its serialization in a file.
 *
 * @author Matteo Cossu
 */
public class Translator {
    String inputFile;
    String statsFile;
    Stats stats;
    boolean statsActive = false;
    int treeWidth;
    int minimumGroupSize = 2;
    PrefixMapping prefixes;
    List<Var> variables;
    List<Triple> triples;
	private boolean usePropertyTable = false;
	private boolean useReversePropertyTable = false;
    private static final Logger logger = Logger.getLogger(run.Main.class);
	SparkSession spark;
	SQLContext sqlContext;
    
    public Translator(String input, String statsPath, int treeWidth, String databaseName) {
    	this.inputFile = input;
    	this.statsFile = statsPath;
    	if(statsFile.length() > 0){
    		stats = new Stats(statsFile);
    		statsActive = true;
    	}
    	this.treeWidth = treeWidth;
    	
		// initialize the Spark environment 
		spark = SparkSession
				  .builder()
				  .appName("PRoST-Translator")
				  .getOrCreate();
		sqlContext = spark.sqlContext();
		
		// use the selected database
		sqlContext.sql("USE "+ databaseName);
		logger.info("USE "+ databaseName);
    }
    
    public JoinTree translateQuery() {
    	// parse the query and extract prefixes
        Query query = QueryFactory.read("file:"+inputFile);
        prefixes = query.getPrefixMapping();
        
        logger.info("** SPARQL QUERY **\n" + query +"\n****************"  );
        
        
        // extract variables, list of triples and filter
        Op opQuery = Algebra.compile(query);
        QueryVisitor queryVisitor = new QueryVisitor();
        OpWalker.walk(opQuery, queryVisitor);
        triples = queryVisitor.getTriple_patterns();
        variables  = queryVisitor.getVariables();
           
        // build the tree
        Node root_node = buildTree();
        JoinTree tree = new JoinTree(root_node, inputFile);
        
        // TODO: set the filter when is ready
        //tree.setFilter(queryVisitor.getFilter());
        
        // if distinct keyword is present
        tree.setDistinct(query.isDistinct());
        
        logger.info("** Spark JoinTree **\n" + tree +"\n****************" );
        
        return tree;
    }
    
    /*
     * buildTree constructs the JoinTree, ready to be serialized.
     */
    public Node buildTree() {
    	// sort the triples before adding them
    	//this.sortTriples();    	
    	
    	PriorityQueue<Node> nodesQueue = getNodesQueue();
    	
    	Node tree = nodesQueue.poll();
    	
    	// set the root node with the variables that need to be projected
    	ArrayList<String> projectionList = new ArrayList<String>();
    	for(int i = 0; i < variables.size(); i++)
    		projectionList.add(variables.get(i).getVarName());
    	tree.setProjectionList(projectionList);
    	
    	// visit the hypergraph to build the tree
    	Node currentNode = tree;
    	ArrayDeque<Node> visitableNodes = new ArrayDeque<Node>();
    	while(!nodesQueue.isEmpty()){
    		
    		int limitWidth = 0;
    		// if a limit not set, a heuristic decides the width 
    		if(treeWidth == -1){
    	    	treeWidth = heuristicWidth(currentNode); 
    		}
    		
    		Node newNode =  findRelateNode(currentNode, nodesQueue);
    		
    		// there are nodes that are impossible to join with the current tree width
    		if (newNode == null && visitableNodes.isEmpty()) {
    			// set the limit to infinite and execute again
    			treeWidth = Integer.MAX_VALUE;
    			return buildTree();
    		}
    		
    		// add every possible children (wide tree) or limit to a custom width
    		// stop if a width limit exists and is reached
    		while(newNode != null && !(treeWidth > 0 && limitWidth == treeWidth)){
    			
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
    		if(!visitableNodes.isEmpty() && !nodesQueue.isEmpty()){
    			currentNode = visitableNodes.pop();
    		}
    	}
    	return tree;
    }
        
    private PriorityQueue<Node> getNodesQueue() {
    	PriorityQueue<Node> nodesQueue = new PriorityQueue<Node>
    		(triples.size(), new NodeComparator(this.stats));
      	
    	if(usePropertyTable && !useReversePropertyTable){
    		//RPT disabled
			HashMap<String, List<Triple>> subjectGroups = getSubjectGroups(triples);
			
			logger.info("PT and VP models only");
			
			// create and add the proper nodes
			for(String subject : subjectGroups.keySet()){
				createPtVPNode(subjectGroups.get(subject), nodesQueue);
			}	
    	} else if(!usePropertyTable && useReversePropertyTable){
    		//PT disabled
    		HashMap<String, List<Triple>> objectGroups = getObjectGroups(triples);
			
    		logger.info("RPT and VP only");
			// create and add the proper nodes
			for(String object : objectGroups.keySet()){
				createRPtVPNode(objectGroups.get(object), nodesQueue);
			}
    	} else if(usePropertyTable && useReversePropertyTable){
    		//RPT, PT, and VP enabled
    		HashMap<String, List<Triple>> objectGroups = getObjectGroups(triples);
    		HashMap<String, List<Triple>> subjectGroups = getSubjectGroups(triples);
    		logger.info("Mixed strategy");
    		
    		while (objectGroups.size()!=0 && subjectGroups.size()!=0) { //repeats until there are no unassigned triple patterns left
    			//Calculate biggest group by object
	    		String biggestObjectGroupIndex="";
	    		int biggestObjectGroupSize = 0;
	    		List<Triple> biggestObjectGroupTriples = new ArrayList<Triple>();
	    		for (HashMap.Entry<String, List<Triple>> entry : objectGroups.entrySet()) {
	    		    int size = entry.getValue().size();
	    		    if (size>biggestObjectGroupSize) {
	    		    	biggestObjectGroupIndex = entry.getKey();
	    		    	biggestObjectGroupSize = size;
	    		    	biggestObjectGroupTriples = entry.getValue();
	    		    }
	    		}
	    		
	    		//calculate biggest group by subject
	    		String biggestSubjectGroupIndex="";
	    		int biggestSubjectGroupSize = 0;
	    		List<Triple> biggestSubjectGroupTriples = new ArrayList<Triple>();
	    		for (HashMap.Entry<String, List<Triple>> entry : subjectGroups.entrySet()) {
	    		    int size = entry.getValue().size();
	    		    if (size>biggestSubjectGroupSize) {
	    		    	biggestSubjectGroupIndex = entry.getKey();
	    		    	biggestSubjectGroupSize = size;
	    		    	biggestSubjectGroupTriples = entry.getValue();
	    		    }
	    		}
	    		
	    		//create nodes
	    		if (biggestObjectGroupSize>biggestSubjectGroupSize) {
	    			// create and add the rpt or vp node
					if (biggestObjectGroupSize >= minimumGroupSize){
						nodesQueue.add(new RPtNode(biggestObjectGroupTriples, prefixes, this.stats));
					} else {
						for (Triple t : biggestObjectGroupTriples){
						    String tableName = this.stats.findTableName(t.getPredicate().toString());
							Node newNode = new VpNode(new TriplePattern(t, prefixes, this.stats.arePrefixesActive()), tableName);
							nodesQueue.add(newNode);
						}
					}
				    removeTriplesFromGroups(biggestObjectGroupTriples, subjectGroups); //remove empty groups
				    objectGroups.remove(biggestObjectGroupIndex); //remove group of created node
	    		} else {
	    			/// create and add the pt or vp node
					if (biggestSubjectGroupSize >= minimumGroupSize){
						nodesQueue.add(new PtNode(biggestSubjectGroupTriples, prefixes, this.stats));
					} else {
						for (Triple t : biggestSubjectGroupTriples){
						    String tableName = this.stats.findTableName(t.getPredicate().toString());
							Node newNode = new VpNode(new TriplePattern(t, prefixes, this.stats.arePrefixesActive()), tableName);
							nodesQueue.add(newNode);
						}
					}
					removeTriplesFromGroups(biggestSubjectGroupTriples, objectGroups); //remove empty groups
				    subjectGroups.remove(biggestSubjectGroupIndex); //remove group of created node
	    		}
    		}
		} else {
			//VP only
			logger.info("VP only");
			for(Triple t : triples){
			    String tableName = this.stats.findTableName(t.getPredicate().toString());
				Node newNode = new VpNode(new TriplePattern(t, prefixes, this.stats.arePrefixesActive()), tableName);
				nodesQueue.add(newNode);
			}
		}
    	return nodesQueue;
	}
    
    /** Receives a list of triples, create a PT node or VP nodes, according to the minimum group size, and add it 
     * to the nodesQueue
     * @param triples
     * @param nodesQueue
     */
    private void createPtVPNode(List<Triple> triples, PriorityQueue<Node> nodesQueue) {
    	if (triples.size() >= minimumGroupSize){
			nodesQueue.add(new PtNode(triples, prefixes, this.stats));
		} else {
			for (Triple t : triples){
			    String tableName = this.stats.findTableName(t.getPredicate().toString());
				Node newNode = new VpNode(new TriplePattern(t, prefixes, this.stats.arePrefixesActive()), tableName);
				nodesQueue.add(newNode);
			}
		}
    }

    /** Receives a list of triples, create a RPT node or VP nodes, according to the minimum group size, and add it 
     * to the nodesQueue
     * @param triples
     * @param nodesQueue
     */
    private void createRPtVPNode(List<Triple> triples, PriorityQueue<Node> nodesQueue) {
    	if (triples.size() >= minimumGroupSize){
			nodesQueue.add(new RPtNode(triples, prefixes, this.stats));
		} else {
			for (Triple t : triples){
			    String tableName = this.stats.findTableName(t.getPredicate().toString());
				Node newNode = new VpNode(new TriplePattern(t, prefixes, this.stats.arePrefixesActive()), tableName);
				nodesQueue.add(newNode);
			}
		}
    }
    
    /** Remove every instance of a triple from input triples from the given groups
     * and guarantees that there are no empty entries in groups
     * @param triples list of triples to be removed
     * @param groups HashMap containing a list of grouped triples
     */
    private void removeTriplesFromGroups(List<Triple> triples, HashMap<String, List<Triple>> groups) {
    	for (HashMap.Entry<String, List<Triple>> entry : groups.entrySet()) {
		    entry.getValue().removeAll(triples);
		}
		//remove empty groups
		Iterator it = groups.entrySet().iterator();
	    while (it.hasNext()) {
	        HashMap.Entry<String, List<Triple>> pair = (Entry<String, List<Triple>>) it.next();
	        if (pair.getValue().size()==0) {
	        	it.remove(); // avoids a ConcurrentModificationException
	        }
	    }
    }
    
    /** Groups the input triples by subject
     * @param triples
     * @return hashmap of triples grouped by the subject
     */
    private HashMap<String, List<Triple>> getSubjectGroups(List<Triple> triples) {
    	HashMap<String, List<Triple>> subjectGroups = new HashMap<String, List<Triple>>();
    	for(Triple triple : triples){
			String subject = triple.getSubject().toString(prefixes);
	
			if (subjectGroups.containsKey(subject)) {
				subjectGroups.get(subject).add(triple);
			} else { //new entry in the HashMap
				List<Triple> subjTriples = new ArrayList<Triple>();
				subjTriples.add(triple);
				subjectGroups.put(subject, subjTriples);
			}
		}
    	return subjectGroups;
    }
    
    /** Groups the input triples by object
     * @param triples
     * @return hashmap of triples grouped by the object
     */
    private HashMap<String, List<Triple>> getObjectGroups(List<Triple> triples) {
    	HashMap<String, List<Triple>> objectGroups = new HashMap<String, List<Triple>>();
    	for(Triple triple : triples){
			String object = triple.getObject().toString(prefixes);
	
			if (objectGroups.containsKey(object)) {
				objectGroups.get(object).add(triple);
			} else { //new entry in the HashMap
				List<Triple> objTriples = new ArrayList<Triple>();
				objTriples.add(triple);
				objectGroups.put(object, objTriples);
			}
		}
    	return objectGroups;
    }
 
    /*
     * findRelateNode, given a source node, finds another node
     * with at least one variable in common, if there isn't return null
     */
    private Node findRelateNode(Node sourceNode, PriorityQueue<Node> availableNodes){
    	if (sourceNode.isPropertyTable){
    		// sourceNode is a group
    		for(TriplePattern tripleSource : sourceNode.tripleGroup){
				for (Node node : availableNodes){
					if(node.isPropertyTable||node.isReversePropertyTable) {
						for(TriplePattern tripleDest : node.tripleGroup)
	    					if(existsVariableInCommon(tripleSource, tripleDest))
	    						return node;
					} else {
						if(existsVariableInCommon(tripleSource, node.triplePattern))
							return node;
					}
				}
    		}	
    	} else if(sourceNode.isReversePropertyTable) {
    		// sourceNode is a group
    		for(TriplePattern tripleSource : sourceNode.tripleGroup){
				for (Node node : availableNodes){
					if(node.isReversePropertyTable||node.isPropertyTable) {
						for(TriplePattern tripleDest : node.tripleGroup)
	    					if(existsVariableInCommon(tripleSource, tripleDest))
	    						return node;
					} else {
						if(existsVariableInCommon(tripleSource, node.triplePattern))
							return node;
					}
				}
    		}
    	} else {
    		// source node is not a group
    		// TODO Clarify the for loop
    		for (Node node : availableNodes) {
    			if(node.isPropertyTable || node.isReversePropertyTable) {
    				for(TriplePattern tripleDest : node.tripleGroup){
    					if(existsVariableInCommon(tripleDest, sourceNode.triplePattern))
    						return node;
    				}
    			} else {
    				if(existsVariableInCommon(sourceNode.triplePattern, node.triplePattern))
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
    	if(triple_a.objectType == ElementType.VARIABLE && (
    			triple_a.object.equals(triple_b.subject) || triple_a.object.equals(triple_b.object))) 
    		return true;
    	
    	if(triple_a.subjectType == ElementType.VARIABLE && (
    			triple_a.subject.equals(triple_b.subject) || triple_a.subject.equals(triple_b.object)))
    		return true;
		return false;
    }
    
    /*
     * heuristicWidth decides a width based on the proportion
     * between the number of elements in a table and the unique subjects.
     */
    private int heuristicWidth(Node node){
    	if(node.isPropertyTable)
    		return 5;
    	if(node.isReversePropertyTable)
    		return 5;
    	String predicate = node.triplePattern.predicate;
    	int tableSize = stats.getTableSize(predicate);
    	int numberUniqueSubjects = stats.getTableDistinctSubjects(predicate);
    	float proportion = tableSize / numberUniqueSubjects;
    	if(proportion > 1)
    		return 3;
    	return 2;
    }
    
	public void setPropertyTable(boolean b) {
		this.usePropertyTable = b;
	}
	
	public void setReversePropertyTable(boolean b) {
		this.useReversePropertyTable = b;
	}
	
	public void setMinimumGroupSize(int size){
		this.minimumGroupSize = size;
	}
}
