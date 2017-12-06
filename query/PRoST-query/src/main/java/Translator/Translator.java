package Translator;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.PriorityQueue;

import org.apache.log4j.Logger;

import JoinTree.ElementType;
import JoinTree.JoinTree;
import JoinTree.Node;
import JoinTree.PtNode;
import JoinTree.TriplePattern;
import JoinTree.VpNode;

import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.query.Query;
import com.hp.hpl.jena.query.QueryFactory;
import com.hp.hpl.jena.shared.PrefixMapping;
import com.hp.hpl.jena.sparql.algebra.Algebra;
import com.hp.hpl.jena.sparql.algebra.op.OpBGP;
import com.hp.hpl.jena.sparql.algebra.op.OpProject;
import com.hp.hpl.jena.sparql.core.Var;
/**
 * This class parses the SPARQL query,
 * build the Tree and save its serialization in a file.
 *
 * @author Matteo Cossu
 */
public class Translator {
	
	final int DEFAULT_MIN_GROUP_SIZE = 2;
    String inputFile;
    String outputFile;
    String statsFile;
    Stats stats;
    boolean statsActive = false;
    int treeWidth;
    int minimumGroupSize = DEFAULT_MIN_GROUP_SIZE;
    PrefixMapping prefixes;
    List<Var> variables;
    List<Triple> triples;
	private boolean usePropertyTable;
    private static final Logger logger = Logger.getLogger(run.Main.class);
    
    public Translator(String input, String output, String statsPath, int treeWidth) {
    	this.inputFile = input;
    	this.outputFile = output != null && output.length() > 0 ? output : input + ".out";
    	this.statsFile = statsPath;
    	if(statsFile.length() > 0){
    		stats = new Stats(statsFile);
    		statsActive = true;
    	}
    	this.treeWidth = treeWidth;
    }
    
    public JoinTree translateQuery(){
    	
    	// parse the query and extract prefixes
        Query query = QueryFactory.read("file:"+inputFile);
        prefixes = query.getPrefixMapping();
        
        logger.info("** SPARQL QUERY **\n" + query +"\n****************"  );
        
        // extract variables and list of triples from the unique BGP
        OpProject opRoot = (OpProject) Algebra.compile(query);
        OpBGP singleBGP = (OpBGP) opRoot.getSubOp();
        variables = opRoot.getVars();
        triples = singleBGP.getPattern().getList();
        
        // build the tree
        Node root_node = buildTree();
        logger.info("** Spark JoinTree **\n" + root_node +"\n****************" );
        
        return new JoinTree(root_node);
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
    	if(usePropertyTable){
			HashMap<String, List<Triple>> subjectGroups = new HashMap<String, List<Triple>>();
			
			// group by subjects
			for(Triple triple : triples){
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
			for(String subject : subjectGroups.keySet()){
				if (subjectGroups.get(subject).size() >= minimumGroupSize){
					nodesQueue.add(new PtNode(subjectGroups.get(subject), prefixes));
				} else {
					for (Triple t : subjectGroups.get(subject)){
						Node newNode = new VpNode(new TriplePattern(t, prefixes));
						nodesQueue.add(newNode);
					}
				}
			}			
    
		} else {
			for(Triple t : triples){
				Node newNode = new VpNode(new TriplePattern(t, prefixes));
				nodesQueue.add(newNode);
			}
		}
    	return nodesQueue;
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
					if(node.isPropertyTable) {
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
    		for (Node node : availableNodes) {
    			if(node.isPropertyTable) {
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
    	String predicate = node.triplePattern.predicate;
    	int tableSize = stats.getTableSize(predicate);
    	int numberUniqueSubjects = stats.getTableDistinctSubjects(predicate);
    	float proportion = tableSize / numberUniqueSubjects;
    	if(proportion > 1)
    		return 3;
    	return 2;
    }
    
    
    /*
     * Simple triples reordering based on statistics.
     * Thanks to this method the building of the tree will follow a better order.
     */
    private void sortTriples(){
    	if(triples.size() == 0 || !statsActive) return;
    	
    	logger.info("Triples being sorted");
    	
    	// find the best root
    	int indexBestRoot = 0;
    	String predicate = triples.get(0).getPredicate().toString(prefixes);
    	int bestSize = stats.getTableSize(predicate);
    	float bestProportion = bestSize / stats.getTableDistinctSubjects(predicate);
    	for(int i = 1; i < triples.size(); i++){
    		predicate = triples.get(i).getPredicate().toString(prefixes);
        	float proportion = stats.getTableSize(predicate) / 
        			stats.getTableDistinctSubjects(predicate);
        	
        	// update best if the proportion is better
        	if (proportion > bestProportion){
        		indexBestRoot = i;
        		bestProportion = proportion;
        		bestSize = stats.getTableSize(predicate); 
        	} // or if the table size is bigger
        	else if (proportion == bestProportion && stats.getTableSize(predicate) > bestSize) {
        		indexBestRoot = i;
        		bestSize = stats.getTableSize(predicate);        		
        	}
    	}

    	// move the best triple to the front
    	if(indexBestRoot > 0){
    		Triple tripleToMove = triples.get(indexBestRoot);
    		triples.remove(indexBestRoot);
    		triples.add(0, tripleToMove);
    	}
    	  	
    }

	public void setPropertyTable(boolean b) {
		this.usePropertyTable = b;
	}
	
	public void setMinimumGroupSize(int size){
		this.minimumGroupSize = size;
	}
    
}
