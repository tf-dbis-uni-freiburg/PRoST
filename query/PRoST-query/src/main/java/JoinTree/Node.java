package JoinTree;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

import Executor.Utils;

/*
 * A single node of the JoinTree
 * 
 */
public abstract class Node {
	public TriplePattern triplePattern;
	public List<Node> children;
	public List<String> projection;
	public List<TriplePattern> tripleGroup;
	// the spark dataset containing the data relative to this node
	public Dataset<Row> sparkNodeData;
	public boolean isPropertyTable = false;
	
	/**
	 * computeNodeData sets the Dataset<Row> to the data referring to this node
	 */
	public abstract void computeNodeData(SQLContext sqlContext);
	
	public Node(){
				
		this.children = new ArrayList<Node>();
		
		// set the projections (if present)
		this.projection = Collections.emptyList();
	}
	
	
	public void setProjectionList(List<String> projections) {
		this.projection = projections;
	}
	
	
	// call computeNodeData recursively on the whole subtree
	public void computeSubTreeData(SQLContext sqlContext){
		this.computeNodeData(sqlContext);
		for(Node child: this.children)
			child.computeSubTreeData(sqlContext);
	}
	
	// join tables between itself and all the children
	public Dataset<Row> computeJoinWithChildren(SQLContext sqlContext){
		if (sparkNodeData == null)
			this.computeNodeData(sqlContext);
		Dataset<Row> currentResult = this.sparkNodeData;
		for (Node child: children){
			Dataset<Row> childResult = child.computeJoinWithChildren(sqlContext);
			String joinVariable = Utils.findCommonVariable(triplePattern, tripleGroup, child.triplePattern, child.tripleGroup);
			if (joinVariable != null)
				currentResult = currentResult.join(childResult, joinVariable);
			
		}
		return currentResult;
	}
	
	
	@Override
	public String toString(){
		StringBuilder str = new StringBuilder("[Triple: " + triplePattern.toString() + " Children: ");
		for (Node child: children){
			str.append(child.toString() + "\t" );
		}
		str.append("]");
		return str.toString();
	}

	public void addChildren(Node newNode) {
		this.children.add(newNode);
		
	}

	public int getChildrenCount() {
		return this.children.size();
	}
}