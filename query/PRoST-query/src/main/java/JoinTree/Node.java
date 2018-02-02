package JoinTree;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

import Executor.Utils;
import Translator.Stats;

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
	public boolean isReversePropertyTable = false;
	protected Stats stats;
	
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
			List<String> joinVariables = Utils.commonVariables(currentResult.columns(), childResult.columns());
			currentResult = currentResult.join(childResult, 
			    scala.collection.JavaConversions.asScalaBuffer(joinVariables).seq());
		}
		return currentResult;
	}
	
	
	@Override
	public String toString(){
		StringBuilder str = new StringBuilder("{");
		if (this instanceof PtNode) {
			str.append("pt node: ");
		  for(TriplePattern tp_group : this.tripleGroup)
		    str.append(tp_group.toString() + ", ");
		} else if (this instanceof RPtNode){
			str.append("rpt node: ");
			for(TriplePattern tp_group : this.tripleGroup)
			    str.append(tp_group.toString() + ", ");
		} else {
			str.append("vp node: ");
			str.append(triplePattern.toString());
		}
		str.append(" }");
		str.append(" [");
		for (Node child: children){
			str.append("\n" + child.toString());
		}
		str.append("\n]");
		return str.toString();
	}

	public void addChildren(Node newNode) {
		this.children.add(newNode);
		
	}

	public int getChildrenCount() {
		return this.children.size();
	}
	
	
}