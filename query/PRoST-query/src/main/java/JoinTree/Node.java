package JoinTree;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.log4j.Logger;
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
	// the spark data set containing the data relative to this node
	public Dataset<Row> sparkNodeData;
	public boolean isPropertyTable = false;
	public boolean isExtVPNode = false;
	public boolean isVPNode = false;
	public String filter;
	
	private boolean isRootNode = false;
	
	private static final Logger logger = Logger.getLogger("PRoST");
	
	/**
	 * computeNodeData sets the Dataset<Row> to the data referring to this node
	 */
	public abstract void computeNodeData(SQLContext sqlContext);

	public Node() {

		this.children = new ArrayList<Node>();

		// set the projections (if present)
		this.projection = Collections.emptyList();
	}

	public void setProjectionList(List<String> projections) {
		this.projection = projections;
	}

	// call computeNodeData recursively on the whole subtree
	public void computeSubTreeData(SQLContext sqlContext) {
		this.computeNodeData(sqlContext);
		for (Node child : this.children)
			child.computeSubTreeData(sqlContext);
	}

	// join tables between itself and all the children
	public Dataset<Row> computeJoinWithChildren(SQLContext sqlContext) {
		if (sparkNodeData == null)
			this.computeNodeData(sqlContext);
		Dataset<Row> currentResult = this.sparkNodeData;
		
		
		//logger.info("Computing node data");
		
		//if (children.size()==0) {
		//	this.isRootNode = true;
		//	logger.info("set to root");
		//}
		
		for (Node child : children) {
			//logger.info("computing child data");
			
			Dataset<Row> childResult = child.computeJoinWithChildren(sqlContext);
			//if (child.isRootNode) {
			//	logger.info("child is root");
			//}
			List<String> joinVariables = Utils.commonVariables(currentResult.columns(), childResult.columns());
			currentResult = currentResult.join(childResult,
					scala.collection.JavaConversions.asScalaBuffer(joinVariables).seq());
		}
		
		return currentResult;
	}

	@Override
	public String toString() {
		StringBuilder str = new StringBuilder("{");
		if (this instanceof PtNode) {
			for (TriplePattern tp_group : this.tripleGroup)
				str.append(tp_group.toString() + ", ");
		} else {
			str.append(triplePattern.toString());
		}
		str.append(" }");
		str.append(" [");
		for (Node child : children) {
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