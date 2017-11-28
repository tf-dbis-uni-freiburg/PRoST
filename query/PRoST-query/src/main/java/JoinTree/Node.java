package JoinTree;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.shared.PrefixMapping;

import Executor.Utils;

public class Node {
	public TriplePattern triplePattern;
	public List<Node> children;
	public List<String> projection;
	public List<TriplePattern> tripleGroup;
	// the spark dataset containing the data relative to this node
	public Dataset<Row> sparkNodeData;
	public boolean isPropertyTable = false;
	
	
	public Node(TriplePattern triplePattern, List<TriplePattern> tripleGroup){
		
		if(tripleGroup.size() > 0){
			this.isPropertyTable = true;
			this.tripleGroup = tripleGroup;
		} else {
			this.triplePattern = triplePattern;
			this.tripleGroup = Collections.emptyList();
		}
		
		this.children = new ArrayList<Node>();
		
		// set the projections (if present)
		this.projection = Collections.emptyList();
		
	}
	
	public Node(List<Triple> jenaTriples, PrefixMapping prefixes) {
		ArrayList<TriplePattern> triplePatterns = new ArrayList<TriplePattern>();
		for (Triple t : jenaTriples){
			triplePatterns.add(new TriplePattern(t, prefixes));
		}
		this.isPropertyTable = true;
		this.tripleGroup = triplePatterns;
		this.children = new ArrayList<Node>();
		this.projection = Collections.emptyList();
		
	}
	
	public void setProjectionList(List<String> projections) {
		this.projection = projections;
	}
	
	
	/**
	 * computeNodeData sets the Dataset<Row> to the data referring to this node
	 */
	public void computeNodeData(SQLContext sqlContext){
		if(isPropertyTable){
			computePropertyTableNodeData(sqlContext);
			return;
		}
		StringBuilder query = new StringBuilder("SELECT DISTINCT ");
		
		// SELECT
		if (triplePattern.subjectType == ElementType.VARIABLE &&
				triplePattern.objectType == ElementType.VARIABLE)
			query.append("s AS " + Utils.removeQuestionMark(triplePattern.subject) + 
					", o AS " + Utils.removeQuestionMark(triplePattern.object) + " ");
		else if (triplePattern.subjectType == ElementType.VARIABLE)
			query.append("s AS " + Utils.removeQuestionMark(triplePattern.subject) );
		else if (triplePattern.objectType == ElementType.VARIABLE) 
			query.append("o AS " + Utils.removeQuestionMark(triplePattern.object));
	
		// FROM
		query.append(" FROM ");
		query.append("vp_" + Utils.toMetastoreName(triplePattern.predicate));
		
		// WHERE
		if( triplePattern.objectType == ElementType.CONSTANT || triplePattern.subjectType == ElementType.CONSTANT)
			query.append(" WHERE ");
		if (triplePattern.objectType == ElementType.CONSTANT)
			query.append(" o='" + triplePattern.object +"' ");
		
		if (triplePattern.subjectType == ElementType.CONSTANT)
			query.append(" o='" + triplePattern.subject +"' ");
		
		this.sparkNodeData = sqlContext.sql(query.toString());
	}
	
	public void computePropertyTableNodeData(SQLContext sqlContext){
		
		StringBuilder query = new StringBuilder("SELECT ");
		ArrayList<String> whereConditions = new ArrayList<String>();		
		ArrayList<String> explodedColumns = new ArrayList<String>();
		
		// subject
		query.append("s AS " + Utils.removeQuestionMark(tripleGroup.get(0).subject) + ",");

		// objects
		for(TriplePattern t : tripleGroup){
			if (t.objectType == ElementType.CONSTANT){
				if (t.isComplex)
					whereConditions.add("array_contains(" + Utils.toMetastoreName(t.predicate) + ", '" + t.object + "')");
				else
					whereConditions.add(Utils.toMetastoreName(t.predicate) + "='" + t.object + "'");
			} else if (t.isComplex){
				String columnName = Utils.toMetastoreName(t.predicate);
				query.append(" P" + columnName +" AS " +
					Utils.removeQuestionMark(t.object) + ",");
				explodedColumns.add(columnName);
				//query.append(" explode(" + Utils.toMetastoreName(t.predicate) + ") AS " + 
				//		Utils.removeQuestionMark(t.object) + ",");
			} else {
				query.append(" " + Utils.toMetastoreName(t.predicate) + " AS " +
						Utils.removeQuestionMark(t.object) + ",");
				whereConditions.add(Utils.toMetastoreName(t.predicate) + " IS NOT NULL");
			} 
		}
		
		// delete last comma
		query.deleteCharAt(query.length() - 1);
		
		// TODO: parameterize the name of the table
		query.append(" FROM property_table ");
		for(String explodedColumn : explodedColumns){
			query.append("\n lateral view explode(" + explodedColumn +") exploded" + explodedColumn +
					" AS P" + explodedColumn);
		}
		
		
		if(!whereConditions.isEmpty()){
			query.append(" WHERE ");
			query.append(String.join(" AND ", whereConditions));
		}
		
		this.sparkNodeData = sqlContext.sql(query.toString());
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