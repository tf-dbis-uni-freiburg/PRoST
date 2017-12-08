package JoinTree;

import java.util.Collections;

import org.apache.spark.sql.SQLContext;

import Executor.Utils;


/*
 * A node of the JoinTree that refers to the Vertical Partitioning.
 */
public class VpNode extends Node {
	
	/*
	 * The node contains a single triple pattern.
	 */
	public VpNode(TriplePattern triplePattern){
		
		super();
		this.triplePattern = triplePattern;
		this.tripleGroup = Collections.emptyList();
		
	}
	
	public void computeNodeData(SQLContext sqlContext){
		
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

}
