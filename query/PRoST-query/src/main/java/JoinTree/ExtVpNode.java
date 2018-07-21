package JoinTree;

import java.util.Collections;

import org.apache.log4j.Logger;
import org.apache.spark.sql.SQLContext;

import Executor.Utils;

public class ExtVpNode extends Node{
	//private static final Logger logger = Logger.getLogger("PRoST");
	
	private String tableName;
	//private String extVPDatabaseName;
	private String tableNameWithDatabaseIdentifier;
	
	public ExtVpNode(TriplePattern triplePattern, String tableName, String databaseName) {
		super();
		this.isExtVPNode = true;
		this.triplePattern = triplePattern;
        this.tripleGroup = Collections.emptyList();    
		this.tableName = tableName;
		//this.extVPDatabaseName = databaseName;
		this.tableNameWithDatabaseIdentifier = databaseName + "." + tableName;
	}

	@Override
	public void computeNodeData(SQLContext sqlContext) {
		
		TriplePattern mainPattern = triplePattern;

		StringBuilder query = new StringBuilder("Select distinct ");
		if (mainPattern.subjectType == ElementType.VARIABLE && mainPattern.objectType == ElementType.VARIABLE) {
			query.append("s as " + Utils.removeQuestionMark(mainPattern.subject) + ", o as " + Utils.removeQuestionMark(mainPattern.object) + " ");
		}
		else if (mainPattern.subjectType == ElementType.VARIABLE)
            query.append("s AS " + Utils.removeQuestionMark(mainPattern.subject) );
        else if (mainPattern.objectType == ElementType.VARIABLE) 
            query.append("o AS " + Utils.removeQuestionMark(mainPattern.object));
		
		query.append(" from " + tableNameWithDatabaseIdentifier);
		
		if (mainPattern.objectType == ElementType.CONSTANT || mainPattern.subjectType == ElementType.CONSTANT) {
			query.append(" where ");
			if (mainPattern.objectType == ElementType.CONSTANT && mainPattern.subjectType == ElementType.CONSTANT) {
				query.append(" s='" + mainPattern.subject +"' and o='" + mainPattern.object + "'");
			}		
			else if (mainPattern.subjectType == ElementType.CONSTANT) {
	            query.append(" s='" + mainPattern.subject +"' ");
			}
			else {
	            query.append(" o='" + mainPattern.object +"' ");
			}
		}
		
		//logger.info(query.toString());
		this.sparkNodeData = sqlContext.sql(query.toString());
	}
	
	@Override
	public String toString() {
		StringBuilder str = new StringBuilder("{");
		str.append("- " + this.tableName + "- ");
		str.append(triplePattern.toString());
		str.append(" }");
		str.append(" [");
		for (Node child : children) {
			str.append("\n" + child.toString());
		}
		str.append("\n]");
		return str.toString();
	}
}
