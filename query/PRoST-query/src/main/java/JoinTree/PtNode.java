package JoinTree;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.spark.sql.SQLContext;

import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.shared.PrefixMapping;

import Executor.Utils;
import Translator.Stats;

/*
 * A node of the JoinTree that refers to the Property Table.
 */
public class PtNode extends Node {
	

  /*
	 * The node contains a list of triple patterns with the same subject.
	 */
	public PtNode(List<TriplePattern> tripleGroup, Stats stats){
		super();
		this.isPropertyTable = true;
		this.tripleGroup = tripleGroup;
		this.stats = stats;
		this.setIsComplex();
		
	}
	
	/*
	 * Alternative constructor, used to instantiate a Node directly with
	 * a list of jena triple patterns.
	 */
	public PtNode(List<Triple> jenaTriples, PrefixMapping prefixes, Stats stats) {
		ArrayList<TriplePattern> triplePatterns = new ArrayList<TriplePattern>();
		this.isPropertyTable = true;
		this.tripleGroup = triplePatterns;
		this.children = new ArrayList<Node>();
		this.projection = Collections.emptyList();
		this.stats = stats;
		for (Triple t : jenaTriples){
		  triplePatterns.add(new TriplePattern(t, prefixes, this.stats.arePrefixesActive()));
		}
		this.setIsComplex();
		
	}

	private void setIsComplex() {
	  for(TriplePattern triplePattern: this.tripleGroup) {
	    triplePattern.isComplex = stats.isTableComplex(triplePattern.predicate);
	  }
    }

  public void computeNodeData(SQLContext sqlContext) {

		StringBuilder query = new StringBuilder("SELECT ");
		ArrayList<String> whereConditions = new ArrayList<String>();
		ArrayList<String> explodedColumns = new ArrayList<String>();

		// subject
		if (tripleGroup.get(0).subjectType == ElementType.VARIABLE) 
		  query.append("s AS " + Utils.removeQuestionMark(tripleGroup.get(0).subject) + ",");

		// objects
		for (TriplePattern t : tripleGroup) {
		    String columnName = stats.findTableName(t.predicate.toString());
		    if (columnName == null) {
		      System.err.println("This column does not exists: " + t.predicate);
		      return;
		    }
		    if(t.subjectType == ElementType.CONSTANT) {
		      whereConditions.add("s='" + t.subject + "'");
		    }
			if (t.objectType == ElementType.CONSTANT) {
				if (t.isComplex)
					whereConditions
							.add("array_contains(" +columnName + ", '" + t.object + "')");
				else
					whereConditions.add(columnName + "='" + t.object + "'");
			} else if (t.isComplex) {
				query.append(" P" + columnName + explodedColumns.size() + " AS " + Utils.removeQuestionMark(t.object) + ",");
				explodedColumns.add(columnName);
			} else {
				query.append(
						" " + columnName + " AS " + Utils.removeQuestionMark(t.object) + ",");
				whereConditions.add(columnName + " IS NOT NULL");
			}
		}

		// delete last comma
		query.deleteCharAt(query.length() - 1);

		// TODO: parameterize the name of the table
		query.append(" FROM property_table ");
		int counter = 0;
		for (String explodedColumn : explodedColumns) {
			query.append("\n lateral view explode(" + explodedColumn + ") exploded" + explodedColumn + " AS P"
					+ explodedColumn + Integer.toString(counter));
			counter++;
		}

		if (!whereConditions.isEmpty()) {
			query.append(" WHERE ");
			query.append(String.join(" AND ", whereConditions));
		}

		this.sparkNodeData = sqlContext.sql(query.toString());
	}
}
