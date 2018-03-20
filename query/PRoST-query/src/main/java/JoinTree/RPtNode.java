package JoinTree;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.spark.sql.SQLContext;

import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.shared.PrefixMapping;

import Executor.Utils;
import Translator.Stats;

/**
 * A node of the JoinTree that refers to the Reverse Property Table.
 */
public class RPtNode extends Node {
	protected final String reversePropertyTableName = "reverse_property_table";
	
	/** The node contains a list of triple patterns with the same object.
	 * @param tripleGroup List of TriplePattern refering to the same object
	 * @param stats Database statistics
	 */
	public RPtNode(List<TriplePattern> tripleGroup, Stats stats){	
		super();
		this.isReversePropertyTable = true;
		this.tripleGroup = tripleGroup;
		this.stats = stats;
		this.setIsComplex();
		
	}
	
	/** Alternative constructor, used to instantiate a Node directly with
	 * a list of jena triple patterns with the same object.
	 * @param jenaTriples list of Triples refering to the same object
	 * @param prefixes 
	 * @param stats Database statistics
	 */
	public RPtNode(List<Triple> jenaTriples, PrefixMapping prefixes, Stats stats) {
		ArrayList<TriplePattern> triplePatterns = new ArrayList<TriplePattern>();
		this.isReversePropertyTable = true;
		this.tripleGroup = triplePatterns;
		this.children = new ArrayList<Node>();
		this.projection = Collections.emptyList();
		this.stats = stats;
		for (Triple t : jenaTriples){
		  triplePatterns.add(new TriplePattern(t, prefixes, this.stats.arePrefixesActive()));
		}
		this.setIsComplex();	
	}

	/** Uses the database statistics to determine if the object of triples in the node is complex
	 */
	private void setIsComplex() {
		for(TriplePattern triplePattern: this.tripleGroup) {
			triplePattern.isComplex = stats.isTableReverseComplex(triplePattern.predicate);
		}
	}

  /* (non-Javadoc)
 * @see JoinTree.Node#computeNodeData(org.apache.spark.sql.SQLContext)
 */
public void computeNodeData(SQLContext sqlContext) {
		StringBuilder query = new StringBuilder("SELECT ");
		ArrayList<String> whereConditions = new ArrayList<String>();
		ArrayList<String> explodedColumns = new ArrayList<String>();

		// object
		if (tripleGroup.get(0).objectType == ElementType.VARIABLE) 
			  query.append("s AS " + Utils.removeQuestionMark(tripleGroup.get(0).object) + ",");

		// subjects
		for (TriplePattern t : tripleGroup) {
		    String columnName = stats.findTableName(t.predicate.toString());
		    if (columnName == null) {
		      System.err.println("This column does not exists: " + t.predicate);
		      return;
		    }
		    if(t.objectType == ElementType.CONSTANT) {
			      whereConditions.add("s='" + t.object + "'");
			}
			if (t.subjectType == ElementType.CONSTANT) {
				if (t.isComplex)
					whereConditions
							.add("array_contains(" +columnName + ", '" + t.subject + "')");
				else
					whereConditions.add(columnName + "='" + t.subject + "'");
			} else if (t.isComplex) {
				query.append(" P" + columnName + explodedColumns.size() + " AS " + Utils.removeQuestionMark(t.subject) + ",");
				explodedColumns.add(columnName);
			} else {
				query.append(
						" " + columnName + " AS " + Utils.removeQuestionMark(t.subject) + ",");
				whereConditions.add(columnName + " IS NOT NULL");
			}
		}

		// delete last comma
		query.deleteCharAt(query.length() - 1);
		
		query.append(" FROM ").append(reversePropertyTableName).append(" ");
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
