package JoinTree;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.spark.sql.SQLContext;

import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.shared.PrefixMapping;

import Executor.Utils;

/*
 * A node of the JoinTree that refers to the Property Table.
 */
public class PtNode extends Node {
	
	/*
	 * The node contains a list of triple patterns with the same subject.
	 */
	public PtNode(List<TriplePattern> tripleGroup){
		
		super();
		this.isPropertyTable = true;
		this.tripleGroup = tripleGroup;
		
	}
	
	/*
	 * Alternative constructor, used to instantiate a Node directly with
	 * a list of jena triple patterns.
	 */
	public PtNode(List<Triple> jenaTriples, PrefixMapping prefixes) {
		ArrayList<TriplePattern> triplePatterns = new ArrayList<TriplePattern>();
		for (Triple t : jenaTriples){
			triplePatterns.add(new TriplePattern(t, prefixes));
		}
		this.isPropertyTable = true;
		this.tripleGroup = triplePatterns;
		this.children = new ArrayList<Node>();
		this.projection = Collections.emptyList();
		
	}

	public void computeNodeData(SQLContext sqlContext) {

		StringBuilder query = new StringBuilder("SELECT ");
		ArrayList<String> whereConditions = new ArrayList<String>();
		ArrayList<String> explodedColumns = new ArrayList<String>();

		// subject
		query.append("s AS " + Utils.removeQuestionMark(tripleGroup.get(0).subject) + ",");

		// objects
		for (TriplePattern t : tripleGroup) {
			if (t.objectType == ElementType.CONSTANT) {
				if (t.isComplex)
					whereConditions
							.add("array_contains(" + Utils.toMetastoreName(t.predicate) + ", '" + t.object + "')");
				else
					whereConditions.add(Utils.toMetastoreName(t.predicate) + "='" + t.object + "'");
			} else if (t.isComplex) {
				String columnName = Utils.toMetastoreName(t.predicate);
				query.append(" P" + columnName + " AS " + Utils.removeQuestionMark(t.object) + ",");
				explodedColumns.add(columnName);
			} else {
				query.append(
						" " + Utils.toMetastoreName(t.predicate) + " AS " + Utils.removeQuestionMark(t.object) + ",");
				whereConditions.add(Utils.toMetastoreName(t.predicate) + " IS NOT NULL");
			}
		}

		// delete last comma
		query.deleteCharAt(query.length() - 1);

		// TODO: parameterize the name of the table
		query.append(" FROM property_table ");
		for (String explodedColumn : explodedColumns) {
			query.append("\n lateral view explode(" + explodedColumn + ") exploded" + explodedColumn + " AS P"
					+ explodedColumn);
		}

		if (!whereConditions.isEmpty()) {
			query.append(" WHERE ");
			query.append(String.join(" AND ", whereConditions));
		}

		this.sparkNodeData = sqlContext.sql(query.toString());
	}
}
