package joinTree;

import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;
import org.apache.spark.sql.SQLContext;

import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.shared.PrefixMapping;

import joinTree.stats.Stats;
import utils.Utils;

/*
 * A node of the JoinTree that refers to the Property Table.
 */
public class PTNode extends MVNode {

	private static final Logger logger = Logger.getLogger("PRoST");

	/**
	 * The default value is "wide_property_table" when only one PT exists. If an
	 * emergent schema is used, then there exist more than one property tables. Each
	 * of them contains a specific set of predicates. In this case, the default
	 * table name can be changes depending on the list of triples this node
	 * contains.
	 */
	// private String tableName = "par_wide_property_table";
	private String tableName = "wide_property_table";

	public PTNode(Node parent, final List<TriplePattern> tripleGroup) {
		this.parent = parent;
		this.tripleGroup = tripleGroup;
		setIsComplex();
	}

	/*
	 * Alternative constructor, used to instantiate a Node directly with a list of
	 * jena triple patterns.
	 */
	public PTNode(final List<Triple> jenaTriples, final PrefixMapping prefixes) {
		final ArrayList<TriplePattern> triplePatterns = new ArrayList<>();
		tripleGroup = triplePatterns;
		for (final Triple t : jenaTriples) {
			triplePatterns.add(new TriplePattern(t, prefixes));
		}
		setIsComplex();
	}

	/*
	 * Alternative constructor, used to instantiate a Node directly with a list of
	 * jena triple patterns.
	 */
	public PTNode(final List<Triple> jenaTriples, final PrefixMapping prefixes, String tableName) {
		this(jenaTriples, prefixes);
		this.tableName = tableName;
	}
	
	/**
	 * If an emergent schema is used, then there exist more than one property
	 * tables. In this case, the default table name can be changes depending on the
	 * list of triples this node contains.
	 */
	public void setTableName(String tableName) {
		this.tableName = tableName;
	}

	/**
	 * For each triple, set if it contains a complex predicate. A predicate is
	 * complex when there exists at least one subject that has two or more triples
	 * containing the predicate.
	 */
	private void setIsComplex() {
		for (final TriplePattern triplePattern : tripleGroup) {
			triplePattern.isComplex = Stats.getInstance().isTableComplex(triplePattern.predicate);
		}
	}

	@Override
	public void computeNodeData(final SQLContext sqlContext) {

		final StringBuilder query = new StringBuilder("SELECT ");
		final ArrayList<String> whereConditions = new ArrayList<>();
		final ArrayList<String> explodedColumns = new ArrayList<>();

		// subject
		if (tripleGroup.get(0).subjectType == ElementType.VARIABLE) {
			query.append("s AS " + Utils.removeQuestionMark(tripleGroup.get(0).subject) + ",");
		}

		// objects
		for (final TriplePattern t : tripleGroup) {
			final String columnName = Stats.getInstance().findTableName(t.predicate.toString());
			if (columnName == null) {
				System.err.println("This column does not exists: " + t.predicate);
				return;
			}
			if (t.subjectType == ElementType.CONSTANT) {
				whereConditions.add("s='" + t.subject + "'");
			}
			if (t.objectType == ElementType.CONSTANT) {
				if (t.isComplex) {
					whereConditions.add("array_contains(" + columnName + ", '" + t.object + "')");
				} else {
					whereConditions.add(columnName + "='" + t.object + "'");
				}
			} else if (t.isComplex) {
				query.append(" P" + columnName + " AS " + Utils.removeQuestionMark(t.object) + ",");
				explodedColumns.add(columnName);
			} else {
				query.append(" " + columnName + " AS " + Utils.removeQuestionMark(t.object) + ",");
				whereConditions.add(columnName + " IS NOT NULL");
			}
		}

		// delete last comma
		query.deleteCharAt(query.length() - 1);
		query.append(" FROM " + this.tableName + " ");
		//query.append(" FROM " + "par_" + this.tableName + " ");
		for (final String explodedColumn : explodedColumns) {
			query.append("\n lateral view explode(" + explodedColumn + ") exploded" + explodedColumn + " AS P"
					+ explodedColumn);
		}

		if (!whereConditions.isEmpty()) {
			query.append(" WHERE ");
			query.append(String.join(" AND ", whereConditions));
		}
		sparkNodeData = sqlContext.sql(query.toString());
	}

	@Override
	public String toString() {
		final StringBuilder str = new StringBuilder("{");
		str.append("WPT node: ");
		for (final TriplePattern tpGroup : tripleGroup) {
			str.append(tpGroup.toString() + ", ");
		}
		str.append(" }");
		return str.toString();
	}
}
