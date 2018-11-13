package joinTree;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.sql.SQLContext;

import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.shared.PrefixMapping;

import executor.Utils;
import translator.Stats;

public class IptNode extends MVNode {
	private static final String COLUMN_NAME_OBJECT = "o";
	private static final String INVERSE_PROPERTY_TABLE_NAME = "inverse_wide_property_table";

	public List<TriplePattern> tripleGroup;


	/**
	 * The node contains a list of triple patterns with the same object.
	 *
	 * @param tripleGroup List of TriplePattern referring to the same object
	 */
	public IptNode(final List<TriplePattern> tripleGroup) {
		super();
		this.tripleGroup = tripleGroup;
		setIsComplex();
	}

	/**
	 * Alternative constructor, used to instantiate a Node directly with a list of
	 * jena triple patterns with the same object.
	 *
	 * @param jenaTriples list of Triples referring to the same object.
	 * @param prefixes    prefix mapping of the properties.
	 */
	public IptNode(final List<Triple> jenaTriples, final PrefixMapping prefixes) {
		final ArrayList<TriplePattern> triplePatterns = new ArrayList<>();
		for (final Triple t : jenaTriples) {
			triplePatterns.add(new TriplePattern(t, prefixes));
		}
		this.tripleGroup = triplePatterns;
		setIsComplex();
	}

	/**
	 * Uses the database statistics to determine if the object of triples in the
	 * node is complex.
	 */
	private void setIsComplex() {
		for (final TriplePattern triplePattern : tripleGroup) {
			triplePattern.isComplex = Stats.getInstance().isInverseTableComplex(triplePattern.predicate);
		}
	}

	/*
	 * (non-Javadoc)
	 *
	 * @see JoinTree.Node#computeNodeData(org.apache.spark.sql.SQLContext)
	 */
	@Override
	public void computeNodeData(final SQLContext sqlContext) {
		final StringBuilder query = new StringBuilder("SELECT ");
		final ArrayList<String> whereConditions = new ArrayList<>();
		final ArrayList<String> explodedColumns = new ArrayList<>();

		// object
		if (tripleGroup.get(0).objectType == ElementType.VARIABLE) {
			query.append(COLUMN_NAME_OBJECT + " AS " + Utils.removeQuestionMark(tripleGroup.get(0).object) + ",");
		}

		// subjects
		for (final TriplePattern t : tripleGroup) {
			final String columnName = Stats.getInstance().findTableName(t.predicate.toString());
			if (columnName == null) {
				System.err.println("This column does not exists: " + t.predicate);
				return;
			}
			if (t.objectType == ElementType.CONSTANT) {
				whereConditions.add(COLUMN_NAME_OBJECT + "='" + t.object + "'");
			}
			if (t.subjectType == ElementType.CONSTANT) {
				if (t.isComplex) {
					whereConditions.add("array_contains(" + columnName + ", '" + t.subject + "')");
				} else {
					whereConditions.add(columnName + "='" + t.subject + "'");
				}
			} else if (t.isComplex) {
				query.append(" P" + columnName + " AS " + Utils.removeQuestionMark(t.subject) + ",");
				explodedColumns.add(columnName);
			} else {
				query.append(" " + columnName + " AS " + Utils.removeQuestionMark(t.subject) + ",");
				whereConditions.add(columnName + " IS NOT NULL");
			}
		}

		// delete last comma
		query.deleteCharAt(query.length() - 1);


		query.append(" FROM ").append(INVERSE_PROPERTY_TABLE_NAME).append(" ");
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
		str.append("IWPT node: ");
		for (final TriplePattern tpGroup : tripleGroup) {
			str.append(tpGroup.toString() + ", ");
		}
		str.append(" }");
		return str.toString();
	}
}
