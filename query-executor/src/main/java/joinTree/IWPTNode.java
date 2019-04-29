package joinTree;

import java.util.ArrayList;
import java.util.List;

import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.shared.PrefixMapping;
import org.apache.spark.sql.SQLContext;
import stats.DatabaseStatistics;
import utils.Utils;

/**
 * A node that uses an Inverse Wide Property Table.
 */
public class IWPTNode extends MVNode {
	private static final String OBJECT_COLUMN_NAME = "o";
	private static final String TABLE_NAME = "inverse_wide_property_table";

	private final List<TriplePattern> tripleGroup;


	/**
	 * Alternative constructor, used to instantiate a Node directly with a list of
	 * jena triple patterns with the same object.
	 *
	 * @param jenaTriples list of Triples referring to the same object.
	 * @param prefixes    prefix mapping of the properties.
	 */
	public IWPTNode(final List<Triple> jenaTriples, final PrefixMapping prefixes, final DatabaseStatistics statistics) {
		super(statistics);
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
			triplePattern.isComplex =
					statistics.getProperties().get(triplePattern.predicate).isInverseComplex();
		}
	}

	/*
	 * (non-Javadoc)
	 *
	 * @see JoinTree.Node#computeNodeData(org.apache.spark.sql.SQLContext)
	 */
	@Override
	public void computeNodeData(final SQLContext sqlContext) {
		final ArrayList<String> whereElements = new ArrayList<>();
		final ArrayList<String> selectElements = new ArrayList<>();
		final ArrayList<String> explodedElements = new ArrayList<>();

		if (tripleGroup.get(0).objectType == ElementType.VARIABLE) {
			selectElements.add(OBJECT_COLUMN_NAME + " AS ");
		} else {
			whereElements.add(OBJECT_COLUMN_NAME + "='" + tripleGroup.get(0).object + "'");
		}

		for (final TriplePattern t : tripleGroup) {
			final String columnName = statistics.getProperties().get(t.predicate).getInternalName();
			if (columnName == null) {
				System.err.println("This column does not exists: " + t.predicate);
				return;
			}
			if (t.subjectType == ElementType.CONSTANT) {
				if (t.isComplex) {
					whereElements.add("array_contains(" + columnName + ", '" + t.subject + "')");
				} else {
					whereElements.add(columnName + "='" + t.subject + "'");
				}
			} else if (t.isComplex) {
				selectElements.add("P" + columnName + " AS " + Utils.removeQuestionMark(t.subject));
				explodedElements.add("\n lateral view explode(" + columnName + ") exploded" + columnName
						+ " AS P" + columnName);
			} else {
				selectElements.add(columnName + " AS " + Utils.removeQuestionMark(t.subject));
				whereElements.add(columnName + " IS NOT NULL");
			}
		}

		String query = "SELECT " + String.join(", ", selectElements);
		query += " FROM " + TABLE_NAME;
		if (!explodedElements.isEmpty()) {
			query += " " + String.join(" ", explodedElements);
		}
		if (!whereElements.isEmpty()) {
			query += " WHERE " + String.join(" AND ", whereElements);
		}

		sparkNodeData = sqlContext.sql(query);
	}

	@Override
	public String toString() {
		final StringBuilder str = new StringBuilder("{");
		str.append("IWPT node: ");
		for (final TriplePattern tpGroup : tripleGroup) {
			str.append(tpGroup.toString()).append(", ");
		}
		str.append(" }");
		return str.toString();
	}
}
