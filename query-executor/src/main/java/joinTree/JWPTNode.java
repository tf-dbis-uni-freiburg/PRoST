package joinTree;

import java.util.ArrayList;
import java.util.List;

import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.shared.PrefixMapping;
import org.apache.spark.sql.SQLContext;
import stats.DatabaseStatistics;
import translator.JoinedTriplesGroup;
import utils.Utils;

/**
 * A node that uses a Joined Wide Property Table.
 */
public class JWPTNode extends MVNode {

	private static final String COLUMN_NAME_COMMON_RESOURCE = "r";
	private static final String JOINED_TABLE_NAME = "joined_wide_property_table";
	private static final String WPT_PREFIX = "o_";
	private static final String IWPT_PREFIX = "s_";

	private final List<TriplePattern> wptTripleGroup;
	private final List<TriplePattern> iwptTripleGroup;

	public JWPTNode(final JoinedTriplesGroup joinedTriplesGroup, final PrefixMapping prefixes,
					final DatabaseStatistics statistics) {

		super(statistics);

		final ArrayList<TriplePattern> wptTriplePatterns = new ArrayList<>();
		wptTripleGroup = wptTriplePatterns;
		for (final Triple t : joinedTriplesGroup.getWptGroup()) {
			final TriplePattern tp = new TriplePattern(t, prefixes);
			wptTriplePatterns.add(tp);
		}

		final ArrayList<TriplePattern> iwptTriplePatterns = new ArrayList<>();
		iwptTripleGroup = iwptTriplePatterns;
		for (final Triple t : joinedTriplesGroup.getIwptGroup()) {
			final TriplePattern tp = new TriplePattern(t, prefixes);
			iwptTriplePatterns.add(tp);
		}
		setIsComplex();
	}

	/**
	 * Uses the database statistics to determine if the column in the JWPT for each
	 * <code>TriplePattern</code> in <code>wptTripleGroup</code> and
	 * <code>iwptTripleGroup</code>is complex.
	 */
	private void setIsComplex() {
		for (final TriplePattern triplePattern : wptTripleGroup) {
			triplePattern.isComplex = super.statistics.getProperties().get(triplePattern.predicate).isComplex();
		}

		for (final TriplePattern triplePattern : iwptTripleGroup) {
			triplePattern.isComplex =
					super.statistics.getProperties().get(triplePattern.predicate).isInverseComplex();
		}
	}

	@Override
	public void computeNodeData(final SQLContext sqlContext) {
		final ArrayList<String> whereElements = new ArrayList<>();
		final ArrayList<String> selectElements = new ArrayList<>();
		final ArrayList<String> explodedElements = new ArrayList<>();

		// subject
		if (!wptTripleGroup.isEmpty()) {
			if (wptTripleGroup.get(0).subjectType == ElementType.VARIABLE) {
				selectElements.add(COLUMN_NAME_COMMON_RESOURCE + " AS "
						+ Utils.removeQuestionMark(wptTripleGroup.get(0).subject));
			} else {
				whereElements.add(COLUMN_NAME_COMMON_RESOURCE + "='" + wptTripleGroup.get(0).subject + "'");
			}
		} else if (!iwptTripleGroup.isEmpty()) {
			if (iwptTripleGroup.get(0).objectType == ElementType.VARIABLE) {
				selectElements.add(COLUMN_NAME_COMMON_RESOURCE + " AS "
						+ Utils.removeQuestionMark(iwptTripleGroup.get(0).object));
			} else {
				whereElements.add(COLUMN_NAME_COMMON_RESOURCE + "='" + iwptTripleGroup.get(0).object + "'");
			}
		}

		// forward patterns
		for (final TriplePattern t : wptTripleGroup) {
			final String columnName =
					WPT_PREFIX.concat(statistics.getProperties().get(t.predicate).getInternalName());
			if (columnName.equals(WPT_PREFIX)) {
				System.err.println("This column does not exists: " + t.predicate);
				return;
			}

			if (t.objectType == ElementType.CONSTANT) {
				if (t.isComplex) {
					whereElements.add("array_contains(" + columnName + ", '" + t.object + "')");
				} else {
					whereElements.add(columnName + "='" + t.object + "'");
				}
			} else if (t.isComplex) {
				selectElements.add(" P" + columnName + " AS " + Utils.removeQuestionMark(t.object));
				explodedElements.add("\n lateral view explode(" + columnName + ") exploded" + columnName
						+ " AS P" + columnName);
			} else {
				selectElements.add(columnName + " AS " + Utils.removeQuestionMark(t.object));
				whereElements.add(columnName + " IS NOT NULL");
			}
		}

		// inverse patterns
		for (final TriplePattern t : iwptTripleGroup) {
			final String columnName = IWPT_PREFIX.concat(statistics.getProperties().get(t.predicate).getInternalName());
			if (columnName.equals(IWPT_PREFIX)) {
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
				selectElements.add(" P" + columnName + " AS ");
				explodedElements.add("\n lateral view explode(" + columnName + ") exploded" + columnName
						+ " AS P" + columnName);
			} else {
				selectElements.add(columnName + " AS " + Utils.removeQuestionMark(t.subject));
				whereElements.add(columnName + " IS NOT NULL");
			}
		}

		String query = "SELECT " + String.join(",", selectElements);
		query += " FROM " + JOINED_TABLE_NAME;
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
		str.append("JWPT node: ");
		for (final TriplePattern tpGroup : tripleGroup) {
			str.append(tpGroup.toString()).append(", ");
		}
		str.append(" }");
		return str.toString();
	}

}
