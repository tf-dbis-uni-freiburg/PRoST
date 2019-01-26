package joinTree;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.spark.sql.SQLContext;

import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.shared.PrefixMapping;

import executor.Utils;
import translator.JoinedTriplesGroup;
import translator.Stats;

/**
 * A node that uses a Joined Wide Property Table.
 *
 */
public class JptNode extends Node {
	private static final String COLUMN_NAME_COMMON_RESOURCE = "r";
	private static final String TABLE_NAME = "joined_wide_property_table";
	private static final String WPT_PREFIX = "o_";
	private static final String IWPT_PREFIX = "s_";
	private final List<TriplePattern> wptTripleGroup;
	private final List<TriplePattern> iwptTripleGroup;
	public PrefixMapping prefixes;

	public JptNode(final JoinedTriplesGroup joinedTriplesGroup, final PrefixMapping prefixes) {
		final ArrayList<TriplePattern> triplePatterns = new ArrayList<>();
		tripleGroup = triplePatterns;

		final ArrayList<TriplePattern> wptTriplePatterns = new ArrayList<>();
		wptTripleGroup = wptTriplePatterns;
		children = new ArrayList<>();
		projection = Collections.emptyList();
		for (final Triple t : joinedTriplesGroup.getWptGroup()) {
			final TriplePattern tp = new TriplePattern(t, prefixes);
			wptTriplePatterns.add(tp);
			triplePatterns.add(tp);
		}

		final ArrayList<TriplePattern> iwptTriplePatterns = new ArrayList<>();
		iwptTripleGroup = iwptTriplePatterns;
		for (final Triple t : joinedTriplesGroup.getIwptGroup()) {
			final TriplePattern tp = new TriplePattern(t, prefixes);
			iwptTriplePatterns.add(tp);
			triplePatterns.add(tp);
		}
		setIsComplex();
		this.prefixes = prefixes;
	}

	/**
	 * Uses the database statistics to determine if the column in the JWPT for each
	 * <code>TriplePattern</code> in <code>wptTripleGroup</code> and
	 * <code>iwptTripleGroup</code>is complex.
	 */
	private void setIsComplex() {
		for (final TriplePattern triplePattern : wptTripleGroup) {
			triplePattern.isComplex = Stats.getInstance().isTableComplex(triplePattern.predicate);
		}

		for (final TriplePattern triplePattern : iwptTripleGroup) {
			triplePattern.isComplex = Stats.getInstance().isInverseTableComplex(triplePattern.predicate);
		}
	}

	@Override
	public void computeNodeData(final SQLContext sqlContext) {
		final StringBuilder query = new StringBuilder("SELECT ");
		final ArrayList<String> whereConditions = new ArrayList<>();
		final ArrayList<String> explodedColumns = new ArrayList<>();

		// subject
		if (!wptTripleGroup.isEmpty()) {
			if (wptTripleGroup.get(0).subjectType == ElementType.VARIABLE) {
				query.append(COLUMN_NAME_COMMON_RESOURCE + " AS " + Utils.removeQuestionMark(wptTripleGroup.get(0).subject) + ",");
			}
		} else if (!iwptTripleGroup.isEmpty()) {
			if (iwptTripleGroup.get(0).objectType == ElementType.VARIABLE) {
				query.append(COLUMN_NAME_COMMON_RESOURCE + " AS " + Utils.removeQuestionMark(iwptTripleGroup.get(0).object) + ",");
			}
		}

		// wpt
		for (final TriplePattern t : wptTripleGroup) {
			final String columnName = WPT_PREFIX.concat(Stats.getInstance().findCorrectTableName(t.predicate.toString(), prefixes));
			if (columnName.equals(WPT_PREFIX)) {
				System.err.println("This column does not exists: " + t.predicate);
				return;
			}
			if (t.subjectType == ElementType.CONSTANT) {
				whereConditions.add(COLUMN_NAME_COMMON_RESOURCE + "='<" + t.subject + ">'");
			}
			if (t.objectType == ElementType.CONSTANT) {
				if (t.isComplex) {
					whereConditions.add("array_contains(" + columnName + ", '<" + t.object + ">')");
				} else {
					whereConditions.add(columnName + "='<" + t.object + ">'");
				}
			} else if (t.isComplex) {
				query.append(" P" + columnName + " AS " + Utils.removeQuestionMark(t.object) + ",");
				explodedColumns.add(columnName);
			} else {
				query.append(" " + columnName + " AS " + Utils.removeQuestionMark(t.object) + ",");
				whereConditions.add(columnName + " IS NOT NULL");
			}
		}

		// iwpt
		for (final TriplePattern t : iwptTripleGroup) {
			final String columnName = IWPT_PREFIX.concat(Stats.getInstance().findCorrectTableName(t.predicate.toString(), prefixes));
			if (columnName.equals(IWPT_PREFIX)) {
				System.err.println("This column does not exists: " + t.predicate);
				return;
			}
			if (t.objectType == ElementType.CONSTANT) {
				whereConditions.add(COLUMN_NAME_COMMON_RESOURCE + "='<" + t.object + ">'");
			}
			if (t.subjectType == ElementType.CONSTANT) {
				if (t.isComplex) {
					whereConditions.add("array_contains(" + columnName + ", '<" + t.subject + ">')");
				} else {
					whereConditions.add(columnName + "='<" + t.subject + ">'");
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

		query.append(" FROM ").append(TABLE_NAME).append(" ");
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
}
