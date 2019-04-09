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
public class JPTNode extends MVNode  {
	
	private static final String COLUMN_NAME_COMMON_RESOURCE = "r";
	private static final String JOINED_TABLE_NAME = "joined_wide_property_table";
	private static final String WPT_PREFIX = "o_";
	private static final String IWPT_PREFIX = "s_";

	private final List<TriplePattern> wptTripleGroup;
	private final List<TriplePattern> iwptTripleGroup;

	public JPTNode(final JoinedTriplesGroup joinedTriplesGroup, final PrefixMapping prefixes,
				   final DatabaseStatistics statistics) {

		super(statistics);
		//TODO triplePatterns never used?
		final ArrayList<TriplePattern> triplePatterns = new ArrayList<>();

		final ArrayList<TriplePattern> wptTriplePatterns = new ArrayList<>();
		wptTripleGroup = wptTriplePatterns;
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
	}

	/**
	 * Uses the database statistics to determine if the column in the JWPT for each
	 * <code>TriplePattern</code> in <code>wptTripleGroup</code> and
	 * <code>iwptTripleGroup</code>is complex.
	 */
	private void setIsComplex() {
		for (final TriplePattern triplePattern : wptTripleGroup) {
			triplePattern.isComplex = super.statistics.getPropertyStatistics().get(triplePattern.predicate).isComplex();
		}

		for (final TriplePattern triplePattern : iwptTripleGroup) {
			triplePattern.isComplex =
					super.statistics.getPropertyStatistics().get(triplePattern.predicate).isInverseComplex();
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
				query.append(COLUMN_NAME_COMMON_RESOURCE + " AS ").append(Utils.removeQuestionMark(wptTripleGroup.get(0).subject)).append(",");
			}
		} else if (!iwptTripleGroup.isEmpty()) {
			if (iwptTripleGroup.get(0).objectType == ElementType.VARIABLE) {
				query.append(COLUMN_NAME_COMMON_RESOURCE + " AS ").append(Utils.removeQuestionMark(iwptTripleGroup.get(0).object)).append(",");
			}
		}

		// wpt
		for (final TriplePattern t : wptTripleGroup) {
			final String columnName =
					//TODO check what is being retrieved here. Original statistics was getting the table name, but it
					// should be the name of the column in the pt
					WPT_PREFIX.concat(statistics.getPropertyStatistics().get(t.predicate).getVpTableName());
					//Stats.getInstance().findTableName(t.predicate));
			if (columnName.equals(WPT_PREFIX)) {
				System.err.println("This column does not exists: " + t.predicate);
				return;
			}
			if (t.subjectType == ElementType.CONSTANT) {
				whereConditions.add(COLUMN_NAME_COMMON_RESOURCE + "='" + t.subject + "'");
			}
			if (t.objectType == ElementType.CONSTANT) {
				if (t.isComplex) {
					whereConditions.add("array_contains(" + columnName + ", '" + t.object + "')");
				} else {
					whereConditions.add(columnName + "='" + t.object + "'");
				}
			} else if (t.isComplex) {
				query.append(" P").append(columnName).append(" AS ").append(Utils.removeQuestionMark(t.object)).append(",");
				explodedColumns.add(columnName);
			} else {
				query.append(" ").append(columnName).append(" AS ").append(Utils.removeQuestionMark(t.object)).append(",");
				whereConditions.add(columnName + " IS NOT NULL");
			}
		}

		// iwpt
		for (final TriplePattern t : iwptTripleGroup) {
			//TODO check what is being retrieved here. Original statistics was getting the table name, but it
			// should be the name of the column in the pt
			final String columnName = IWPT_PREFIX.concat(statistics.getPropertyStatistics().get(t.predicate).getVpTableName());
			if (columnName.equals(IWPT_PREFIX)) {
				System.err.println("This column does not exists: " + t.predicate);
				return;
			}
			if (t.objectType == ElementType.CONSTANT) {
				whereConditions.add(COLUMN_NAME_COMMON_RESOURCE + "='" + t.object + "'");
			}
			if (t.subjectType == ElementType.CONSTANT) {
				if (t.isComplex) {
					whereConditions.add("array_contains(" + columnName + ", '" + t.subject + "')");
				} else {
					whereConditions.add(columnName + "='" + t.subject + "'");
				}
			} else if (t.isComplex) {
				query.append(" P").append(columnName).append(" AS ").append(Utils.removeQuestionMark(t.subject)).append(",");
				explodedColumns.add(columnName);
			} else {
				query.append(" ").append(columnName).append(" AS ").append(Utils.removeQuestionMark(t.subject)).append(",");
				whereConditions.add(columnName + " IS NOT NULL");
			}
		}

		// delete last comma
		query.deleteCharAt(query.length() - 1);

		query.append(" FROM ").append(JOINED_TABLE_NAME).append(" ");
		for (final String explodedColumn : explodedColumns) {
			query.append("\n lateral view explode(").append(explodedColumn).append(") exploded").append(explodedColumn).append(" AS P").append(explodedColumn);
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
		str.append("JWPT node: ");
		for (final TriplePattern tpGroup : tripleGroup) {
			str.append(tpGroup.toString()).append(", ");
		}
		str.append(" }");
		return str.toString();
	}

}
