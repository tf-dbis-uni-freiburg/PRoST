package joinTree;

import java.util.ArrayList;
import java.util.List;

import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.shared.PrefixMapping;
import org.apache.log4j.Logger;
import org.apache.spark.sql.SQLContext;
import stats.DatabaseStatistics;
import stats.PropertyStatistics;
import translator.JoinedTriplesGroup;
import utils.Utils;

/**
 * A node that uses a Joined Wide Property Table.
 */
public class JWPTNode extends MVNode {
	private static final Logger logger = Logger.getLogger("PRoST");
	private static final String COLUMN_NAME_COMMON_RESOURCE = "r";
	private static final String WPT_PREFIX = "o_";
	private static final String IWPT_PREFIX = "s_";
	private final List<TriplePattern> wptTripleGroup;
	private final List<TriplePattern> iwptTripleGroup;
	private String JOINED_TABLE_NAME = "joined_wide_property_table_outer";

	public JWPTNode(final JoinedTriplesGroup joinedTriplesGroup, final PrefixMapping prefixes,
					final DatabaseStatistics statistics) {

		super(statistics);

		final ArrayList<TriplePattern> wptTriplePatterns = new ArrayList<>();
		wptTripleGroup = wptTriplePatterns;
		for (final Triple t : joinedTriplesGroup.getWptGroup()) {
			final TriplePattern tp = new TriplePattern(t, prefixes);
			wptTriplePatterns.add(tp);
			tripleGroup.add(tp);
		}

		final ArrayList<TriplePattern> iwptTriplePatterns = new ArrayList<>();
		iwptTripleGroup = iwptTriplePatterns;

		for (final Triple t : joinedTriplesGroup.getIwptGroup()) {
			final TriplePattern tp = new TriplePattern(t, prefixes);
			iwptTriplePatterns.add(tp);
			tripleGroup.add(tp);
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
		final TriplePattern triple = tripleGroup.get(0);
		if (tripleGroup.size() == 1 && triple.predicateType.equals(ElementType.VARIABLE)) {
			if (triple.subjectType.equals(ElementType.CONSTANT) || triple.objectType.equals(ElementType.VARIABLE)) {
				computeForwardVariablePredicateNodeData(sqlContext);
			} else {
				computeInverseVariablePredicateNodeData(sqlContext);
			}
		} else {
			computeConstantPredicateNodeData(sqlContext);
		}
	}

	private void computeConstantPredicateNodeData(final SQLContext sqlContext) {
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
			assert !columnName.equals(WPT_PREFIX) : "This column does not exists: " + columnName;

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
			assert !columnName.equals(IWPT_PREFIX) : "This column does not exists: " + columnName;

			if (t.subjectType == ElementType.CONSTANT) {
				if (t.isComplex) {
					whereElements.add("array_contains(" + columnName + ", '" + t.subject + "')");
				} else {
					whereElements.add(columnName + "='" + t.subject + "'");
				}
			} else if (t.isComplex) {
				selectElements.add(" P" + columnName + " AS " + Utils.removeQuestionMark(t.subject));
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

	//assumes a single pattern in the triples groups, uses the forward part of the table
	private void computeForwardVariablePredicateNodeData(final SQLContext sqlContext) {
		assert tripleGroup.size() == 1 : "JWPT nodes with variable predicates can only contain one triple pattern";

		final List<String> properties = new ArrayList<>();
		for (final PropertyStatistics propertyStatistics : statistics.getProperties().values()) {
			properties.add(propertyStatistics.getInternalName());
		}
		final TriplePattern triple = tripleGroup.get(0);

		for (final String property : properties) {
			final ArrayList<String> selectElements = new ArrayList<>();
			final ArrayList<String> whereElements = new ArrayList<>();
			final ArrayList<String> explodedElements = new ArrayList<>();

			if (triple.subjectType == ElementType.VARIABLE) {
				selectElements.add(COLUMN_NAME_COMMON_RESOURCE + " AS "
						+ Utils.removeQuestionMark(triple.subject));
			} else {
				whereElements.add(COLUMN_NAME_COMMON_RESOURCE + "='" + triple.subject + "'");
			}

			selectElements.add("'" + property + "' as " + Utils.removeQuestionMark(triple.predicate));

			final String columnName = WPT_PREFIX.concat(property);

			if (triple.objectType == ElementType.CONSTANT) {
				if (triple.isComplex) {
					whereElements.add("array_contains(" + columnName + ", '" + triple.object + "')");
				} else {
					whereElements.add(columnName + "='" + triple.object + "'");
				}
			} else if (triple.isComplex) {
				selectElements.add(" P" + columnName + " AS " + Utils.removeQuestionMark(triple.object));
				explodedElements.add("\n lateral view explode(" + columnName + ") exploded" + columnName
						+ " AS P" + columnName);
			} else {
				selectElements.add(columnName + " AS " + Utils.removeQuestionMark(triple.object));
				whereElements.add(columnName + " IS NOT NULL");
			}

			String query = "SELECT " + String.join(",", selectElements);
			query += " FROM " + JOINED_TABLE_NAME;
			if (!explodedElements.isEmpty()) {
				query += " " + String.join(" ", explodedElements);
			}
			if (!whereElements.isEmpty()) {
				query += " WHERE " + String.join(" AND ", whereElements);
			}

			if (sparkNodeData == null) {
				sparkNodeData = sqlContext.sql(query);
			} else {
				sparkNodeData = sparkNodeData.union(sqlContext.sql(query));
			}
		}
	}

	//assumes a single pattern in the triples groups, uses the forward part of the table
	private void computeInverseVariablePredicateNodeData(final SQLContext sqlContext) {
		assert tripleGroup.size() == 1 : "JWPT nodes with variable predicates can only contain one triple pattern";

		final List<String> properties = new ArrayList<>();
		for (final PropertyStatistics propertyStatistics : statistics.getProperties().values()) {
			properties.add(propertyStatistics.getInternalName());
		}
		final TriplePattern triple = tripleGroup.get(0);

		for (final String property : properties) {
			final ArrayList<String> selectElements = new ArrayList<>();
			final ArrayList<String> whereElements = new ArrayList<>();
			final ArrayList<String> explodedElements = new ArrayList<>();

			if (triple.objectType == ElementType.VARIABLE) {
				selectElements.add(COLUMN_NAME_COMMON_RESOURCE + " AS "
						+ Utils.removeQuestionMark(triple.object));
			} else {
				whereElements.add(COLUMN_NAME_COMMON_RESOURCE + "='" + triple.object + "'");
			}

			selectElements.add("'" + property + "' as " + Utils.removeQuestionMark(triple.predicate));

			final String columnName = IWPT_PREFIX.concat(property);

			if (triple.subjectType == ElementType.CONSTANT) {
				if (triple.isComplex) {
					whereElements.add("array_contains(" + columnName + ", '" + triple.subject + "')");
				} else {
					whereElements.add(columnName + "='" + triple.subject + "'");
				}
			} else if (triple.isComplex) {
				selectElements.add(" P" + columnName + " AS ");
				explodedElements.add("\n lateral view explode(" + columnName + ") exploded" + columnName
						+ " AS P" + columnName);
			} else {
				selectElements.add(columnName + " AS " + Utils.removeQuestionMark(triple.subject));
				whereElements.add(columnName + " IS NOT NULL");
			}

			String query = "SELECT " + String.join(",", selectElements);
			query += " FROM " + JOINED_TABLE_NAME;
			if (!explodedElements.isEmpty()) {
				query += " " + String.join(" ", explodedElements);
			}
			if (!whereElements.isEmpty()) {
				query += " WHERE " + String.join(" AND ", whereElements);
			}

			if (sparkNodeData == null) {
				sparkNodeData = sqlContext.sql(query);
			} else {
				sparkNodeData = sparkNodeData.union(sqlContext.sql(query));
			}
		}
	}

	@Override
	public String toString() {
		final StringBuilder str = new StringBuilder("{");
		str.append("JWPT node (").append(this.getPriority()).append("): ");
		for (final TriplePattern tpGroup : tripleGroup) {
			str.append(tpGroup.toString()).append(", ");
		}
		str.append(" }");
		return str.toString();
	}

}
