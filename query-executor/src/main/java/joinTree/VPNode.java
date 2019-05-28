package joinTree;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.sql.SQLContext;
import stats.DatabaseStatistics;
import stats.PropertyStatistics;
import utils.Utils;

/**
 * A node of the JoinTree that refers to the Vertical Partitioning. Does not support patterns containing a variable
 * predicate.
 */
public class VPNode extends Node {
	private final TriplePattern triplePattern;

	public VPNode(final TriplePattern triplePattern, final DatabaseStatistics statistics) {
		super(statistics);
		this.triplePattern = triplePattern;
	}

	@Override
	public void computeNodeData(final SQLContext sqlContext) {
		if (triplePattern.predicateType.equals(ElementType.CONSTANT)) {
			assert statistics.getProperties().get(triplePattern.predicate) != null
					: "Property " + triplePattern.predicate + " not found in the statistics file";
			final String tableName = "vp_" + statistics.getProperties().get(triplePattern.predicate).getInternalName();
			sparkNodeData = sqlContext.sql(createSQLQuery(tableName));
		} else {
			for (final PropertyStatistics propertyStatistics : statistics.getProperties().values()) {
				final String tableName =
						"vp_" + propertyStatistics.getInternalName();
				if (sparkNodeData == null) {
					sparkNodeData = sqlContext.sql(createSQLQuery(tableName));
				} else {
					sparkNodeData = sparkNodeData.union(sqlContext.sql(createSQLQuery(tableName)));
				}
			}
		}
	}

	private String createSQLQuery(final String vpTableName) {
		final ArrayList<String> selectElements = new ArrayList<>();
		final ArrayList<String> whereElements = new ArrayList<>();

		if (triplePattern.subjectType == ElementType.VARIABLE) {
			selectElements.add("s AS " + Utils.removeQuestionMark(triplePattern.subject));
		} else {
			whereElements.add("s='" + triplePattern.subject + "'");
		}
		if (triplePattern.objectType == ElementType.VARIABLE) {
			selectElements.add("o AS " + Utils.removeQuestionMark(triplePattern.object));
		} else {
			whereElements.add("o='" + triplePattern.object + "'");
		}

		String query = "SELECT " + String.join(", ", selectElements);
		query += " FROM " + vpTableName;
		if (!whereElements.isEmpty()) {
			query += " WHERE " + String.join(" AND ", whereElements);
		}
		return query;
	}

	@Override
	public String toString() {
		return "{VP node (" + this.getPriority() +"): " + triplePattern.toString() + " }";
	}

	@Override
	public List<TriplePattern> collectTriples() {
		final ArrayList<TriplePattern> patterns = new ArrayList<>();
		patterns.add(triplePattern);
		return patterns;
	}
}