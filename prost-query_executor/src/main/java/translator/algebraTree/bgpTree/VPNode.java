package translator.algebraTree.bgpTree;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.spark.sql.SQLContext;
import statistics.DatabaseStatistics;
import statistics.PropertyStatistics;
import utils.Settings;
import utils.Utils;

/**
 * A node of the bgpTree that refers to the Vertical Partitioning. Does not support patterns containing a variable
 * predicate.
 */
public class VPNode extends BgpNode {
	private final TriplePattern triplePattern;

	public VPNode(final TriplePattern triplePattern, final DatabaseStatistics statistics, final Settings settings) {
		super(statistics, settings);
		this.triplePattern = triplePattern;
	}

	@Override
	public void computeNodeData(final SQLContext sqlContext) {
		if (triplePattern.getPredicateType().equals(ElementType.CONSTANT)) {
			assert this.getStatistics().getProperties().get(triplePattern.getPredicate()) != null
					: "Property " + triplePattern.getPredicate() + " not found in the statistics file";
			final String tableName =
					"vp_" + this.getStatistics().getProperties().get(triplePattern.getPredicate()).getInternalName();
			this.setSparkNodeData(sqlContext.sql(createSQLQuery(tableName)));
		} else {
			for (final Map.Entry<String, PropertyStatistics> entry : this.getStatistics().getProperties().entrySet()) {
				final PropertyStatistics propertyStatistics = entry.getValue();
				final String tableName =
						"vp_" + propertyStatistics.getInternalName();
				if (this.getSparkNodeData() == null) {
					this.setSparkNodeData(sqlContext.sql(createSQLQuery(tableName, entry.getKey())));
				} else {
					this.setSparkNodeData(this.getSparkNodeData()
							.union(sqlContext.sql(createSQLQuery(tableName, entry.getKey()))));
				}
			}
		}
	}

	private String createSQLQuery(final String vpTableName) {
		return createSQLQuery(vpTableName, null);
	}

	private String createSQLQuery(final String vpTableName, final String vpPropertyName) {
		final ArrayList<String> selectElements = new ArrayList<>();
		final ArrayList<String> whereElements = new ArrayList<>();

		if (triplePattern.getSubjectType() == ElementType.VARIABLE) {
			selectElements.add("s AS " + Utils.removeQuestionMark(triplePattern.getSubject()));
		} else {
			whereElements.add("s='" + triplePattern.getSubject() + "'");
		}

		if (triplePattern.getPredicateType() == ElementType.VARIABLE) {
			assert vpPropertyName != null;
			selectElements.add("'" + vpPropertyName + "' AS "
					+ Utils.removeQuestionMark((triplePattern.getPredicate())));
		}

		if (triplePattern.getObjectType() == ElementType.VARIABLE) {
			selectElements.add("o AS " + Utils.removeQuestionMark(triplePattern.getObject()));
		} else {
			whereElements.add("o='" + triplePattern.getObject() + "'");
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
		return "{VP node (" + this.getPriority() + "): " + triplePattern.toString() + " }";
	}

	@Override
	public List<TriplePattern> collectTriples() {
		final ArrayList<TriplePattern> patterns = new ArrayList<>();
		patterns.add(triplePattern);
		return patterns;
	}
}