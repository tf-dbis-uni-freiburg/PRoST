package translator.algebraTree.bgpTree;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.sql.SQLContext;
import statistics.DatabaseStatistics;
import utils.Settings;
import utils.Utils;

/**
 * A node of the bgpTree that refers to the Triple Table.
 */
public class TTNode extends BgpNode {
	private TriplePattern triplePattern;

	/*
	 * The node contains a single triple pattern.
	 */
	public TTNode(final TriplePattern triplePattern, final DatabaseStatistics statistics, final Settings settings) {
		super(statistics, settings);
		this.triplePattern = triplePattern;
	}

	@Override
	public void computeNodeData(final SQLContext sqlContext) {
		final ArrayList<String> selectElements = new ArrayList<>();
		final ArrayList<String> whereElements = new ArrayList<>();
		if (triplePattern.getSubjectType() == ElementType.VARIABLE) {
			selectElements.add("s AS " + Utils.removeQuestionMark(triplePattern.getSubject()));
		} else {
			whereElements.add("s='" + triplePattern.getSubject() + "'");
		}
		if (triplePattern.getPredicateType() == ElementType.VARIABLE) {
			selectElements.add("p AS " + Utils.removeQuestionMark(triplePattern.getPredicate()));
		} else {
			whereElements.add("p='" + triplePattern.getPredicate() + "'");
		}
		if (triplePattern.getObjectType() == ElementType.VARIABLE) {
			selectElements.add("o AS " + Utils.removeQuestionMark(triplePattern.getObject()));
		} else {
			whereElements.add("o='" + triplePattern.getObject() + "'");
		}

		String query = "SELECT " + String.join(", ", selectElements);
		query += " FROM " + DataModel.TT.getTableName();
		if (!whereElements.isEmpty()) {
			query += " WHERE " + String.join(" AND ", whereElements);
		}
		this.setSparkNodeData(sqlContext.sql(query));
	}

	@Override
	public String toString() {
		return  "{TT node (" + this.getPriority() + "(: " + triplePattern.toString() + " }";
	}

	@Override
	public List<TriplePattern> collectTriples() {
		final ArrayList<TriplePattern> patterns = new ArrayList<>();
		patterns.add(triplePattern);
		return patterns;
	}
}