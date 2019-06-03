package joinTree;

import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;
import org.apache.spark.sql.SQLContext;
import stats.DatabaseStatistics;
import utils.Utils;

/*
 * A node of the JoinTree that refers to the Triple Table.
 */
public class TTNode extends Node {

	private static final Logger logger = Logger.getLogger("PRoST");

	public TriplePattern triplePattern;

	/*
	 * The node contains a single triple pattern.
	 */
	public TTNode(final TriplePattern triplePattern, final DatabaseStatistics statistics) {
		super(statistics);
		this.triplePattern = triplePattern;
	}

	/*
	 * The node contains a single triple pattern.
	 */
	public TTNode(final Node parent, final TriplePattern triplePattern, final DatabaseStatistics statistics) {
		super(parent, statistics);
		this.triplePattern = triplePattern;
	}

	@Override
	public void computeNodeData(final SQLContext sqlContext) {
		final ArrayList<String> selectElements = new ArrayList<>();
		final ArrayList<String> whereElements = new ArrayList<>();
		if (triplePattern.subjectType == ElementType.VARIABLE) {
			selectElements.add("s AS " + Utils.removeQuestionMark(triplePattern.subject));
		} else {
			whereElements.add("s='" + triplePattern.subject + "'");
		}
		if (triplePattern.predicateType == ElementType.VARIABLE) {
			selectElements.add("p as " + Utils.removeQuestionMark(triplePattern.predicate));
		} else {
			whereElements.add("p='<" + triplePattern.predicate + ">'");
		}
		if (triplePattern.objectType == ElementType.VARIABLE) {
			selectElements.add("o AS " + Utils.removeQuestionMark(triplePattern.object));
		} else {
			whereElements.add("o='" + triplePattern.object + "'");
		}

		String query = "SELECT " + String.join(", ", selectElements);
		query += " FROM tripletable";
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