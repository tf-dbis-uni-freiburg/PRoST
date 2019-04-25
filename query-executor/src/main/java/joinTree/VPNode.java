package joinTree;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.sql.SQLContext;
import stats.DatabaseStatistics;
import utils.Utils;

/**
 * A node of the JoinTree that refers to the Vertical Partitioning. Does not support patterns containing a variable
 * predicate.
 */
public class VPNode extends Node {
	private final TriplePattern triplePattern;
	private final String tableName;

	public VPNode(final TriplePattern triplePattern, final String tableName, final DatabaseStatistics statistics) {
		super(statistics);
		//TODO tableName should be retrieved from the statistics file based on the triple pattern predicate instead
		// of being given by an argument
		this.tableName = tableName;
		this.triplePattern = triplePattern;
		if (triplePattern.predicateType == ElementType.VARIABLE) {
			System.err.println("Vertical partitioning nodes do not support patterns with variable predicates");
		}
	}

	@Override
	public void computeNodeData(final SQLContext sqlContext) {
		if (tableName == null) {
			System.err.println("The predicate does not have a VP table: " + triplePattern.predicate);
			return;
		}

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
		// TODO variable tableName is not really the table name if 'vp_' still needs to be attached to it. Fix all
		//  calls to VPNode to give the actual tableName
		query += " FROM vp_" + tableName;
		if (!whereElements.isEmpty()) {
			query += " WHERE " + String.join(" AND ", whereElements);
		}
		sparkNodeData = sqlContext.sql(query);
	}

	@Override
	public String toString() {
		return "{VP node: " + triplePattern.toString() + " }";
	}

	@Override
	public List<TriplePattern> collectTriples() {
		final ArrayList<TriplePattern> patterns = new ArrayList<>();
		patterns.add(triplePattern);
		return patterns;
	}
}