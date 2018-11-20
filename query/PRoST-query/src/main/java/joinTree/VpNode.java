package joinTree;

import java.util.ArrayList;
import java.util.List;
import org.apache.spark.sql.SQLContext;
import org.apache.log4j.Logger;

import utils.Utils;
/*
 * A node of the JoinTree that refers to the Vertical Partitioning.
 */
public class VpNode extends Node {

	private static final Logger logger = Logger.getLogger("PRoST");
	
	private final String tableName;
	public TriplePattern triplePattern;

	/*
	 * The node contains a single triple pattern.
	 */
	public VpNode(final TriplePattern triplePattern, final String tableName) {
		super();
		this.tableName = tableName;
		this.triplePattern = triplePattern;
	}

	/*
	 * The node contains a single triple pattern.
	 */
	public VpNode(Node parent, final TriplePattern triplePattern, final String tableName) {
		super(parent);
		this.tableName = tableName;
		this.triplePattern = triplePattern;
	}

	@Override
	public void computeNodeData(final SQLContext sqlContext) {

		if (tableName == null) {
			System.err.println("The predicate does not have a VP table: " + triplePattern.predicate);
			return;
		}

		final StringBuilder query = new StringBuilder("SELECT ");

		// SELECT
		if (triplePattern.subjectType == ElementType.VARIABLE && triplePattern.objectType == ElementType.VARIABLE) {
			query.append("s AS " + Utils.removeQuestionMark(triplePattern.subject) + ", o AS "
					+ Utils.removeQuestionMark(triplePattern.object) + " ");
		} else if (triplePattern.subjectType == ElementType.VARIABLE) {
			query.append("s AS " + Utils.removeQuestionMark(triplePattern.subject));
		} else if (triplePattern.objectType == ElementType.VARIABLE) {
			query.append("o AS " + Utils.removeQuestionMark(triplePattern.object));
		}

		// FROM
		query.append(" FROM ");
		query.append("vp_" + tableName);

		// WHERE
		if (triplePattern.objectType == ElementType.CONSTANT || triplePattern.subjectType == ElementType.CONSTANT) {
			query.append(" WHERE ");
		}
		if (triplePattern.objectType == ElementType.CONSTANT) {
			query.append(" o='" + triplePattern.object + "' ");
		}

		if (triplePattern.subjectType == ElementType.CONSTANT) {
			query.append(" s='" + triplePattern.subject + "' ");
		}
		sparkNodeData = sqlContext.sql(query.toString());
	}

	@Override
	public String toString() {
		final StringBuilder str = new StringBuilder("{");
		str.append("VP node: ");
		str.append(triplePattern.toString());
		str.append(" }");
		return str.toString();
	}

	@Override
	public List<TriplePattern> collectTriples() {
		ArrayList<TriplePattern> patterns = new ArrayList<>();
		patterns.add(triplePattern);
		return patterns;
	}
}