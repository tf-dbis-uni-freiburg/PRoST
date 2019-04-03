package joinTree;

import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;
import org.apache.spark.sql.SQLContext;
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
	public TTNode(final TriplePattern triplePattern) {
		super();
		this.triplePattern = triplePattern;
	}

	/*
	 * The node contains a single triple pattern.
	 */
	public TTNode(final Node parent, final TriplePattern triplePattern, final String tableName) {
		super(parent);
		this.triplePattern = triplePattern;
	}

	@Override
	public void computeNodeData(final SQLContext sqlContext) {

		final StringBuilder query = new StringBuilder("SELECT ");

		// SELECT
		if (triplePattern.subjectType == ElementType.VARIABLE && triplePattern.objectType == ElementType.VARIABLE) {
			query.append("s AS ").append(Utils.removeQuestionMark(triplePattern.subject)).append(", o AS ").append(Utils.removeQuestionMark(triplePattern.object)).append(" ");
		} else if (triplePattern.subjectType == ElementType.VARIABLE) {
			query.append("s AS ").append(Utils.removeQuestionMark(triplePattern.subject));
		} else if (triplePattern.objectType == ElementType.VARIABLE) {
			query.append("o AS ").append(Utils.removeQuestionMark(triplePattern.object));
		}

		// FROM
		query.append(" FROM ");
		query.append("tripletable");
		//query.append("par_tripletable");
		query.append(" WHERE ");
		query.append(" p='<").append(triplePattern.predicate).append(">' ");
		// WHERE
		if (triplePattern.objectType == ElementType.CONSTANT || triplePattern.subjectType == ElementType.CONSTANT) {
			query.append(" AND ");
		}
		if (triplePattern.objectType == ElementType.CONSTANT) {
			query.append(" o='").append(triplePattern.object).append("' ");
		}

		if (triplePattern.subjectType == ElementType.CONSTANT) {
			query.append(" s='").append(triplePattern.subject).append("' ");
		}
		sparkNodeData = sqlContext.sql(query.toString());
	}

	@Override
	public String toString() {
		final String str = "{" + "TT node: "
				+ triplePattern.toString()
				+ " }";
		return str;
	}

	@Override
	public List<TriplePattern> collectTriples() {
		final ArrayList<TriplePattern> patterns = new ArrayList<>();
		patterns.add(triplePattern);
		return patterns;
	}
}