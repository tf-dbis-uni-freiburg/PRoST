package joinTree;

import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;
import org.apache.spark.sql.SQLContext;
import utils.Utils;

/*
 * A node of the JoinTree that refers to the Vertical Partitioning.
 */
public class VPNode extends Node {

	private static final Logger logger = Logger.getLogger("PRoST");
	public TriplePattern triplePattern;
	private final String tableName;


	/*
	 * The node contains a single triple pattern.
	 */
	public VPNode(final TriplePattern triplePattern, final String tableName) {
		super();
		this.tableName = tableName;
		this.triplePattern = triplePattern;
	}

	/*
	 * The node contains a single triple pattern.
	 */
	public VPNode(final Node parent, final TriplePattern triplePattern, final String tableName) {
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
			query.append("s AS ").append(Utils.removeQuestionMark(triplePattern.subject)).append(", o AS ").append(Utils.removeQuestionMark(triplePattern.object)).append(" ");
		} else if (triplePattern.subjectType == ElementType.VARIABLE) {
			query.append("s AS ").append(Utils.removeQuestionMark(triplePattern.subject));
		} else if (triplePattern.objectType == ElementType.VARIABLE) {
			query.append("o AS ").append(Utils.removeQuestionMark(triplePattern.object));
		}

		// FROM
		query.append(" FROM ");
		// when partition by subject
		//query.append("par_vp_" + tableName);
		query.append("vp_").append(tableName);

		// WHERE
		if (triplePattern.objectType == ElementType.CONSTANT || triplePattern.subjectType == ElementType.CONSTANT) {
			query.append(" WHERE ");
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
		final String str = "{" + "VP node: "
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