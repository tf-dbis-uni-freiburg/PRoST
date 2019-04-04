package joinTree;

import java.util.Collections;

//import org.apache.log4j.Logger;
import extVp.DatabaseStatistics;
import org.apache.spark.sql.SQLContext;

import executor.Utils;

/**
 * Join tree node that uses or create a semi-join table.
 */
public class ExtVpNode extends Node {
	private final String tableName;
	private final String tableNameWithDatabaseIdentifier; // databaseName.tableName
	public DatabaseStatistics extVPDatabaseStatistic;

	public ExtVpNode(final TriplePattern triplePattern, final String tableName, final String databaseName, DatabaseStatistics extvpStats) {
		super();
		this.triplePattern = triplePattern;
		tripleGroup = Collections.emptyList();
		this.tableName = tableName;
		tableNameWithDatabaseIdentifier = databaseName + "." + tableName;
		this.extVPDatabaseStatistic = extvpStats;
	}

	@Override
	public void computeNodeData(final SQLContext sqlContext) {
		final TriplePattern mainPattern = triplePattern;

		final StringBuilder query = new StringBuilder("Select distinct ");
		if (mainPattern.subjectType == ElementType.VARIABLE && mainPattern.objectType == ElementType.VARIABLE) {
			query.append("s as " + Utils.removeQuestionMark(mainPattern.subject) + ", o as "
					+ Utils.removeQuestionMark(mainPattern.object) + " ");
		} else if (mainPattern.subjectType == ElementType.VARIABLE) {
			query.append("s AS " + Utils.removeQuestionMark(mainPattern.subject));
		} else if (mainPattern.objectType == ElementType.VARIABLE) {
			query.append("o AS " + Utils.removeQuestionMark(mainPattern.object));
		}

		query.append(" from " + tableNameWithDatabaseIdentifier);

		if (mainPattern.objectType == ElementType.CONSTANT || mainPattern.subjectType == ElementType.CONSTANT) {
			query.append(" where ");
			if (mainPattern.objectType == ElementType.CONSTANT && mainPattern.subjectType == ElementType.CONSTANT) {
				query.append(" s='<" + mainPattern.subject + ">' and o='<" + mainPattern.object + ">'");
			} else if (mainPattern.subjectType == ElementType.CONSTANT) {
				query.append(" s='<" + mainPattern.subject + ">' ");
			} else {
				query.append(" o='<" + mainPattern.object + ">' ");
			}
		}

		sparkNodeData = sqlContext.sql(query.toString());
	}

	@Override
	public String toString() {
		final StringBuilder str = new StringBuilder("{");
		str.append("- " + tableName + "- ");
		str.append(triplePattern.toString());
		str.append(" }");
		str.append(" [");
		for (final Node child : children) {
			str.append("\n" + child.toString());
		}
		str.append("\n]");
		return str.toString();
	}

	public String getTableName(){
		return this.tableName;
	}
}
