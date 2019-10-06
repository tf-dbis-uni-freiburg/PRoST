package translator.algebraTree.bgpTree;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.shared.PrefixMapping;
import org.apache.spark.sql.SQLContext;
import statistics.DatabaseStatistics;
import statistics.PropertyStatistics;
import utils.Settings;
import utils.Utils;

/**
 * A node that uses an Inverse Wide Property Table.
 */
public class IWPTNode extends MVNode {
	private static final String OBJECT_COLUMN_NAME = "o";
	private static final String TABLE_NAME = DataModel.IWPT.getTableName();

	/**
	 * Alternative constructor, used to instantiate a Node directly with a list of
	 * jena triple patterns with the same object.
	 *
	 * @param jenaTriples list of Triples referring to the same object.
	 * @param prefixes    prefix mapping of the properties.
	 */
	public IWPTNode(final List<Triple> jenaTriples, final PrefixMapping prefixes, final DatabaseStatistics statistics,
					final Settings settings) {
		super(statistics, settings);
		final ArrayList<TriplePattern> triplePatterns = new ArrayList<>();
		for (final Triple t : jenaTriples) {
			triplePatterns.add(new TriplePattern(t, prefixes));
		}
		this.setTripleGroup(triplePatterns);
	}

	/*
	 * (non-Javadoc)
	 *
	 * @see JoinTree.Node#computeNodeData(org.apache.spark.sql.SQLContext)
	 */
	@Override
	public void computeNodeData(final SQLContext sqlContext) {
		if (this.size() == 1 && this.getFirstTriplePattern().getPredicateType().equals(ElementType.VARIABLE)) {
			computeVariablePredicateNodeData(sqlContext);
		} else {
			computeConstantPredicateNodeData(sqlContext);
		}
	}

	private void computeConstantPredicateNodeData(final SQLContext sqlContext) {
		final ArrayList<String> whereElements = new ArrayList<>();
		final ArrayList<String> selectElements = new ArrayList<>();
		final ArrayList<String> explodedElements = new ArrayList<>();

		if (this.getFirstTriplePattern().getObjectType() == ElementType.VARIABLE) {
			selectElements.add(OBJECT_COLUMN_NAME + " AS "
					+ Utils.removeQuestionMark(this.getFirstTriplePattern().getObject()));
		} else {
			whereElements.add(OBJECT_COLUMN_NAME + "='" + this.getFirstTriplePattern().getObject() + "'");
		}

		//A unique id for exploded columns. Necessary for BGB with multiple patterns that explode the same column.
		int explodedColumnId = 0;
		for (final TriplePattern t : this.getTripleGroup()) {
			final String columnName = this.getStatistics().getProperties().get(t.getPredicate()).getInternalName();
			if (columnName == null) {
				System.err.println("This column does not exists: " + t.getPredicate());
				return;
			}
			if (t.getSubjectType() == ElementType.CONSTANT) {
				if (t.isInverseComplex(getStatistics(), t.getPredicate())) {
					whereElements.add("array_contains(" + columnName + ", '" + t.getSubject() + "')");
				} else {
					whereElements.add(columnName + "='" + t.getSubject() + "'");
				}
			} else if (t.isInverseComplex(getStatistics(), t.getPredicate())) {
				selectElements.add("P" + columnName + "_" + explodedColumnId + " AS " + Utils.removeQuestionMark(t.getSubject()));
				explodedElements.add("\n lateral view explode(" + columnName + ") exploded" + columnName
						+ " AS P" + columnName + "_" + explodedColumnId);
				explodedColumnId++;
			} else {
				selectElements.add(columnName + " AS " + Utils.removeQuestionMark(t.getSubject()));
				whereElements.add(columnName + " IS NOT NULL");
			}
		}

		String query = "SELECT " + String.join(", ", selectElements);
		query += " FROM " + TABLE_NAME;
		if (!explodedElements.isEmpty()) {
			query += " " + String.join(" ", explodedElements);
		}
		if (!whereElements.isEmpty()) {
			query += " WHERE " + String.join(" AND ", whereElements);
		}

		this.setSparkNodeData(sqlContext.sql(query));
	}

	private void computeVariablePredicateNodeData(final SQLContext sqlContext) {
		final TriplePattern triple = this.getFirstTriplePattern();

		for (final Map.Entry<String, PropertyStatistics> entry : this.getStatistics().getProperties().entrySet()) {
			final String property = entry.getValue().getInternalName();
			final String key = entry.getKey();

			final ArrayList<String> selectElements = new ArrayList<>();
			final ArrayList<String> whereElements = new ArrayList<>();
			final ArrayList<String> explodedElements = new ArrayList<>();

			if (triple.getObjectType() == ElementType.VARIABLE) {
				selectElements.add(OBJECT_COLUMN_NAME + " AS "
						+ Utils.removeQuestionMark(this.getFirstTriplePattern().getObject()));
			} else {
				whereElements.add(OBJECT_COLUMN_NAME + "='" + triple.getObject() + "'");
			}

			selectElements.add("'" + key + "' as " + Utils.removeQuestionMark(triple.getPredicate()));

			if (triple.getSubjectType() == ElementType.CONSTANT) {
				if (triple.isInverseComplex(getStatistics(), key)) {
					whereElements.add("array_contains(" + property + ", '" + triple.getSubject() + "')");
				} else {
					whereElements.add(property + "='" + triple.getSubject() + "'");
				}
			} else if (triple.isInverseComplex(getStatistics(), key)) {
				selectElements.add("P" + property + " AS " + Utils.removeQuestionMark(triple.getSubject()));
				explodedElements.add("\n lateral view explode(" + property + ") exploded" + property
						+ " AS P" + property);
			} else {
				selectElements.add(property + " AS " + Utils.removeQuestionMark(triple.getSubject()));
				whereElements.add(property + " IS NOT NULL");
			}

			String query = "SELECT " + String.join(", ", selectElements);
			query += " FROM " + TABLE_NAME;
			if (!explodedElements.isEmpty()) {
				query += " " + String.join(" ", explodedElements);
			}
			if (!whereElements.isEmpty()) {
				query += " WHERE " + String.join(" AND ", whereElements);
			}

			if (this.getSparkNodeData() == null) {
				this.setSparkNodeData(sqlContext.sql(query));
			} else {
				this.setSparkNodeData(this.getSparkNodeData().union(sqlContext.sql(query)));
			}
		}
	}

	@Override
	public String toString() {
		final StringBuilder str = new StringBuilder("{");
		str.append("IWPT node(").append(this.getPriority()).append(": ");
		for (final TriplePattern tpGroup : this.getTripleGroup()) {
			str.append(tpGroup.toString()).append(", ");
		}
		str.append(" }");
		return str.toString();
	}
}
