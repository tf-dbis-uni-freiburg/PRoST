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
 * A node of the bgpTree that refers to the Property Table.
 */
public class WPTNode extends MVNode {

	/**
	 * The default value is "wide_property_table" when only one PT exists. If an
	 * emergent schema is used, then there exist more than one property tables. Each
	 * of them contains a specific set of predicates. In this case, the default
	 * table name can be changes depending on the list of triples this node
	 * contains.
	 */
	private static String TABLE_NAME = DataModel.WPT.getTableName();

	/*
	 * Alternative constructor, used to instantiate a Node directly with a list of
	 * jena triple patterns.
	 */
	public WPTNode(final List<Triple> jenaTriples, final PrefixMapping prefixes, final DatabaseStatistics statistics,
				   final Settings settings) {
		super(statistics, settings);
		final ArrayList<TriplePattern> triplePatterns = new ArrayList<>();
		for (final Triple t : jenaTriples) {
			triplePatterns.add(new TriplePattern(t, prefixes));
		}
		this.setTripleGroup(triplePatterns);
	}

	@Override
	public void computeNodeData(final SQLContext sqlContext) {
		if (this.size() == 1 && this.getFirstTriplePattern().getPredicateType().equals(ElementType.VARIABLE)) {
			computeVariablePredicateNodeData(sqlContext);
		} else {
			computeConstantPredicateNodeData(sqlContext);
		}
	}

	private void computeConstantPredicateNodeData(final SQLContext sqlContext) {
		final ArrayList<String> selectElements = new ArrayList<>();
		final ArrayList<String> whereElements = new ArrayList<>();
		final ArrayList<String> explodedElements = new ArrayList<>();

		if (this.getFirstTriplePattern().getSubjectType() == ElementType.VARIABLE) {
			selectElements.add("s AS " + Utils.removeQuestionMark(this.getFirstTriplePattern().getSubject()));
		} else {
			whereElements.add("s='" + this.getFirstTriplePattern().getSubject() + "'");
		}
		//A unique id for exploded columns. Necessary for BGB with multiple patterns that explode the same column.
		int explodedColumnId = 0;
		for (final TriplePattern t : this.getTripleGroup()) {
			final String columnName = this.getStatistics().getProperties().get(t.getPredicate()).getInternalName();
			if (columnName == null) {
				System.err.println("This property does not exists: " + t.getPredicate());
				return;
			}
			if (t.getObjectType() == ElementType.CONSTANT) {
				if ((t.isComplex(getStatistics(), t.getPredicate()))) {
					whereElements.add("array_contains(" + columnName + ", '" + t.getObject() + "')");
				} else {
					whereElements.add(columnName + "='" + t.getObject() + "'");
				}
			} else if ((t.isComplex(getStatistics(), t.getPredicate()))) {
				selectElements.add("P" + columnName + "_" + explodedColumnId + " AS " + Utils.removeQuestionMark(t.getObject()));
				explodedElements.add("\nlateral view explode(" + columnName + ") exploded" + columnName
						+ " AS P" + columnName + "_" + explodedColumnId);
				explodedColumnId++;
			} else {
				selectElements.add(columnName + " AS " + Utils.removeQuestionMark(t.getObject()));
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

	//assumes a single pattern in the triples groups
	private void computeVariablePredicateNodeData(final SQLContext sqlContext) {
		final TriplePattern triple = this.getFirstTriplePattern();

		for (final Map.Entry<String, PropertyStatistics> entry : this.getStatistics().getProperties().entrySet()) {
			final String property = entry.getValue().getInternalName();
			final String key = entry.getKey();

			final ArrayList<String> selectElements = new ArrayList<>();
			final ArrayList<String> whereElements = new ArrayList<>();
			final ArrayList<String> explodedElements = new ArrayList<>();

			if (triple.getSubjectType() == ElementType.VARIABLE) {
				selectElements.add("s AS " + Utils.removeQuestionMark(triple.getSubject()));
			} else {
				whereElements.add("s='" + triple.getSubject() + "'");
			}

			selectElements.add("'" + key + "' AS " + Utils.removeQuestionMark(triple.getPredicate()));

			if (triple.getObjectType() == ElementType.CONSTANT) {
				if ((triple.isComplex(getStatistics(), key))) {
					whereElements.add("array_contains(" + property + ", '" + triple.getObject() + "')");
				} else {
					whereElements.add(property + "='" + triple.getObject() + "'");
				}
			} else if ((triple.isComplex(getStatistics(), key))) {
				selectElements.add("P" + property + " AS " + Utils.removeQuestionMark(triple.getObject()));
				explodedElements.add("\nlateral view explode(" + property + ") exploded" + property
						+ " AS P" + property);
			} else {
				selectElements.add(property + " AS " + Utils.removeQuestionMark(triple.getObject()));
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
		str.append("WPT node (").append(this.getPriority()).append("): ");
		for (final TriplePattern tpGroup : this.getTripleGroup()) {
			str.append(tpGroup.toString()).append(", ");
		}
		str.append(" }");
		return str.toString();
	}
}
