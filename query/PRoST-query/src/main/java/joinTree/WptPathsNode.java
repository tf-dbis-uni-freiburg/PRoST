package joinTree;

import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.shared.PrefixMapping;
import executor.Utils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import translator.Stats;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class WptPathsNode extends Node {

    public WptPathsNode(final Triple jenaTriple, final PrefixMapping prefixes) {
        final ArrayList<TriplePattern> triplePatterns = new ArrayList<>();
        tripleGroup = triplePatterns;
        children = new ArrayList<>();
        projection = Collections.emptyList();
        triplePatterns.add(new TriplePattern(jenaTriple, prefixes));
    }

    @Override
    public void computeNodeData(final SQLContext sqlContext) {
        final ArrayList<String> selectVariables = new ArrayList<>();
        final ArrayList<String> whereConditions = new ArrayList<>();
        final ArrayList<String> explodedColumns = new ArrayList<>();

        TriplePattern triple = tripleGroup.get(0);

        for (String column: Stats.getInstance().getTableNames()){
            boolean isComplex = Stats.getInstance().getTableSize(column) != Stats.getInstance().getTableDistinctSubjects(column);

            //s= constant
            whereConditions.add("s='" + triple.subject + "'");

            //p= variable
            selectVariables.add(column + " as " + Utils.removeQuestionMark(tripleGroup.get(0).subject));

            //o = variable
            if (isComplex) {
                selectVariables.add("P" + column + " AS " + Utils.removeQuestionMark(triple.object));
                explodedColumns.add(column);
            } else {
                selectVariables.add(column + " AS " + Utils.removeQuestionMark(triple.object));
                whereConditions.add(column + " IS NOT NULL");
            }

            StringBuilder query = new StringBuilder("SELECT ");
            query.append(String.join(", ", selectVariables));
            query.append(" FROM wide_property_table ");
            for (final String explodedColumn : explodedColumns) {
                query.append("\n lateral view explode(" + explodedColumn + ") exploded" + explodedColumn + " AS P"
                        + explodedColumn);
            }
            query.append(" WHERE ");
            query.append(String.join(" AND ", whereConditions));

             Dataset<Row> data = sqlContext.sql(query.toString());
             if (sparkNodeData==null){
                 sparkNodeData = data;
             } else {
                 sparkNodeData=sparkNodeData.union(data);
             }
        }
    }
}
