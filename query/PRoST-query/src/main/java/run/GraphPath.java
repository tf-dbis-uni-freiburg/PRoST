package run;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.apache.spark.sql.*;
import translator.Stats;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Properties;

public class GraphPath {

    private static final Logger logger = Logger.getLogger("PRoST");

    public static void main(final String[] args) throws IOException {
        final InputStream inStream = Main.class.getClassLoader().getResourceAsStream("log4j.properties");
        final Properties props = new Properties();
        props.load(inStream);
        PropertyConfigurator.configure(props);

        final String path = "r0 p0  r1";
        final String databaseName = "mag";
        final String statsFileName = "mag.stats";
        final String outputFile = "mag_paths";
        final String sourcesOutputFile = "sources";

        Stats.getInstance().parseStats(statsFileName);

        SparkSession spark;
        SQLContext sqlContext;

        spark = SparkSession.builder().appName("PRoST-GraphPath").getOrCreate();
        sqlContext = spark.sqlContext();
        sqlContext.sql("USE " + databaseName);

        //get source

        String source = "<http://ma-graph.org/entity/100000009>";

        //Dataset<Row> paths = computeHops(sqlContext,source);

        //paths.write().parquet(outputFile);

        //computeSources(sqlContext).write().parquet(sourcesOutputFile);

        partitionTable(sqlContext, "wide_property_table", "wide_property_table_p", "s");
    }

    private static Dataset<Row> computeHops(final SQLContext sqlContext, String source) {
        Dataset<Row> result = null;

        logger.info("COMPUTING HOPS FROM " + source);

        for (String column : Stats.getInstance().getTableNames()) {
            boolean isComplex = Stats.getInstance().getTableSize(column) != Stats.getInstance().getTableDistinctSubjects(column);

            final ArrayList<String> selectVariables = new ArrayList<>();
            final ArrayList<String> whereConditions = new ArrayList<>();
            final ArrayList<String> explodedColumns = new ArrayList<>();

            //s= constant
            whereConditions.add("s='" + source + "'");

            //p= variable

            //TODO change column to the String instead of array of Strings
            selectVariables.add("'"+source+"' as s ");
            selectVariables.add("'"+column+"' as p ");

            //o = variable
            if (isComplex) {
                selectVariables.add("P" + column + " AS o");
                explodedColumns.add(column);
                //whereConditions.add("P" + column + " IS NOT NULL");
            } else {
                selectVariables.add(column + " AS o");
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

            //logger.info(query);

            Dataset<Row> data = sqlContext.sql(query.toString());
            if (result == null) {
                result = data;
            } else{
                result = result.union(data);
            }
        }
        logger.info("COMPUTED HOPS FROM " + source);
        return result;
    }

    private static Dataset<Row> computeSources(final SQLContext sqlContext) {
        String query = "SELECT DISTINCT s FROM wide_property_table";
        Dataset<Row> data = sqlContext.sql(query);
        return data;
    }

    private static void partitionTable(final SQLContext sqlContext, String originalTableName, String partitionedTableName, String columnName){
        Dataset<Row> data = sqlContext.sql("Select * FROM " + originalTableName);
        logger.info("saving partitioned table");
        data.write().mode(SaveMode.Overwrite).partitionBy(columnName)
                .format("parquet").saveAsTable(partitionedTableName);
        logger.info("DONE!");
    }

    private static void repartitionTable(final SQLContext sqlContext, String originalTableName, String partitionedTableName, String columnName){
        Dataset<Row> data = sqlContext.sql("Select * FROM " + originalTableName);
        logger.info("saving partitioned table");
        data = data.repartition(data.col(columnName));
        data.write().mode(SaveMode.Overwrite).format("parquet").saveAsTable(partitionedTableName);
        logger.info("DONE!");
    }
}
