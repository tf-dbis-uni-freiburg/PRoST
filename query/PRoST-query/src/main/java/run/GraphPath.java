package run;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;
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

        String path = "r0   p0  r1";
        String databaseName = "mag";
        String statsFileName = "mag.stats";
        String outputFile = "mag_paths";

        Stats.getInstance().parseStats(statsFileName);


        SparkSession spark;
        SQLContext sqlContext;

        spark = SparkSession.builder().appName("PRoST-GraphPath").getOrCreate();
        sqlContext = spark.sqlContext();
        sqlContext.sql("USE " + databaseName);

        //get source

        String source = "r1";

        Dataset<Row> paths = computeHops(sqlContext,source);

        paths.write().parquet(outputFile);
    }

    private static Dataset<Row> computeHops(final SQLContext sqlContext, String source) {
        final ArrayList<String> selectVariables = new ArrayList<>();
        final ArrayList<String> whereConditions = new ArrayList<>();
        final ArrayList<String> explodedColumns = new ArrayList<>();

        Dataset<Row> result = null;

        logger.info("COMPUTING HOPS FROM " + source);

        for (String column : Stats.getInstance().getTableNames()) {
            boolean isComplex = Stats.getInstance().getTableSize(column) != Stats.getInstance().getTableDistinctSubjects(column);

            //s= constant
            whereConditions.add("s='" + source + "'");

            //p= variable
            selectVariables.add(column + " as p ");

            //o = variable
            if (isComplex) {
                selectVariables.add("P" + column + " AS o");
                explodedColumns.add(column);
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

            logger.info(query);

            Dataset<Row> data = sqlContext.sql(query.toString());
            if (result == null) {
                result = data;
            } else {
                result = result.union(data);
            }
        }
        logger.info("COMPUTED HOPS FROM " + source);
        return result;
    }
}
