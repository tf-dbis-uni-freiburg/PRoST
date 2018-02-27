package loader;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.*;

import static org.junit.jupiter.api.Assertions.assertEquals;

class PropertyTableLoaderTest {
    private static SparkSession spark;
    private static String INPUT_TEST_GRAPH = "test_dataset/100k.nt";
    private static long numberSubjects;
    private static long numberPredicates;

    /**
     * loadTripleTable loads the particular input testing graph
     * the real triple table loader cannot be tested in absence of hive support.
     */
    static void loadTripleTable() {

        Dataset<Row> tripleTable = spark.read().option("delimiter", "\t").csv(INPUT_TEST_GRAPH);
        tripleTable = tripleTable.selectExpr("_c0 AS s","_c1 AS p", "_c2 AS o");
        tripleTable.write().mode("overwrite").saveAsTable("tripletable");
    }

    @BeforeAll
    static void initAll() {
        spark = SparkSession
                .builder()
                .master("local[*]")
                .appName("PRoST-Tester")
                .getOrCreate();
        spark.sparkContext().setLogLevel("ERROR");
        spark.sql("DROP DATABASE IF EXISTS testingDB CASCADE");
        spark.sql("CREATE DATABASE testingDB");
        spark.sql("USE testingDB");
        loadTripleTable();

        numberSubjects = spark.sql("SELECT DISTINCT s FROM tripletable").count();
        numberPredicates = spark.sql("SELECT DISTINCT p FROM tripletable").count();
        assert(numberPredicates > 0 && numberSubjects > 0);
    }


    @Test
    void propertyTableTest() {
        PropertyTableLoader pt_loader = new PropertyTableLoader("", "testingDB", spark);
        pt_loader.load();
        Dataset<Row> propertyTable = spark.sql("SELECT * FROM property_table");

        // there should be a row for each distinct subject
        long ptNumberRows = propertyTable.count();
        assert(ptNumberRows == numberSubjects);

        // there should be a column for each distinct property (except the subject column)
        long ptNumberColumns = propertyTable.columns().length - 1;
        assertEquals(numberPredicates, ptNumberColumns, "Number of columns must be " + numberPredicates);
    }

}