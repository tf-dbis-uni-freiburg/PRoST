package loader;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

class LoaderTest {
	private static SparkSession spark;
	private static String INPUT_TEST_GRAPH = "test_dataset/100k.nt";
	private static long numberSubjects;
	private static long numberPredicates;

	/**
	 * loadTripleTable loads the particular input testing graph the real triple table loader cannot be tested in absence
	 * of hive support.
	 */
	static void loadTripleTable() {

		Dataset<Row> tripleTable = spark.read().option("delimiter", "\t").csv(INPUT_TEST_GRAPH);
		tripleTable = tripleTable.selectExpr("_c0 AS s", "_c1 AS p", "_c2 AS o");
		tripleTable.write().mode("overwrite").saveAsTable("tripletable");
	}

	@BeforeAll
	static void initAll() {
		spark = SparkSession.builder().master("local[*]").appName("PRoST-Tester").getOrCreate();
		spark.sparkContext().setLogLevel("ERROR");
		spark.sql("DROP DATABASE IF EXISTS testingDB CASCADE");
		spark.sql("CREATE DATABASE testingDB");
		spark.sql("USE testingDB");
		loadTripleTable();

		numberSubjects = spark.sql("SELECT DISTINCT s FROM tripletable").count();
		numberPredicates = spark.sql("SELECT DISTINCT p FROM tripletable").count();
		assert (numberPredicates > 0 && numberSubjects > 0);
	}

	@Test
	void propertyTableTest() {
		final WidePropertyTableLoader pt_loader = new WidePropertyTableLoader("", "testingDB", spark, false);
		pt_loader.load();
		final Dataset<Row> propertyTable = spark.sql("SELECT * FROM property_table");

		// there should be a row for each distinct subject
		final long ptNumberRows = propertyTable.count();
		assert (ptNumberRows == numberSubjects);

		// there should be a column for each distinct property (except the subject column)
		final long ptNumberColumns = propertyTable.columns().length - 1;
		assertEquals(numberPredicates, ptNumberColumns, "Number of columns must be " + numberPredicates);
	}

	@Test
	void verticalPartitioningTest() {
		final VerticalPartitioningLoader vp_loader = new VerticalPartitioningLoader("", "testingDB", spark, false);
		vp_loader.load();
		final Dataset<Row> tables_list = spark.sql("SHOW tables");

		// there should be at least a table for each distinct property
		final long tables_count = tables_list.count();
		assert (tables_count > numberPredicates);
	}

}