package query.run;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import com.holdenkarau.spark.testing.JavaDataFrameSuiteBase;
import loader.InverseWidePropertyTableLoader;
import loader.JoinedWidePropertyTableLoader;
import loader.VerticalPartitioningLoader;
import loader.WidePropertyTableLoader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.Ignore;
import org.junit.Test;
import org.spark_project.guava.collect.ImmutableList;
import query.utilities.TripleBean;
import statistics.DatabaseStatistics;
import translator.Query;
import utils.Settings;

/**
 * This class tests represents the highest level of testing, i.e. given a query it checks that results are correctly and
 * consistently returned according to ALL supported logical partitioning strategies (at the moment WPT, IWPT, JWPT, and
 * VP?), i.e. these tests verify are about SPARQL semantics.
 *
 * @author Kristin Plettau
 */
public class SumTest extends JavaDataFrameSuiteBase implements Serializable {
	private static final long serialVersionUID = 1329L;
	private static final Encoder<TripleBean> triplesEncoder = Encoders.bean(TripleBean.class);

	@Test
	@Ignore("Operation not yet implemented.")
	public void queryTest2() throws Exception {
		final DatabaseStatistics statistics = new DatabaseStatistics("queryTestSumTest1_db");
		initializeDb2(statistics);
		queryOnTT2(statistics);
		queryOnVp2(statistics);
		queryOnWpt2(statistics);
		queryOnIwpt2(statistics);
		queryOnJwptOuter2(statistics);
		queryOnJwptLeftOuter2(statistics);
	}

	private void queryOnTT2(final DatabaseStatistics statistics) throws Exception {
		final Settings settings = new Settings.Builder("queryTestSumTest1_db").usingTTNodes().usingCharacteristicSets().build();
		final ClassLoader classLoader = getClass().getClassLoader();
		final Query query = new Query(classLoader.getResource("queryTestSum1.q").getPath(), statistics, settings);

		//EXPECTED
		final StructType schema = DataTypes.createStructType(new StructField[]{
				DataTypes.createStructField("title", DataTypes.StringType, true),
				DataTypes.createStructField("sales", DataTypes.StringType, true),
		});
		final Row row1 = RowFactory.create("Title1", "6");
		final List<Row> rowList = ImmutableList.of(row1);
		final Dataset<Row> expectedResult = spark().createDataFrame(rowList, schema);

		//ACTUAL
		final Dataset<Row> actualResult = query.compute(spark().sqlContext()).orderBy("title", "sales");
		final Dataset<Row> nullableActualResult = sqlContext().createDataFrame(actualResult.collectAsList(),
				actualResult.schema().asNullable());
		System.out.print("SumTest: queryTest1");
		expectedResult.printSchema();
		expectedResult.show();

		nullableActualResult.printSchema();
		nullableActualResult.show();
		assertDataFrameEquals(expectedResult, nullableActualResult);
	}

	private void queryOnVp2(final DatabaseStatistics statistics) throws Exception {
		final Settings settings = new Settings.Builder("queryTestSumTest1_db").usingVPNodes().build();
		final ClassLoader classLoader = getClass().getClassLoader();

		final Query query = new Query(classLoader.getResource("queryTestSum1.q").getPath(), statistics, settings);

		//EXPECTED
		final StructType schema = DataTypes.createStructType(new StructField[]{
				DataTypes.createStructField("title", DataTypes.StringType, true),
				DataTypes.createStructField("sales", DataTypes.StringType, true),
		});
		final Row row1 = RowFactory.create("Title1", "6");
		final List<Row> rowList = ImmutableList.of(row1);
		final Dataset<Row> expectedResult = spark().createDataFrame(rowList, schema);

		//ACTUAL
		final Dataset<Row> actualResult = query.compute(spark().sqlContext()).orderBy("title", "sales");
		final Dataset<Row> nullableActualResult = sqlContext().createDataFrame(actualResult.collectAsList(),
				actualResult.schema().asNullable());

		assertDataFrameEquals(expectedResult, nullableActualResult);
	}

	private void queryOnWpt2(final DatabaseStatistics statistics) throws Exception {
		final Settings settings = new Settings.Builder("queryTestSumTest1_db").usingWPTNodes().build();
		final ClassLoader classLoader = getClass().getClassLoader();

		final Query query = new Query(classLoader.getResource("queryTestSum1.q").getPath(), statistics, settings);

		//EXPECTED
		final StructType schema = DataTypes.createStructType(new StructField[]{
				DataTypes.createStructField("title", DataTypes.StringType, true),
				DataTypes.createStructField("sales", DataTypes.StringType, true),
		});
		final Row row1 = RowFactory.create("Title1", "6");
		final List<Row> rowList = ImmutableList.of(row1);
		final Dataset<Row> expectedResult = spark().createDataFrame(rowList, schema);

		//ACTUAL
		final Dataset<Row> actualResult = query.compute(spark().sqlContext()).orderBy("title", "sales");
		final Dataset<Row> nullableActualResult = sqlContext().createDataFrame(actualResult.collectAsList(),
				actualResult.schema().asNullable());

		assertDataFrameEquals(expectedResult, nullableActualResult);
	}

	private void queryOnIwpt2(final DatabaseStatistics statistics) throws Exception {
		final Settings settings = new Settings.Builder("queryTestSumTest1_db").usingIWPTNodes().build();
		final ClassLoader classLoader = getClass().getClassLoader();

		final Query query = new Query(classLoader.getResource("queryTestSum1.q").getPath(), statistics, settings);

		//EXPECTED
		final StructType schema = DataTypes.createStructType(new StructField[]{
				DataTypes.createStructField("title", DataTypes.StringType, true),
				DataTypes.createStructField("sales", DataTypes.StringType, true),
		});
		final Row row1 = RowFactory.create("Title1", "6");
		final List<Row> rowList = ImmutableList.of(row1);
		final Dataset<Row> expectedResult = spark().createDataFrame(rowList, schema);

		//ACTUAL
		final Dataset<Row> actualResult = query.compute(spark().sqlContext()).orderBy("title", "sales");
		final Dataset<Row> nullableActualResult = sqlContext().createDataFrame(actualResult.collectAsList(),
				actualResult.schema().asNullable());

		assertDataFrameEquals(expectedResult, nullableActualResult);
	}

	private void queryOnJwptOuter2(final DatabaseStatistics statistics) throws Exception {
		final Settings settings = new Settings.Builder("queryTestSumTest1_db").usingJWPTOuterNodes().build();
		final ClassLoader classLoader = getClass().getClassLoader();

		final Query query = new Query(classLoader.getResource("queryTestSum1.q").getPath(), statistics, settings);

		//EXPECTED
		final StructType schema = DataTypes.createStructType(new StructField[]{
				DataTypes.createStructField("title", DataTypes.StringType, true),
				DataTypes.createStructField("sales", DataTypes.StringType, true),
		});
		final Row row1 = RowFactory.create("Title1", "6");
		final List<Row> rowList = ImmutableList.of(row1);
		final Dataset<Row> expectedResult = spark().createDataFrame(rowList, schema);

		//ACTUAL
		final Dataset<Row> actualResult = query.compute(spark().sqlContext()).orderBy("title", "sales");
		final Dataset<Row> nullableActualResult = sqlContext().createDataFrame(actualResult.collectAsList(),
				actualResult.schema().asNullable());

		assertDataFrameEquals(expectedResult, nullableActualResult);
	}

	private void queryOnJwptLeftOuter2(final DatabaseStatistics statistics) throws Exception {
		final Settings settings = new Settings.Builder("queryTestSumTest1_db").usingJWPTLeftouterNodes().build();
		final ClassLoader classLoader = getClass().getClassLoader();

		final Query query = new Query(classLoader.getResource("queryTestSum1.q").getPath(), statistics, settings);

		//EXPECTED
		final StructType schema = DataTypes.createStructType(new StructField[]{
				DataTypes.createStructField("title", DataTypes.StringType, true),
				DataTypes.createStructField("sales", DataTypes.StringType, true),
		});
		final Row row1 = RowFactory.create("Title1", "6");
		final List<Row> rowList = ImmutableList.of(row1);
		final Dataset<Row> expectedResult = spark().createDataFrame(rowList, schema);

		//ACTUAL
		final Dataset<Row> actualResult = query.compute(spark().sqlContext()).orderBy("title", "sales");
		final Dataset<Row> nullableActualResult = sqlContext().createDataFrame(actualResult.collectAsList(),
				actualResult.schema().asNullable());

		assertDataFrameEquals(expectedResult, nullableActualResult);
	}

	private void initializeDb2(final DatabaseStatistics statistics) {
		spark().sql("DROP DATABASE IF EXISTS queryTestSumTest1_db CASCADE");
		spark().sql("CREATE DATABASE IF NOT EXISTS  queryTestSumTest1_db");
		spark().sql("USE queryTestSumTest1_db");

		// creates test tt table
		final TripleBean t1 = new TripleBean();
		t1.setS("<http://example.org/book1>");
		t1.setP("<http://example.org/title>");
		t1.setO("Title1");

		final TripleBean t2 = new TripleBean();
		t2.setS("<http://example.org/book1>");
		t2.setP("<http://example.org/sales>");
		t2.setO("1");

		final TripleBean t3 = new TripleBean();
		t3.setS("<http://example.org/book1>");
		t3.setP("<http://example.org/sales>");
		t3.setO("2");

		final TripleBean t4 = new TripleBean();
		t4.setS("<http://example.org/book1>");
		t4.setP("<http://example.org/sales>");
		t4.setO("3");

		final ArrayList<TripleBean> triplesList = new ArrayList<>();
		triplesList.add(t1);
		triplesList.add(t2);
		triplesList.add(t3);
		triplesList.add(t4);

		final Dataset<Row> ttDataset = spark().createDataset(triplesList, triplesEncoder).select("s", "p", "o").orderBy(
				"s", "p", "o");
		ttDataset.write().saveAsTable("tripletable");

		final loader.Settings loaderSettings =
				new loader.Settings.Builder("queryTestSumTest1_db").withInputPath((System.getProperty(
						"user.dir") + "\\target\\test_output\\SumTest").replace('\\', '/'))
						.generateVp().generateWpt().generateIwpt().generateJwptOuter()
						.generateJwptLeftOuter().generateJwptInner().build();

		final VerticalPartitioningLoader vpLoader = new VerticalPartitioningLoader(loaderSettings, spark(), statistics);
		vpLoader.load();

		statistics.computePropertyStatistics(spark());

		final WidePropertyTableLoader wptLoader = new WidePropertyTableLoader(loaderSettings, spark(), statistics);
		wptLoader.load();

		final InverseWidePropertyTableLoader iwptLoader = new InverseWidePropertyTableLoader(loaderSettings, spark(),
				statistics);
		iwptLoader.load();

		final JoinedWidePropertyTableLoader jwptOuterLoader = new JoinedWidePropertyTableLoader(loaderSettings,
				spark(), JoinedWidePropertyTableLoader.JoinType.outer, statistics);
		jwptOuterLoader.load();

		final JoinedWidePropertyTableLoader jwptLeftOuterLoader = new JoinedWidePropertyTableLoader(loaderSettings,
				spark(), JoinedWidePropertyTableLoader.JoinType.leftouter, statistics);
		jwptLeftOuterLoader.load();

		/*final JoinedWidePropertyTableLoader jwptInnerLoader = new JoinedWidePropertyTableLoader(loaderSettings,
				spark(), JoinedWidePropertyTableLoader.JoinType.inner, statistics);
		jwptLeftOuterLoader.load();*/

	}
}

/*
PREFIX ex: <http://example.org/#>.

TABLE:
================================================================================================================
ex:book1		| ex:title			| "Title1"
ex:book1		| ex:sales			| "1"
ex:book1		| ex:sales			| "2"
ex:book1		| ex:sales			| "3"


================================================================================================================

QUERY:
-----------------------------------------------------------------------------------------------------------------
SELECT ?title (SUM(?sales) AS ?sales)
WHERE
{
	?book <http://example.org/title> ?title.
	?book <http://example.org/sales> ?sales.
}
GROUP BY ?title
-----------------------------------------------------------------------------------------------------------------
RESULT:
-----------------------------------------------------------------------------------------------------------------
Expected:
+------+-----+
| title|sales|
+------+-----+
|Title1|   6 |
+------+-----+

Actual:
com.hp.hpl.jena.query.QueryParseException: Variable used when already in-scope: 
?sales in ((AGG ?.0 SUM(?sales)) AS ?sales)

-----------------------------------------------------------------------------------------------------------------
*/
