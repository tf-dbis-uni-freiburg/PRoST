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
public class HavingTest extends JavaDataFrameSuiteBase implements Serializable {
	private static final long serialVersionUID = 1329L;
	private static final Encoder<TripleBean> triplesEncoder = Encoders.bean(TripleBean.class);

	@Test
	@Ignore("Operation not yet implemented.")
	public void queryTest1() throws Exception {
		final DatabaseStatistics statistics = new DatabaseStatistics("queryTestHaving1_db");
		initializeDb1(statistics);
		queryOnTT1(statistics);
		queryOnVp1(statistics);
		queryOnWpt1(statistics);
		queryOnIwpt1(statistics);
		queryOnJwptOuter1(statistics);
		queryOnJwptLeftOuter1(statistics);
	}

	private void queryOnTT1(final DatabaseStatistics statistics) throws Exception {
		final Settings settings = new Settings.Builder("queryTestHaving1_db").usingTTNodes().usingCharacteristicSets().build();
		final ClassLoader classLoader = getClass().getClassLoader();
		final Query query = new Query(classLoader.getResource("queryTestHaving1.q").getPath(), statistics, settings);

		//EXPECTED
		final StructType schema = DataTypes.createStructType(new StructField[]{
				DataTypes.createStructField("title", DataTypes.StringType, true),
				DataTypes.createStructField("sum", DataTypes.StringType, true),
		});
		final Row row1 = RowFactory.create("Title1", "6");
		final List<Row> rowList = ImmutableList.of(row1);
		final Dataset<Row> expectedResult = spark().createDataFrame(rowList, schema);

		//ACTUAL
		final Dataset<Row> actualResult = query.compute(spark().sqlContext()).orderBy("title", "sum");
		final Dataset<Row> nullableActualResult = sqlContext().createDataFrame(actualResult.collectAsList(),
				actualResult.schema().asNullable());
		System.out.print("HavingTest: queryTest1");
		expectedResult.printSchema();
		expectedResult.show();

		nullableActualResult.printSchema();
		nullableActualResult.show();
		assertDataFrameEquals(expectedResult, nullableActualResult);
	}

	private void queryOnVp1(final DatabaseStatistics statistics) throws Exception {
		final Settings settings = new Settings.Builder("queryTestHaving1_db").usingVPNodes().build();
		final ClassLoader classLoader = getClass().getClassLoader();
		final Query query = new Query(classLoader.getResource("queryTestHaving1.q").getPath(), statistics, settings);

		//EXPECTED
		final StructType schema = DataTypes.createStructType(new StructField[]{
				DataTypes.createStructField("title", DataTypes.StringType, true),
				DataTypes.createStructField("sum", DataTypes.StringType, true),
		});
		final Row row1 = RowFactory.create("Title1", "6");
		final List<Row> rowList = ImmutableList.of(row1);
		final Dataset<Row> expectedResult = spark().createDataFrame(rowList, schema);

		//ACTUAL
		final Dataset<Row> actualResult = query.compute(spark().sqlContext()).orderBy("title", "sum");
		final Dataset<Row> nullableActualResult = sqlContext().createDataFrame(actualResult.collectAsList(),
				actualResult.schema().asNullable());

		assertDataFrameEquals(expectedResult, nullableActualResult);
	}

	private void queryOnWpt1(final DatabaseStatistics statistics) throws Exception {
		final Settings settings = new Settings.Builder("queryTestHaving1_db").usingWPTNodes().build();
		final ClassLoader classLoader = getClass().getClassLoader();
		final Query query = new Query(classLoader.getResource("queryTestHaving1.q").getPath(), statistics, settings);

		//EXPECTED
		final StructType schema = DataTypes.createStructType(new StructField[]{
				DataTypes.createStructField("title", DataTypes.StringType, true),
				DataTypes.createStructField("sum", DataTypes.StringType, true),
		});
		final Row row1 = RowFactory.create("Title1", "6");
		final List<Row> rowList = ImmutableList.of(row1);
		final Dataset<Row> expectedResult = spark().createDataFrame(rowList, schema);

		//ACTUAL
		final Dataset<Row> actualResult = query.compute(spark().sqlContext()).orderBy("title", "sum");
		final Dataset<Row> nullableActualResult = sqlContext().createDataFrame(actualResult.collectAsList(),
				actualResult.schema().asNullable());

		assertDataFrameEquals(expectedResult, nullableActualResult);
	}

	private void queryOnIwpt1(final DatabaseStatistics statistics) throws Exception {
		final Settings settings = new Settings.Builder("queryTestHaving1_db").usingIWPTNodes().build();
		final ClassLoader classLoader = getClass().getClassLoader();
		final Query query = new Query(classLoader.getResource("queryTestHaving1.q").getPath(), statistics, settings);

		//EXPECTED
		final StructType schema = DataTypes.createStructType(new StructField[]{
				DataTypes.createStructField("title", DataTypes.StringType, true),
				DataTypes.createStructField("sum", DataTypes.StringType, true),
		});
		final Row row1 = RowFactory.create("Title1", "6");
		final List<Row> rowList = ImmutableList.of(row1);
		final Dataset<Row> expectedResult = spark().createDataFrame(rowList, schema);

		//ACTUAL
		final Dataset<Row> actualResult = query.compute(spark().sqlContext()).orderBy("title", "sum");
		final Dataset<Row> nullableActualResult = sqlContext().createDataFrame(actualResult.collectAsList(),
				actualResult.schema().asNullable());

		assertDataFrameEquals(expectedResult, nullableActualResult);
	}

	private void queryOnJwptOuter1(final DatabaseStatistics statistics) throws Exception {
		final Settings settings = new Settings.Builder("queryTestHaving1_db").usingJWPTOuterNodes().build();
		final ClassLoader classLoader = getClass().getClassLoader();
		final Query query = new Query(classLoader.getResource("queryTestHaving1.q").getPath(), statistics, settings);

		//EXPECTED
		final StructType schema = DataTypes.createStructType(new StructField[]{
				DataTypes.createStructField("title", DataTypes.StringType, true),
				DataTypes.createStructField("sum", DataTypes.StringType, true),
		});
		final Row row1 = RowFactory.create("Title1", "6");
		final List<Row> rowList = ImmutableList.of(row1);
		final Dataset<Row> expectedResult = spark().createDataFrame(rowList, schema);

		//ACTUAL
		final Dataset<Row> actualResult = query.compute(spark().sqlContext()).orderBy("title", "sum");
		final Dataset<Row> nullableActualResult = sqlContext().createDataFrame(actualResult.collectAsList(),
				actualResult.schema().asNullable());

		assertDataFrameEquals(expectedResult, nullableActualResult);
	}

	private void queryOnJwptLeftOuter1(final DatabaseStatistics statistics) throws Exception {
		final Settings settings = new Settings.Builder("queryTestHaving1_db").usingJWPTLeftouterNodes().build();
		final ClassLoader classLoader = getClass().getClassLoader();
		final Query query = new Query(classLoader.getResource("queryTestHaving1.q").getPath(), statistics, settings);

		//EXPECTED
		final StructType schema = DataTypes.createStructType(new StructField[]{
				DataTypes.createStructField("title", DataTypes.StringType, true),
				DataTypes.createStructField("sum", DataTypes.StringType, true),
		});
		final Row row1 = RowFactory.create("Title1", "6");
		final List<Row> rowList = ImmutableList.of(row1);
		final Dataset<Row> expectedResult = spark().createDataFrame(rowList, schema);

		//ACTUAL
		final Dataset<Row> actualResult = query.compute(spark().sqlContext()).orderBy("title", "sum");
		final Dataset<Row> nullableActualResult = sqlContext().createDataFrame(actualResult.collectAsList(),
				actualResult.schema().asNullable());

		assertDataFrameEquals(expectedResult, nullableActualResult);
	}

	private void initializeDb1(final DatabaseStatistics statistics) {
		spark().sql("DROP DATABASE IF EXISTS queryTestHaving1_db CASCADE");
		spark().sql("CREATE DATABASE IF NOT EXISTS  queryTestHaving1_db");
		spark().sql("USE queryTestHaving1_db");

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

		final TripleBean t5 = new TripleBean();
		t5.setS("<http://example.org/book2>");
		t5.setP("<http://example.org/title>");
		t5.setO("Title2");

		final TripleBean t6 = new TripleBean();
		t6.setS("<http://example.org/book2>");
		t6.setP("<http://example.org/sales>");
		t6.setO("1");

		final TripleBean t7 = new TripleBean();
		t7.setS("<http://example.org/book2>");
		t7.setP("<http://example.org/sales>");
		t7.setO("2");

		final ArrayList<TripleBean> triplesList = new ArrayList<>();
		triplesList.add(t1);
		triplesList.add(t2);
		triplesList.add(t3);
		triplesList.add(t4);
		triplesList.add(t5);
		triplesList.add(t6);
		triplesList.add(t7);

		final Dataset<Row> ttDataset = spark().createDataset(triplesList, triplesEncoder).select("s", "p", "o").orderBy(
				"s", "p", "o");
		ttDataset.write().saveAsTable("tripletable");

		final loader.Settings loaderSettings =
				new loader.Settings.Builder("queryTestHaving1_db").withInputPath((System.getProperty(
						"user.dir") + "\\target\\test_output\\HavingTest").replace('\\', '/'))
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

ex:book2		| ex:title			| "Title2"
ex:book2		| ex:sales			| "1"
ex:book2		| ex:sales			| "2"
================================================================================================================


QUERY:
-----------------------------------------------------------------------------------------------------------------
SELECT ?title (SUM(?sales) AS ?sum)
WHERE
{
	?book <http://example.org/title> ?title.
	?book <http://example.org/sales> ?sales.
}
GROUP BY ?title
HAVING (SUM(?sales) > 5)
-----------------------------------------------------------------------------------------------------------------
RESULT:
-----------------------------------------------------------------------------------------------------------------
Expected:
+------+-----+
| title| sum |
+------+-----+
|Title1|   6 |
+------+-----+

Actual:
java.lang.Exception: Operation not yet implemented.
-----------------------------------------------------------------------------------------------------------------
*/
