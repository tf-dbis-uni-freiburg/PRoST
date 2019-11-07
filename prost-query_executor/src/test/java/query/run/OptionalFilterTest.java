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
import org.junit.Test;
import org.spark_project.guava.collect.ImmutableList;
import query.utilities.TripleBean;
import statistics.DatabaseStatistics;
import translator.Query;
import utils.Settings;

/**
 * This class tests represents the highest level of testing, i.e. given a query
 * it checks that results are correctly and consistently returned according to
 * ALL supported logical partitioning strategies (at the moment WPT, IWPT, JWPT,
 * and VP?), i.e. these tests verify are about SPARQL semantics.
 *
 * @author Kristin Plettau
 */
public class OptionalFilterTest extends JavaDataFrameSuiteBase implements Serializable {
	private static final long serialVersionUID = 1329L;
	private static final Encoder<TripleBean> triplesEncoder = Encoders.bean(TripleBean.class);
	
	@Test
	public void queryTest() throws Exception {
		final DatabaseStatistics statistics = new DatabaseStatistics("queryTestOptionalFilter1_db");
		initializeDb(statistics);
		final Dataset<Row> expectedResult = createTestExpectedDataset();

		queryOnTT(statistics, expectedResult);
		queryOnVp(statistics, expectedResult);
		queryOnWpt(statistics, expectedResult);
		queryOnIwpt(statistics, expectedResult);
		queryOnJwptOuter(statistics, expectedResult);
		queryOnJwptLeftOuter(statistics, expectedResult);
	}

	private Dataset<Row> createTestExpectedDataset() {
		final StructType schema = DataTypes.createStructType(new StructField[]{
				DataTypes.createStructField("title", DataTypes.StringType, true),
				DataTypes.createStructField("price", DataTypes.StringType, true),
		});
		final Row row1 = RowFactory.create("Title1", null);
		final Row row2 = RowFactory.create("Title2", "30");
		final Row row3 = RowFactory.create("Title3", "20");
		final List<Row> rowList = ImmutableList.of(row1, row2, row3);
		return spark().createDataFrame(rowList, schema);
	}

	private void queryOnTT(final DatabaseStatistics statistics, final Dataset<Row> expectedResult)  throws Exception {
		final Settings settings = new Settings.Builder("queryTestOptionalFilter1_db").usingTTNodes().build();
		final ClassLoader classLoader = getClass().getClassLoader();
		
		final Query query = new Query(classLoader.getResource("queryTestOptionalFilter1.q").getPath(), statistics, settings);

		//ACTUAL
		final Dataset<Row> actualResult = query.compute(spark().sqlContext()).orderBy("title", "price");
		final Dataset<Row> nullableActualResult = sqlContext().createDataFrame(actualResult.collectAsList(),
				actualResult.schema().asNullable());
		System.out.print("OptionalFilterTest: queryTest1");
		expectedResult.printSchema();
		expectedResult.show();

		nullableActualResult.printSchema();
		nullableActualResult.show();
		assertDataFrameEquals(expectedResult, nullableActualResult);
	}
	
	private void queryOnVp(final DatabaseStatistics statistics, final Dataset<Row> expectedResult)  throws Exception {
		final Settings settings = new Settings.Builder("queryTestOptionalFilter1_db").usingVPNodes().build();
		final ClassLoader classLoader = getClass().getClassLoader();
		
		final Query query = new Query(classLoader.getResource("queryTestOptionalFilter1.q").getPath(), statistics, settings);

		//ACTUAL
		final Dataset<Row> actualResult = query.compute(spark().sqlContext()).orderBy("title", "price");
		final Dataset<Row> nullableActualResult = sqlContext().createDataFrame(actualResult.collectAsList(),
				actualResult.schema().asNullable());
		
		assertDataFrameEquals(expectedResult, nullableActualResult);
	}

	private void queryOnWpt(final DatabaseStatistics statistics, final Dataset<Row> expectedResult)  throws Exception {
		final Settings settings = new Settings.Builder("queryTestOptionalFilter1_db").usingWPTNodes().build();
		final ClassLoader classLoader = getClass().getClassLoader();
		
		final Query query = new Query(classLoader.getResource("queryTestOptionalFilter1.q").getPath(), statistics, settings);

		//ACTUAL
		final Dataset<Row> actualResult = query.compute(spark().sqlContext()).orderBy("title", "price");
		final Dataset<Row> nullableActualResult = sqlContext().createDataFrame(actualResult.collectAsList(),
				actualResult.schema().asNullable());
		
		assertDataFrameEquals(expectedResult, nullableActualResult);
	}

	private void queryOnIwpt(final DatabaseStatistics statistics, final Dataset<Row> expectedResult)  throws Exception {
		final Settings settings = new Settings.Builder("queryTestOptionalFilter1_db").usingIWPTNodes().build();
		final ClassLoader classLoader = getClass().getClassLoader();
		
		final Query query = new Query(classLoader.getResource("queryTestOptionalFilter1.q").getPath(), statistics, settings);

		//ACTUAL
		final Dataset<Row> actualResult = query.compute(spark().sqlContext()).orderBy("title", "price");
		final Dataset<Row> nullableActualResult = sqlContext().createDataFrame(actualResult.collectAsList(),
				actualResult.schema().asNullable());
		
		assertDataFrameEquals(expectedResult, nullableActualResult);
	}

	private void queryOnJwptOuter(final DatabaseStatistics statistics, final Dataset<Row> expectedResult)  throws Exception {
		final Settings settings = new Settings.Builder("queryTestOptionalFilter1_db").usingJWPTOuterNodes().build();
		final ClassLoader classLoader = getClass().getClassLoader();
		
		final Query query = new Query(classLoader.getResource("queryTestOptionalFilter1.q").getPath(), statistics, settings);

		//ACTUAL
		final Dataset<Row> actualResult = query.compute(spark().sqlContext()).orderBy("title", "price");
		final Dataset<Row> nullableActualResult = sqlContext().createDataFrame(actualResult.collectAsList(),
				actualResult.schema().asNullable());
		
		assertDataFrameEquals(expectedResult, nullableActualResult);
	}

	private void queryOnJwptLeftOuter(final DatabaseStatistics statistics, final Dataset<Row> expectedResult)  throws Exception {
		final Settings settings = new Settings.Builder("queryTestOptionalFilter1_db").usingJWPTLeftouterNodes().build();
		final ClassLoader classLoader = getClass().getClassLoader();
		
		final Query query = new Query(classLoader.getResource("queryTestOptionalFilter1.q").getPath(), statistics, settings);

		//ACTUAL
		final Dataset<Row> actualResult = query.compute(spark().sqlContext()).orderBy("title", "price");
		final Dataset<Row> nullableActualResult = sqlContext().createDataFrame(actualResult.collectAsList(),
				actualResult.schema().asNullable());
		
		assertDataFrameEquals(expectedResult, nullableActualResult);
	}

	private Dataset<Row> initializeDb(final DatabaseStatistics statistics) {
		spark().sql("DROP DATABASE IF EXISTS queryTestOptionalFilter1_db CASCADE");
		spark().sql("CREATE DATABASE IF NOT EXISTS  queryTestOptionalFilter1_db");
		spark().sql("USE queryTestOptionalFilter1_db");

		// creates test tt table
		final TripleBean t1 = new TripleBean();
		t1.setS("<http://example.org/book1>");
		t1.setP("<http://example.org/title>");
		t1.setO("Title1");
		
		final TripleBean t2 = new TripleBean();
		t2.setS("<http://example.org/book2>");
		t2.setP("<http://example.org/title>");
		t2.setO("Title2");
		
		final TripleBean t3 = new TripleBean();
		t3.setS("<http://example.org/book3>");
		t3.setP("<http://example.org/title>");
		t3.setO("Title3");

		final TripleBean t4 = new TripleBean();
		t4.setS("<http://example.org/book1>");
		t4.setP("<http://example.org/price>");
		t4.setO("50");
		
		final TripleBean t5 = new TripleBean();
		t5.setS("<http://example.org/book2>");
		t5.setP("<http://example.org/price>");
		t5.setO("30");
		
		final TripleBean t6 = new TripleBean();
		t6.setS("<http://example.org/book3>");
		t6.setP("<http://example.org/price>");
		t6.setO("20");

		final TripleBean t7 = new TripleBean();
		t7.setS("<http://example.org/book1>");
		t7.setP("<http://example.org/genre>");
		t7.setO("Science");

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
				new loader.Settings.Builder("queryTestOptionalFilter1_db").withInputPath((System.getProperty(
						"user.dir") + "\\target\\test_output\\OptionalFilterTest").replace('\\', '/'))
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

		return ttDataset;
	}

	@Test
//	@Ignore("Optionals are not fully implemented yet.")
	public void queryTest2() throws Exception {
		final DatabaseStatistics statistics = new DatabaseStatistics("queryTestOptionalFilter2_db");
		initializeDb2(statistics);
		queryOnTT2(statistics);
		queryOnVp2(statistics);
		queryOnWpt2(statistics);
		queryOnIwpt2(statistics);
		queryOnJwptOuter2(statistics);
		queryOnJwptLeftOuter2(statistics);
	}	
	private void queryOnTT2(final DatabaseStatistics statistics)  throws Exception {
		final Settings settings = new Settings.Builder("queryTestOptionalFilter2_db").usingTTNodes().build();
		final ClassLoader classLoader = getClass().getClassLoader();
		
		final Query query = new Query(classLoader.getResource("queryTestOptionalFilter2.q").getPath(), statistics, settings);

		//EXPECTED
		final StructType schema = DataTypes.createStructType(new StructField[]{
				DataTypes.createStructField("title", DataTypes.StringType, true),
				DataTypes.createStructField("reduced_price", DataTypes.StringType, true),
				});
		// price > 30 and reduced_price <= 20
		final Row row1 = RowFactory.create("Title1", null);
		final Row row2 = RowFactory.create("Title2", "20");
		final Row row3 = RowFactory.create("Title3", null);
		final List<Row> rowList = ImmutableList.of(row1, row2, row3);
		final Dataset<Row> expectedResult = spark().createDataFrame(rowList, schema);
		
		//ACTUAL
		final Dataset<Row> actualResult = query.compute(spark().sqlContext()).orderBy("title", "reduced_price");
		final Dataset<Row> nullableActualResult = sqlContext().createDataFrame(actualResult.collectAsList(),
				actualResult.schema().asNullable());
		System.out.print("OptionalFilterTest: queryTest2");
		expectedResult.printSchema();
		expectedResult.show();

		nullableActualResult.printSchema();
		nullableActualResult.show();
		assertDataFrameEquals(expectedResult, nullableActualResult);
	}
	
	private void queryOnVp2(final DatabaseStatistics statistics)  throws Exception {
		final Settings settings = new Settings.Builder("queryTestOptionalFilter2_db").usingVPNodes().build();
		final ClassLoader classLoader = getClass().getClassLoader();
		
		final Query query = new Query(classLoader.getResource("queryTestOptionalFilter2.q").getPath(), statistics, settings);

		//EXPECTED
		final StructType schema = DataTypes.createStructType(new StructField[]{
				DataTypes.createStructField("title", DataTypes.StringType, true),
				DataTypes.createStructField("reduced_price", DataTypes.StringType, true),
				});
		// price > 30 and reduced_price <= 20
		final Row row1 = RowFactory.create("Title1", null);
		final Row row2 = RowFactory.create("Title2", "20");
		final Row row3 = RowFactory.create("Title3", null);
		final List<Row> rowList = ImmutableList.of(row1, row2, row3);
		final Dataset<Row> expectedResult = spark().createDataFrame(rowList, schema);
		
		//ACTUAL
		final Dataset<Row> actualResult = query.compute(spark().sqlContext()).orderBy("title", "reduced_price");
		final Dataset<Row> nullableActualResult = sqlContext().createDataFrame(actualResult.collectAsList(),
				actualResult.schema().asNullable());
		
		assertDataFrameEquals(expectedResult, nullableActualResult);
	}

	private void queryOnWpt2(final DatabaseStatistics statistics)  throws Exception {
		final Settings settings = new Settings.Builder("queryTestOptionalFilter2_db").usingWPTNodes().build();
		final ClassLoader classLoader = getClass().getClassLoader();
		
		final Query query = new Query(classLoader.getResource("queryTestOptionalFilter2.q").getPath(), statistics, settings);

		//EXPECTED
		final StructType schema = DataTypes.createStructType(new StructField[]{
				DataTypes.createStructField("title", DataTypes.StringType, true),
				DataTypes.createStructField("reduced_price", DataTypes.StringType, true),
				});
		// price > 30 and reduced_price <= 20
		final Row row1 = RowFactory.create("Title1", null);
		final Row row2 = RowFactory.create("Title2", "20");
		final Row row3 = RowFactory.create("Title3", null);
		final List<Row> rowList = ImmutableList.of(row1, row2, row3);
		final Dataset<Row> expectedResult = spark().createDataFrame(rowList, schema);
		
		//ACTUAL
		final Dataset<Row> actualResult = query.compute(spark().sqlContext()).orderBy("title", "reduced_price");
		final Dataset<Row> nullableActualResult = sqlContext().createDataFrame(actualResult.collectAsList(),
				actualResult.schema().asNullable());
		
		assertDataFrameEquals(expectedResult, nullableActualResult);
	}

	private void queryOnIwpt2(final DatabaseStatistics statistics)  throws Exception {
		final Settings settings = new Settings.Builder("queryTestOptionalFilter2_db").usingIWPTNodes().build();
		final ClassLoader classLoader = getClass().getClassLoader();
		
		final Query query = new Query(classLoader.getResource("queryTestOptionalFilter2.q").getPath(), statistics, settings);

		//EXPECTED
		final StructType schema = DataTypes.createStructType(new StructField[]{
				DataTypes.createStructField("title", DataTypes.StringType, true),
				DataTypes.createStructField("reduced_price", DataTypes.StringType, true),
				});
		// price > 30 and reduced_price <= 20
		final Row row1 = RowFactory.create("Title1", null);
		final Row row2 = RowFactory.create("Title2", "20");
		final Row row3 = RowFactory.create("Title3", null);
		final List<Row> rowList = ImmutableList.of(row1, row2, row3);
		final Dataset<Row> expectedResult = spark().createDataFrame(rowList, schema);
		
		//ACTUAL
		final Dataset<Row> actualResult = query.compute(spark().sqlContext()).orderBy("title", "reduced_price");
		final Dataset<Row> nullableActualResult = sqlContext().createDataFrame(actualResult.collectAsList(),
				actualResult.schema().asNullable());
		
		assertDataFrameEquals(expectedResult, nullableActualResult);
	}

	private void queryOnJwptOuter2(final DatabaseStatistics statistics)  throws Exception {
		final Settings settings = new Settings.Builder("queryTestOptionalFilter2_db").usingJWPTOuterNodes().build();
		final ClassLoader classLoader = getClass().getClassLoader();
		
		final Query query = new Query(classLoader.getResource("queryTestOptionalFilter2.q").getPath(), statistics, settings);

		//EXPECTED
		final StructType schema = DataTypes.createStructType(new StructField[]{
				DataTypes.createStructField("title", DataTypes.StringType, true),
				DataTypes.createStructField("reduced_price", DataTypes.StringType, true),
				});
		// price > 30 and reduced_price <= 20
		final Row row1 = RowFactory.create("Title1", null);
		final Row row2 = RowFactory.create("Title2", "20");
		final Row row3 = RowFactory.create("Title3", null);
		final List<Row> rowList = ImmutableList.of(row1, row2, row3);
		final Dataset<Row> expectedResult = spark().createDataFrame(rowList, schema);
		
		//ACTUAL
		final Dataset<Row> actualResult = query.compute(spark().sqlContext()).orderBy("title", "reduced_price");
		final Dataset<Row> nullableActualResult = sqlContext().createDataFrame(actualResult.collectAsList(),
				actualResult.schema().asNullable());
		
		assertDataFrameEquals(expectedResult, nullableActualResult);
	}

	private void queryOnJwptLeftOuter2(final DatabaseStatistics statistics)  throws Exception {
		final Settings settings = new Settings.Builder("queryTestOptionalFilter2_db").usingJWPTLeftouterNodes().build();
		final ClassLoader classLoader = getClass().getClassLoader();
		
		final Query query = new Query(classLoader.getResource("queryTestOptionalFilter2.q").getPath(), statistics, settings);

		//EXPECTED
		final StructType schema = DataTypes.createStructType(new StructField[]{
				DataTypes.createStructField("title", DataTypes.StringType, true),
				DataTypes.createStructField("reduced_price", DataTypes.StringType, true),
				});
		// price > 30 and reduced_price <= 20
		final Row row1 = RowFactory.create("Title1", null);
		final Row row2 = RowFactory.create("Title2", "20");
		final Row row3 = RowFactory.create("Title3", null);
		final List<Row> rowList = ImmutableList.of(row1, row2, row3);
		final Dataset<Row> expectedResult = spark().createDataFrame(rowList, schema);
		
		//ACTUAL
		final Dataset<Row> actualResult = query.compute(spark().sqlContext()).orderBy("title", "reduced_price");
		final Dataset<Row> nullableActualResult = sqlContext().createDataFrame(actualResult.collectAsList(),
				actualResult.schema().asNullable());
		
		assertDataFrameEquals(expectedResult, nullableActualResult);
	}

	private Dataset<Row> initializeDb2(final DatabaseStatistics statistics) {
		spark().sql("DROP DATABASE IF EXISTS queryTestOptionalFilter2_db CASCADE");
		spark().sql("CREATE DATABASE IF NOT EXISTS  queryTestOptionalFilter2_db");
		spark().sql("USE queryTestOptionalFilter2_db");

		// creates test tt table
		final TripleBean t1 = new TripleBean();
		t1.setS("<http://example.org/book1>");
		t1.setP("<http://example.org/title>");
		t1.setO("Title1");
		
		final TripleBean t2 = new TripleBean();
		t2.setS("<http://example.org/book2>");
		t2.setP("<http://example.org/title>");
		t2.setO("Title2");
		
		final TripleBean t3 = new TripleBean();
		t3.setS("<http://example.org/book3>");
		t3.setP("<http://example.org/title>");
		t3.setO("Title3");

		final TripleBean t4 = new TripleBean();
		t4.setS("<http://example.org/book1>");
		t4.setP("<http://example.org/price>");
		t4.setO("50");
		
		final TripleBean t5 = new TripleBean();
		t5.setS("<http://example.org/book2>");
		t5.setP("<http://example.org/price>");
		t5.setO("35");
		
		final TripleBean t6 = new TripleBean();
		t6.setS("<http://example.org/book3>");
		t6.setP("<http://example.org/price>");
		t6.setO("20");
		
		final TripleBean t7 = new TripleBean();
		t7.setS("<http://example.org/book1>");
		t7.setP("<http://example.org/reduced_price>");
		t7.setO("25");
		
		final TripleBean t8 = new TripleBean();
		t8.setS("<http://example.org/book2>");
		t8.setP("<http://example.org/reduced_price>");
		t8.setO("20");
		
		final TripleBean t9 = new TripleBean();
		t9.setS("<http://example.org/book3>");
		t9.setP("<http://example.org/reduced_price>");
		t9.setO("10");

		final ArrayList<TripleBean> triplesList = new ArrayList<>();
		triplesList.add(t1);
		triplesList.add(t2);
		triplesList.add(t3);
		triplesList.add(t4);
		triplesList.add(t5);
		triplesList.add(t6);
		triplesList.add(t7);
		triplesList.add(t8);
		triplesList.add(t9);

		final Dataset<Row> ttDataset = spark().createDataset(triplesList, triplesEncoder).select("s", "p", "o").orderBy(
				"s", "p", "o");
		ttDataset.write().saveAsTable("tripletable");

		final loader.Settings loaderSettings =
				new loader.Settings.Builder("queryTestOptionalFilter2_db").withInputPath((System.getProperty(
						"user.dir") + "\\target\\test_output\\OptionalFilterTest").replace('\\', '/'))
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

		return ttDataset;
	}
}

/*
PREFIX ex: <http://example.org/#>.

TABLE:
================================================================================================================
ex:book1		| ex:title			| "Title1"
ex:book1		| ex:genre			| "Science"
ex:book1		| ex:price			| "50"

ex:book2		| ex:title			| "Title2"
ex:book2		| ex:price			| "30"

ex:book3		| ex:title			| "Title3"
ex:book3		| ex:price			| "20"
================================================================================================================

QUERY: Nested Optional with Filter
-----------------------------------------------------------------------------------------------------------------
SELECT ?title ?price
WHERE
{
	?book <http://example.org/title> ?title.
	OPTIONAL {?book <http://example.org/price> ?price.FILTER(?price <= 30)}
}
-----------------------------------------------------------------------------------------------------------------

RESULT:
-----------------------------------------------------------------------------------------------------------------
Expected:
+------+-----+
| title|price|
+------+-----+
|Title1| null|
|Title2|   20|
|Title3|   30|
+------+-----+

Actual:
+------+-----+
| title|price|
+------+-----+
|Title1| null|
|Title2|   30|
|Title3|   20|
+------+-----+
java.lang.AssertionError: Not Equal Sample: 
(1,([Title2,20],[Title2,30])), (2,([Title3,30],[Title3,20]))

-----------------------------------------------------------------------------------------------------------------
*/

/*
PREFIX ex: <http://example.org/#>.

TABLE:
================================================================================================================
ex:book1		| ex:title			| "Title1"
ex:book1		| ex:price			| "50"
ex:book1		| ex:reduced_price	| "25"

ex:book2		| ex:title			| "Title2"
ex:book2		| ex:price			| "35"
ex:book2		| ex:reduced_price	| "20"

ex:book3		| ex:title			| "Title3"
ex:book3		| ex:price			| "20"
ex:book3		| ex:reduced_price	| "10"
================================================================================================================

QUERY: Nested Optional with Filter
-----------------------------------------------------------------------------------------------------------------
SELECT ?title ?price ?reduced_price
WHERE
{
	?book <http://example.org/title> ?title.
	OPTIONAL {?book <http://example.org/price> ?price.FILTER(?price > 30).
			OPTIONAL {?book <http://example.org/reduced_price> ?reduced_price.FILTER(?reduced_price <= 20)}
	}
}
-----------------------------------------------------------------------------------------------------------------

RESULT:
-----------------------------------------------------------------------------------------------------------------
Expected:
+------+-------------+
| title|reduced_price|
+------+-------------+
|Title1|         null|
|Title2|           20|
|Title3|         null|
+------+-------------+

Actual:
+------+-------------+
| title|reduced_price|
+------+-------------+
|Title1|         null|
|Title2|           20|
|Title3|         null|
+------+-------------+
-----------------------------------------------------------------------------------------------------------------
*/
