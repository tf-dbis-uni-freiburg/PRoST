package query.run;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import com.holdenkarau.spark.testing.JavaDataFrameSuiteBase;
import joinTree.JoinTree;
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
import translator.Translator;
import utils.Settings;

/**
 * This class tests represents the highest level of testing, i.e. given a query
 * it checks that results are correctly and consistently returned according to
 * ALL supported logical partitioning strategies (at the moment WPT, IWPT, JWPT,
 * and VP?), i.e. these tests verify are about SPARQL semantics.
 *
 * @author Kristin Plettau
 */
public class OrderByTest extends JavaDataFrameSuiteBase implements Serializable {
	private static final long serialVersionUID = 1329L;
	private static final Encoder<TripleBean> triplesEncoder = Encoders.bean(TripleBean.class);

	@Test
	public void queryTest2() {
		final DatabaseStatistics statistics = new DatabaseStatistics("queryTest16_db");
		Dataset<Row> fullDataset = initializeDb2(statistics);
		fullDataset = fullDataset.orderBy("s", "p", "o");
		queryOnTT2(statistics, fullDataset);
		queryOnVp2(statistics, fullDataset);
		queryOnWpt2(statistics, fullDataset);
		queryOnIwpt2(statistics, fullDataset);
		queryOnJwptOuter2(statistics, fullDataset);
		queryOnJwptLeftOuter2(statistics, fullDataset);
	}	
	private void queryOnTT2(final DatabaseStatistics statistics, final Dataset<Row> fullDataset) {
		final Settings settings = new Settings.Builder("queryTest16_db").usingTTNodes().usingCharacteristicSets().build();
		final ClassLoader classLoader = getClass().getClassLoader();
		
		final Query query = new Query(classLoader.getResource("queryTestOrderBy1.q").getPath(), statistics, settings);
		

		//EXPECTED
		StructType schema = DataTypes.createStructType(new StructField[]{
				DataTypes.createStructField("title", DataTypes.StringType, true),
				DataTypes.createStructField("price", DataTypes.StringType, true),
				});
		Row row1 = RowFactory.create("Title4", "20");
		Row row2 = RowFactory.create("Title2", "40");
		Row row3 = RowFactory.create("Title1", "50");
		Row row4 = RowFactory.create("Title3", "60");
		List<Row> rowList = ImmutableList.of(row1, row2, row3, row4);
		Dataset<Row> expectedResult = spark().createDataFrame(rowList, schema);
		
		//ACTUAL
		final Dataset<Row> actualResult = query.compute(spark().sqlContext()).orderBy("title", "price");
		final Dataset<Row> nullableActualResult = sqlContext().createDataFrame(actualResult.collectAsList(),
				actualResult.schema().asNullable());
		System.out.print("FilterIsLiteralTest: queryTest1");
		expectedResult.printSchema();
		expectedResult.show();

		nullableActualResult.printSchema();
		nullableActualResult.show();
		assertDataFrameEquals(expectedResult, nullableActualResult);
	}
	
	private void queryOnVp2(final DatabaseStatistics statistics, final Dataset<Row> fullDataset) {
		final Settings settings = new Settings.Builder("queryTest16_db").usingVPNodes().build();
		final ClassLoader classLoader = getClass().getClassLoader();
		
		final Query query = new Query(classLoader.getResource("queryTestOrderBy1.q").getPath(), statistics, settings);
		
		
		//EXPECTED
		StructType schema = DataTypes.createStructType(new StructField[]{
				DataTypes.createStructField("title", DataTypes.StringType, true),
				DataTypes.createStructField("price", DataTypes.StringType, true),
				});
		Row row1 = RowFactory.create("Title4", "20");
		Row row2 = RowFactory.create("Title2", "40");
		Row row3 = RowFactory.create("Title1", "50");
		Row row4 = RowFactory.create("Title3", "60");
		List<Row> rowList = ImmutableList.of(row1, row2, row3, row4);
		Dataset<Row> expectedResult = spark().createDataFrame(rowList, schema);
		
		//ACTUAL
		final Dataset<Row> actualResult = query.compute(spark().sqlContext()).orderBy("title", "price");
		final Dataset<Row> nullableActualResult = sqlContext().createDataFrame(actualResult.collectAsList(),
				actualResult.schema().asNullable());
		
		assertDataFrameEquals(expectedResult, nullableActualResult);
	}

	private void queryOnWpt2(final DatabaseStatistics statistics, final Dataset<Row> fullDataset) {
		final Settings settings = new Settings.Builder("queryTest16_db").usingWPTNodes().build();
		final ClassLoader classLoader = getClass().getClassLoader();
		
		final Query query = new Query(classLoader.getResource("queryTestOrderBy1.q").getPath(), statistics, settings);
		
		
		//EXPECTED
		StructType schema = DataTypes.createStructType(new StructField[]{
				DataTypes.createStructField("title", DataTypes.StringType, true),
				DataTypes.createStructField("price", DataTypes.StringType, true),
				});
		Row row1 = RowFactory.create("Title4", "20");
		Row row2 = RowFactory.create("Title2", "40");
		Row row3 = RowFactory.create("Title1", "50");
		Row row4 = RowFactory.create("Title3", "60");
		List<Row> rowList = ImmutableList.of(row1, row2, row3, row4);
		Dataset<Row> expectedResult = spark().createDataFrame(rowList, schema);
		
		//ACTUAL
		final Dataset<Row> actualResult = query.compute(spark().sqlContext()).orderBy("title", "price");
		final Dataset<Row> nullableActualResult = sqlContext().createDataFrame(actualResult.collectAsList(),
				actualResult.schema().asNullable());		
		
		
		assertDataFrameEquals(expectedResult, nullableActualResult);
	}

	private void queryOnIwpt2(final DatabaseStatistics statistics, final Dataset<Row> fullDataset) {
		final Settings settings = new Settings.Builder("queryTest16_db").usingIWPTNodes().build();
		final ClassLoader classLoader = getClass().getClassLoader();
		
		final Query query = new Query(classLoader.getResource("queryTestOrderBy1.q").getPath(), statistics, settings);
		

		//EXPECTED
		StructType schema = DataTypes.createStructType(new StructField[]{
				DataTypes.createStructField("title", DataTypes.StringType, true),
				DataTypes.createStructField("price", DataTypes.StringType, true),
				});
		Row row1 = RowFactory.create("Title4", "20");
		Row row2 = RowFactory.create("Title2", "40");
		Row row3 = RowFactory.create("Title1", "50");
		Row row4 = RowFactory.create("Title3", "60");
		List<Row> rowList = ImmutableList.of(row1, row2, row3, row4);
		Dataset<Row> expectedResult = spark().createDataFrame(rowList, schema);
		
		//ACTUAL
		final Dataset<Row> actualResult = query.compute(spark().sqlContext()).orderBy("title", "price");
		final Dataset<Row> nullableActualResult = sqlContext().createDataFrame(actualResult.collectAsList(),
				actualResult.schema().asNullable());
		
		assertDataFrameEquals(expectedResult, nullableActualResult);
	}

	private void queryOnJwptOuter2(final DatabaseStatistics statistics, final Dataset<Row> fullDataset) {
		final Settings settings = new Settings.Builder("queryTest16_db").usingJWPTOuterNodes().build();
		final ClassLoader classLoader = getClass().getClassLoader();
		
		final Query query = new Query(classLoader.getResource("queryTestOrderBy1.q").getPath(), statistics, settings);
		
		
		//EXPECTED
		StructType schema = DataTypes.createStructType(new StructField[]{
				DataTypes.createStructField("title", DataTypes.StringType, true),
				DataTypes.createStructField("price", DataTypes.StringType, true),
				});
		Row row1 = RowFactory.create("Title4", "20");
		Row row2 = RowFactory.create("Title2", "40");
		Row row3 = RowFactory.create("Title1", "50");
		Row row4 = RowFactory.create("Title3", "60");
		List<Row> rowList = ImmutableList.of(row1, row2, row3, row4);
		Dataset<Row> expectedResult = spark().createDataFrame(rowList, schema);
		
		//ACTUAL
		final Dataset<Row> actualResult = query.compute(spark().sqlContext()).orderBy("title", "price");
		final Dataset<Row> nullableActualResult = sqlContext().createDataFrame(actualResult.collectAsList(),
				actualResult.schema().asNullable());
		
		assertDataFrameEquals(expectedResult, nullableActualResult);
	}

	private void queryOnJwptLeftOuter2(final DatabaseStatistics statistics, final Dataset<Row> fullDataset) {
		final Settings settings = new Settings.Builder("queryTest16_db").usingJWPTLeftouterNodes().build();
		final ClassLoader classLoader = getClass().getClassLoader();
		
		final Query query = new Query(classLoader.getResource("queryTestOrderBy1.q").getPath(), statistics, settings);
		
		
		//EXPECTED
		StructType schema = DataTypes.createStructType(new StructField[]{
				DataTypes.createStructField("title", DataTypes.StringType, true),
				DataTypes.createStructField("price", DataTypes.StringType, true),
				});
		Row row1 = RowFactory.create("Title4", "20");
		Row row2 = RowFactory.create("Title2", "40");
		Row row3 = RowFactory.create("Title1", "50");
		Row row4 = RowFactory.create("Title3", "60");
		List<Row> rowList = ImmutableList.of(row1, row2, row3, row4);
		Dataset<Row> expectedResult = spark().createDataFrame(rowList, schema);
		
		//ACTUAL
		final Dataset<Row> actualResult = query.compute(spark().sqlContext()).orderBy("title", "price");
		final Dataset<Row> nullableActualResult = sqlContext().createDataFrame(actualResult.collectAsList(),
				actualResult.schema().asNullable());
		
		assertDataFrameEquals(expectedResult, nullableActualResult);
	}

	private Dataset<Row> initializeDb2(final DatabaseStatistics statistics) {
		spark().sql("DROP DATABASE IF EXISTS queryTest16_db CASCADE");
		spark().sql("CREATE DATABASE IF NOT EXISTS  queryTest16_db");
		spark().sql("USE queryTest16_db");

				
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
		t4.setS("<http://example.org/book4>");
		t4.setP("<http://example.org/title>");
		t4.setO("Title4");
		
		final TripleBean t5 = new TripleBean();
		t5.setS("<http://example.org/book1>");
		t5.setP("<http://example.org/price>");
		t5.setO("50");
		
		final TripleBean t6 = new TripleBean();
		t6.setS("<http://example.org/book2>");
		t6.setP("<http://example.org/price>");
		t6.setO("40");
		
		final TripleBean t7 = new TripleBean();
		t7.setS("<http://example.org/book3>");
		t7.setP("<http://example.org/price>");
		t7.setO("60");
		
		final TripleBean t8 = new TripleBean();
		t8.setS("<http://example.org/book4>");
		t8.setP("<http://example.org/price>");
		t8.setO("20");

		final ArrayList<TripleBean> triplesList = new ArrayList<>();
		triplesList.add(t1);
		triplesList.add(t2);
		triplesList.add(t3);
		triplesList.add(t4);
		triplesList.add(t5);
		triplesList.add(t6);
		triplesList.add(t7);
		triplesList.add(t8);


		final Dataset<Row> ttDataset = spark().createDataset(triplesList, triplesEncoder).select("s", "p", "o").orderBy(
				"s", "p", "o");
		ttDataset.write().saveAsTable("tripletable");
		
		final loader.Settings loaderSettings =
				new loader.Settings.Builder("queryTest16_db").withInputPath((System.getProperty(
						"user.dir") + "\\target\\test_output\\OrderByTest").replace('\\', '/'))
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

		final JoinedWidePropertyTableLoader jwptInnerLoader = new JoinedWidePropertyTableLoader(loaderSettings,
				spark(), JoinedWidePropertyTableLoader.JoinType.inner, statistics);
		jwptLeftOuterLoader.load();

		return ttDataset;
	}
	
}

/*
PREFIX ex: <http://example.org/#>.

TABLE:
================================================================================================================
ex:book1		| ex:title			| "Title1"
ex:book1		| ex:price			| "50"

ex:book2		| ex:title			| "Title2"
ex:book2		| ex:price			| "40"

ex:book3		| ex:title			| "Title3"
ex:book3		| ex:price			| "60"

ex:book4		| ex:title			| "Title4"
ex:book4		| ex:price			| "20"
================================================================================================================

QUERY:
-----------------------------------------------------------------------------------------------------------------
SELECT ?title ?price
WHERE
{
	?book <http://example.org/title> ?title.
	?book <http://example.org/price> ?price
}
ORDER BY ?price
-----------------------------------------------------------------------------------------------------------------
RESULT:
-----------------------------------------------------------------------------------------------------------------
Expected:
+------+-----+
| title|price|
+------+-----+
|Title4|   20|
|Title2|   40|
|Title1|   50|
|Title3|   60|
+------+-----+

Actual:
+------+-----+
| title|price|
+------+-----+
|Title1|   50|
|Title2|   40|
|Title3|   60|
|Title4|   20|
+------+-----+

-----------------------------------------------------------------------------------------------------------------
*/
