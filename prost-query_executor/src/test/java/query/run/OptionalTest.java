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
public class OptionalTest extends JavaDataFrameSuiteBase implements Serializable {
	private static final long serialVersionUID = 1329L;
	private static final Encoder<TripleBean> triplesEncoder = Encoders.bean(TripleBean.class);

	
	@Test
	//@Ignore("Optionals are not fully implemented yet.")
	public void queryTest1() {
		final DatabaseStatistics statistics = new DatabaseStatistics("queryTest05_db");
		Dataset<Row> fullDataset = initializeDb(statistics);
		fullDataset = fullDataset.orderBy("s", "p", "o");
		queryOnTT(statistics, fullDataset);
		queryOnVp(statistics, fullDataset);
		queryOnWpt(statistics, fullDataset);
		queryOnIwpt(statistics, fullDataset);
		queryOnJwptOuter(statistics, fullDataset);
		queryOnJwptLeftOuter(statistics, fullDataset);
	}

	private void queryOnTT(final DatabaseStatistics statistics, final Dataset<Row> fullDataset) {
		final Settings settings = new Settings.Builder("queryTest05_db").usingTTNodes().build();
		final ClassLoader classLoader = getClass().getClassLoader();
		
		final Query query = new Query(classLoader.getResource("queryTestOptional1.q").getPath(), statistics, settings);
		

		//EXPECTED
		final StructType schema = DataTypes.createStructType(new StructField[]{
				DataTypes.createStructField("title", DataTypes.StringType, true),
				DataTypes.createStructField("genre", DataTypes.StringType, true),
				});
		final Row row1 = RowFactory.create("Title1", "Science");
		final Row row2 = RowFactory.create("Title2", null);
		final List<Row> rowList = ImmutableList.of(row1, row2);
		final Dataset<Row> expectedResult = spark().createDataFrame(rowList, schema);
		
		//ACTUAL
		final Dataset<Row> actualResult = query.compute(spark().sqlContext()).orderBy("title", "genre");
		final Dataset<Row> nullableActualResult = sqlContext().createDataFrame(actualResult.collectAsList(),
				actualResult.schema().asNullable());
		System.out.print("OptionalTest: queryTest1");
		expectedResult.printSchema();
		expectedResult.show();

		nullableActualResult.printSchema();
		nullableActualResult.show();
		assertDataFrameEquals(expectedResult, nullableActualResult);
	}
	
	private void queryOnVp(final DatabaseStatistics statistics, final Dataset<Row> fullDataset) {
		final Settings settings = new Settings.Builder("queryTest05_db").usingVPNodes().build();
		final ClassLoader classLoader = getClass().getClassLoader();
		
		final Query query = new Query(classLoader.getResource("queryTestOptional1.q").getPath(), statistics, settings);
		
		
		//EXPECTED
		final StructType schema = DataTypes.createStructType(new StructField[]{
				DataTypes.createStructField("title", DataTypes.StringType, true),
				DataTypes.createStructField("genre", DataTypes.StringType, true),
				});
		final Row row1 = RowFactory.create("Title1", "Science");
		final Row row2 = RowFactory.create("Title2", null);
		final List<Row> rowList = ImmutableList.of(row1, row2);
		final Dataset<Row> expectedResult = spark().createDataFrame(rowList, schema);
		
		//ACTUAL
		final Dataset<Row> actualResult = query.compute(spark().sqlContext()).orderBy("title", "genre");
		final Dataset<Row> nullableActualResult = sqlContext().createDataFrame(actualResult.collectAsList(),
				actualResult.schema().asNullable());
		
		assertDataFrameEquals(expectedResult, nullableActualResult);
	}

	private void queryOnWpt(final DatabaseStatistics statistics, final Dataset<Row> fullDataset) {
		final Settings settings = new Settings.Builder("queryTest05_db").usingWPTNodes().build();
		final ClassLoader classLoader = getClass().getClassLoader();
		
		final Query query = new Query(classLoader.getResource("queryTestOptional1.q").getPath(), statistics, settings);
		
		
		//EXPECTED
		final StructType schema = DataTypes.createStructType(new StructField[]{
				DataTypes.createStructField("title", DataTypes.StringType, true),
				DataTypes.createStructField("genre", DataTypes.StringType, true),
				});
		final Row row1 = RowFactory.create("Title1", "Science");
		final Row row2 = RowFactory.create("Title2", null);
		final List<Row> rowList = ImmutableList.of(row1, row2);
		final Dataset<Row> expectedResult = spark().createDataFrame(rowList, schema);
		
		//ACTUAL
		final Dataset<Row> actualResult = query.compute(spark().sqlContext()).orderBy("title", "genre");
		final Dataset<Row> nullableActualResult = sqlContext().createDataFrame(actualResult.collectAsList(),
				actualResult.schema().asNullable());
		
		assertDataFrameEquals(expectedResult, nullableActualResult);
	}

	private void queryOnIwpt(final DatabaseStatistics statistics, final Dataset<Row> fullDataset) {
		final Settings settings = new Settings.Builder("queryTest05_db").usingIWPTNodes().build();
		final ClassLoader classLoader = getClass().getClassLoader();
		
		final Query query = new Query(classLoader.getResource("queryTestOptional1.q").getPath(), statistics, settings);
		

		//EXPECTED
		final StructType schema = DataTypes.createStructType(new StructField[]{
				DataTypes.createStructField("title", DataTypes.StringType, true),
				DataTypes.createStructField("genre", DataTypes.StringType, true),
				});
		final Row row1 = RowFactory.create("Title1", "Science");
		final Row row2 = RowFactory.create("Title2", null);
		final List<Row> rowList = ImmutableList.of(row1, row2);
		final Dataset<Row> expectedResult = spark().createDataFrame(rowList, schema);
		
		//ACTUAL
		final Dataset<Row> actualResult = query.compute(spark().sqlContext()).orderBy("title", "genre");
		final Dataset<Row> nullableActualResult = sqlContext().createDataFrame(actualResult.collectAsList(),
				actualResult.schema().asNullable());
		
		assertDataFrameEquals(expectedResult, nullableActualResult);
	}

	private void queryOnJwptOuter(final DatabaseStatistics statistics, final Dataset<Row> fullDataset) {
		final Settings settings = new Settings.Builder("queryTest05_db").usingJWPTOuterNodes().build();
		final ClassLoader classLoader = getClass().getClassLoader();
		
		final Query query = new Query(classLoader.getResource("queryTestOptional1.q").getPath(), statistics, settings);
		
		
		//EXPECTED
		final StructType schema = DataTypes.createStructType(new StructField[]{
				DataTypes.createStructField("title", DataTypes.StringType, true),
				DataTypes.createStructField("genre", DataTypes.StringType, true),
				});
		final Row row1 = RowFactory.create("Title1", "Science");
		final Row row2 = RowFactory.create("Title2", null);
		final List<Row> rowList = ImmutableList.of(row1, row2);
		final Dataset<Row> expectedResult = spark().createDataFrame(rowList, schema);
		
		//ACTUAL
		final Dataset<Row> actualResult = query.compute(spark().sqlContext()).orderBy("title", "genre");
		final Dataset<Row> nullableActualResult = sqlContext().createDataFrame(actualResult.collectAsList(),
				actualResult.schema().asNullable());
		
		assertDataFrameEquals(expectedResult, nullableActualResult);
	}

	private void queryOnJwptLeftOuter(final DatabaseStatistics statistics, final Dataset<Row> fullDataset) {
		final Settings settings = new Settings.Builder("queryTest05_db").usingJWPTLeftouterNodes().build();
		final ClassLoader classLoader = getClass().getClassLoader();
		
		final Query query = new Query(classLoader.getResource("queryTestOptional1.q").getPath(), statistics, settings);
		
		
		//EXPECTED
		final StructType schema = DataTypes.createStructType(new StructField[]{
				DataTypes.createStructField("title", DataTypes.StringType, true),
				DataTypes.createStructField("genre", DataTypes.StringType, true),
				});
		final Row row1 = RowFactory.create("Title1", "Science");
		final Row row2 = RowFactory.create("Title2", null);
		final List<Row> rowList = ImmutableList.of(row1, row2);
		final Dataset<Row> expectedResult = spark().createDataFrame(rowList, schema);
		
		//ACTUAL
		final Dataset<Row> actualResult = query.compute(spark().sqlContext()).orderBy("title", "genre");
		final Dataset<Row> nullableActualResult = sqlContext().createDataFrame(actualResult.collectAsList(),
				actualResult.schema().asNullable());
		
		assertDataFrameEquals(expectedResult, nullableActualResult);
	}

	private Dataset<Row> initializeDb(final DatabaseStatistics statistics) {
		spark().sql("DROP DATABASE IF EXISTS queryTest05_db CASCADE");
		spark().sql("CREATE DATABASE IF NOT EXISTS  queryTest05_db");
		spark().sql("USE queryTest05_db");

				
		// creates test tt table
		final TripleBean t1 = new TripleBean();
		t1.setS("<http://example.org/book1>");
		t1.setP("<http://example.org/title>");
		t1.setO("Title1");

		final TripleBean t2 = new TripleBean();
		t2.setS("<http://example.org/book1>");
		t2.setP("<http://example.org/genre>");
		t2.setO("Science");

		final TripleBean t3 = new TripleBean();
		t3.setS("<http://example.org/book2>");
		t3.setP("<http://example.org/title>");
		t3.setO("Title2");

		final ArrayList<TripleBean> triplesList = new ArrayList<>();
		triplesList.add(t1);
		triplesList.add(t2);
		triplesList.add(t3);


		final Dataset<Row> ttDataset = spark().createDataset(triplesList, triplesEncoder).select("s", "p", "o").orderBy(
				"s", "p", "o");
		ttDataset.write().saveAsTable("tripletable");

		
		final loader.Settings loaderSettings =
				new loader.Settings.Builder("queryTest05_db").withInputPath((System.getProperty(
						"user.dir") + "\\target\\test_output\\OptionalTest").replace('\\', '/'))
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
	
	
	@Test
//	@Ignore("Optionals are not fully implemented yet.")
	public void queryTest2() {
		final DatabaseStatistics statistics = new DatabaseStatistics("queryTest08_db");
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
		final Settings settings = new Settings.Builder("queryTest08_db").usingTTNodes().usingCharacteristicSets().build();
		final ClassLoader classLoader = getClass().getClassLoader();
		
		final Query query = new Query(classLoader.getResource("queryTestOptional2.q").getPath(), statistics, settings);
		

		//EXPECTED
		StructType schema = DataTypes.createStructType(new StructField[]{
				DataTypes.createStructField("title", DataTypes.StringType, true),
				DataTypes.createStructField("author", DataTypes.StringType, true),
				DataTypes.createStructField("name", DataTypes.StringType, true),
				});
		Row row1 = RowFactory.create("Title1", "<http://example.org/author>", "Author1");
		Row row2 = RowFactory.create("Title2", null, null);
		List<Row> rowList = ImmutableList.of(row1, row2);
		Dataset<Row> expectedResult = spark().createDataFrame(rowList, schema);
		
		//ACTUAL
		final Dataset<Row> actualResult = query.compute(spark().sqlContext()).orderBy("title", "author", "name");
		final Dataset<Row> nullableActualResult = sqlContext().createDataFrame(actualResult.collectAsList(),
				actualResult.schema().asNullable());
		System.out.print("OptionalTest: queryTest2");
		expectedResult.printSchema();
		expectedResult.show();

		nullableActualResult.printSchema();
		nullableActualResult.show();
		assertDataFrameEquals(expectedResult, nullableActualResult);
	}
	
	private void queryOnVp2(final DatabaseStatistics statistics, final Dataset<Row> fullDataset) {
		final Settings settings = new Settings.Builder("queryTest08_db").usingVPNodes().build();
		final ClassLoader classLoader = getClass().getClassLoader();
		
		final Query query = new Query(classLoader.getResource("queryTestOptional2.q").getPath(), statistics, settings);
		
		
		//EXPECTED
		StructType schema = DataTypes.createStructType(new StructField[]{
				DataTypes.createStructField("title", DataTypes.StringType, true),
				DataTypes.createStructField("author", DataTypes.StringType, true),
				DataTypes.createStructField("name", DataTypes.StringType, true),
				});
		Row row1 = RowFactory.create("Title1", "<http://example.org/author>", "Author1");
		Row row2 = RowFactory.create("Title2", null, null);
		List<Row> rowList = ImmutableList.of(row1, row2);
		Dataset<Row> expectedResult = spark().createDataFrame(rowList, schema);
		
		//ACTUAL
		final Dataset<Row> actualResult = query.compute(spark().sqlContext()).orderBy("title", "author", "name");
		final Dataset<Row> nullableActualResult = sqlContext().createDataFrame(actualResult.collectAsList(),
				actualResult.schema().asNullable());
		
		assertDataFrameEquals(expectedResult, nullableActualResult);
	}

	private void queryOnWpt2(final DatabaseStatistics statistics, final Dataset<Row> fullDataset) {
		final Settings settings = new Settings.Builder("queryTest08_db").usingWPTNodes().build();
		final ClassLoader classLoader = getClass().getClassLoader();
		
		final Query query = new Query(classLoader.getResource("queryTestOptional2.q").getPath(), statistics, settings);
		
		
		//EXPECTED
		StructType schema = DataTypes.createStructType(new StructField[]{
				DataTypes.createStructField("title", DataTypes.StringType, true),
				DataTypes.createStructField("author", DataTypes.StringType, true),
				DataTypes.createStructField("name", DataTypes.StringType, true),
				});
		Row row1 = RowFactory.create("Title1", "<http://example.org/author>", "Author1");
		Row row2 = RowFactory.create("Title2", null, null);
		List<Row> rowList = ImmutableList.of(row1, row2);
		Dataset<Row> expectedResult = spark().createDataFrame(rowList, schema);
		
		//ACTUAL
		final Dataset<Row> actualResult = query.compute(spark().sqlContext()).orderBy("title", "author", "name");
		final Dataset<Row> nullableActualResult = sqlContext().createDataFrame(actualResult.collectAsList(),
				actualResult.schema().asNullable());
		
		assertDataFrameEquals(expectedResult, nullableActualResult);
	}

	private void queryOnIwpt2(final DatabaseStatistics statistics, final Dataset<Row> fullDataset) {
		final Settings settings = new Settings.Builder("queryTest08_db").usingIWPTNodes().build();
		final ClassLoader classLoader = getClass().getClassLoader();
		
		final Query query = new Query(classLoader.getResource("queryTestOptional2.q").getPath(), statistics, settings);
		

		//EXPECTED
		StructType schema = DataTypes.createStructType(new StructField[]{
				DataTypes.createStructField("title", DataTypes.StringType, true),
				DataTypes.createStructField("author", DataTypes.StringType, true),
				DataTypes.createStructField("name", DataTypes.StringType, true),
				});
		Row row1 = RowFactory.create("Title1", "<http://example.org/author>", "Author1");
		Row row2 = RowFactory.create("Title2", null, null);
		List<Row> rowList = ImmutableList.of(row1, row2);
		Dataset<Row> expectedResult = spark().createDataFrame(rowList, schema);
		
		//ACTUAL
		final Dataset<Row> actualResult = query.compute(spark().sqlContext()).orderBy("title", "author", "name");
		final Dataset<Row> nullableActualResult = sqlContext().createDataFrame(actualResult.collectAsList(),
				actualResult.schema().asNullable());
		
		assertDataFrameEquals(expectedResult, nullableActualResult);
	}

	private void queryOnJwptOuter2(final DatabaseStatistics statistics, final Dataset<Row> fullDataset) {
		final Settings settings = new Settings.Builder("queryTest08_db").usingJWPTOuterNodes().build();
		final ClassLoader classLoader = getClass().getClassLoader();
		
		final Query query = new Query(classLoader.getResource("queryTestOptional2.q").getPath(), statistics, settings);
		
		
		//EXPECTED
		StructType schema = DataTypes.createStructType(new StructField[]{
				DataTypes.createStructField("title", DataTypes.StringType, true),
				DataTypes.createStructField("author", DataTypes.StringType, true),
				DataTypes.createStructField("name", DataTypes.StringType, true),
				});
		Row row1 = RowFactory.create("Title1", "<http://example.org/author>", "Author1");
		Row row2 = RowFactory.create("Title2", null, null);
		List<Row> rowList = ImmutableList.of(row1, row2);
		Dataset<Row> expectedResult = spark().createDataFrame(rowList, schema);
		
		//ACTUAL
		final Dataset<Row> actualResult = query.compute(spark().sqlContext()).orderBy("title", "author", "name");
		final Dataset<Row> nullableActualResult = sqlContext().createDataFrame(actualResult.collectAsList(),
				actualResult.schema().asNullable());
		
		assertDataFrameEquals(expectedResult, nullableActualResult);
	}

	private void queryOnJwptLeftOuter2(final DatabaseStatistics statistics, final Dataset<Row> fullDataset) {
		final Settings settings = new Settings.Builder("queryTest08_db").usingJWPTLeftouterNodes().build();
		final ClassLoader classLoader = getClass().getClassLoader();
		
		final Query query = new Query(classLoader.getResource("queryTestOptional2.q").getPath(), statistics, settings);
		
		
		//EXPECTED
		StructType schema = DataTypes.createStructType(new StructField[]{
				DataTypes.createStructField("title", DataTypes.StringType, true),
				DataTypes.createStructField("author", DataTypes.StringType, true),
				DataTypes.createStructField("name", DataTypes.StringType, true),
				});
		Row row1 = RowFactory.create("Title1", "<http://example.org/author>", "Author1");
		Row row2 = RowFactory.create("Title2", null, null);
		List<Row> rowList = ImmutableList.of(row1, row2);
		Dataset<Row> expectedResult = spark().createDataFrame(rowList, schema);
		
		//ACTUAL
		final Dataset<Row> actualResult = query.compute(spark().sqlContext()).orderBy("title", "author", "name");
		final Dataset<Row> nullableActualResult = sqlContext().createDataFrame(actualResult.collectAsList(),
				actualResult.schema().asNullable());
		
		assertDataFrameEquals(expectedResult, nullableActualResult);
	}
	
	private Dataset<Row> initializeDb2(final DatabaseStatistics statistics) {
		spark().sql("DROP DATABASE IF EXISTS queryTest08_db CASCADE");
		spark().sql("CREATE DATABASE IF NOT EXISTS  queryTest08_db");
		spark().sql("USE queryTest08_db");

				
		// creates test tt table
		final TripleBean t1 = new TripleBean();
		t1.setS("<http://example.org/book1>");
		t1.setP("<http://example.org/title>");
		t1.setO("Title1");

		final TripleBean t2 = new TripleBean();
		t2.setS("<http://example.org/book1>");
		t2.setP("<http://example.org/writtenBy>");
		t2.setO("<http://example.org/author>");

		final TripleBean t3 = new TripleBean();
		t3.setS("<http://example.org/author>");
		t3.setP("<http://example.org/name>");
		t3.setO("Author1");

		final TripleBean t4 = new TripleBean();
		t4.setS("<http://example.org/book2>");
		t4.setP("<http://example.org/title>");
		t4.setO("Title2");
		

		final ArrayList<TripleBean> triplesList = new ArrayList<>();
		triplesList.add(t1);
		triplesList.add(t2);
		triplesList.add(t3);
		triplesList.add(t4);

		final Dataset<Row> ttDataset = spark().createDataset(triplesList, triplesEncoder).select("s", "p", "o").orderBy(
				"s", "p", "o");
		ttDataset.write().saveAsTable("tripletable");

		
		final loader.Settings loaderSettings =
				new loader.Settings.Builder("queryTest08_db").withInputPath((System.getProperty(
						"user.dir") + "\\target\\test_output\\OptionalTest").replace('\\', '/'))
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
ex:book1		| ex:genre			| "Science"

ex:book2		| ex:title			| "Title2"
================================================================================================================

QUERY:
-----------------------------------------------------------------------------------------------------------------
SELECT ?title ?genre
WHERE
{
	?book <http://example.org/title> ?title.
	OPTIONAL {?book <http://example.org/genre> ?genre}
}
-----------------------------------------------------------------------------------------------------------------

RESULT:
-----------------------------------------------------------------------------------------------------------------
Expected:
+------+--------+
| title| genre	|
+------+--------+
|Title1|Science	|
|Title2|		|
+------+--------+

Actual:
+------+--------+
| title| genre	|
+------+--------+
|Title1|Science	|
|Title2|		|
+------+--------+
-----------------------------------------------------------------------------------------------------------------
*/

/*
PREFIX ex: <http://example.org/#>.

TABLE:
================================================================================================================
ex:book1		| ex:title			| "Title1"
ex:book1		| ex:writtenBy		| ex:author

ex:book2		| ex:title			| "Title2"

ex:author		| ex:name			| "Author1"
================================================================================================================

QUERY:
-----------------------------------------------------------------------------------------------------------------
SELECT ?title ?author ?name
WHERE
{
	?book <http://example.org/title> ?title.
	OPTIONAL {?book <http://example.org/writtenBy> ?author.
			OPTIONAL {?author <http://example.org/name> ?name}}
}
-----------------------------------------------------------------------------------------------------------------

RESULT:
-----------------------------------------------------------------------------------------------------------------
Expected:
+------+---------------------------+-------+
| title| author                    | name  |
+------+---------------------------+-------+
|Title1|<http://example.org/author>|Author1|
|Title2|		                   |       |
+------+---------------------------+-------+

Actual:
java.lang.NullPointerException

-----------------------------------------------------------------------------------------------------------------
*/
