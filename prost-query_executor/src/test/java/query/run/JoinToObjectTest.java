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
public class JoinToObjectTest extends JavaDataFrameSuiteBase implements Serializable {
	private static final long serialVersionUID = 1329L;
	private static final Encoder<TripleBean> triplesEncoder = Encoders.bean(TripleBean.class);

	@Test
	public void queryTest() {
		final DatabaseStatistics statistics = new DatabaseStatistics("queryTest06_db");
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
		final Settings settings = new Settings.Builder("queryTest06_db").usingTTNodes().build();
		final ClassLoader classLoader = getClass().getClassLoader();
		
		final Query query = new Query(classLoader.getResource("queryTestJoinToObject1.q").getPath(), statistics, settings);
		
		

		//EXPECTED
		StructType schema = DataTypes.createStructType(new StructField[]{
				DataTypes.createStructField("book", DataTypes.StringType, true),
				DataTypes.createStructField("genre", DataTypes.StringType, true),
				});
		Row row1 = RowFactory.create("<http://example.org/book1>", "Crime");
		Row row2 = RowFactory.create("<http://example.org/book1>", "Science");
		List<Row> rowList = ImmutableList.of(row1, row2);
		Dataset<Row> expectedResult = spark().createDataFrame(rowList, schema);
		
		//ACTUAL
		final Dataset<Row> actualResult = query.compute(spark().sqlContext()).orderBy("book", "genre");
		final Dataset<Row> nullableActualResult = sqlContext().createDataFrame(actualResult.collectAsList(),
				actualResult.schema().asNullable());
		System.out.print("JoinToObjectTest: queryTest1");
		expectedResult.printSchema();
		expectedResult.show();

		nullableActualResult.printSchema();
		nullableActualResult.show();		
		assertDataFrameEquals(expectedResult, nullableActualResult);
	}
	
	private void queryOnVp(final DatabaseStatistics statistics, final Dataset<Row> fullDataset) {
		final Settings settings = new Settings.Builder("queryTest06_db").usingVPNodes().build();
		final ClassLoader classLoader = getClass().getClassLoader();
		
		final Query query = new Query(classLoader.getResource("queryTestJoinToObject1.q").getPath(), statistics, settings);
		
		
		//EXPECTED
		StructType schema = DataTypes.createStructType(new StructField[]{
				DataTypes.createStructField("book", DataTypes.StringType, true),
				DataTypes.createStructField("genre", DataTypes.StringType, true),
				});
		Row row1 = RowFactory.create("<http://example.org/book1>", "Crime");
		Row row2 = RowFactory.create("<http://example.org/book1>", "Science");
		List<Row> rowList = ImmutableList.of(row1, row2);
		Dataset<Row> expectedResult = spark().createDataFrame(rowList, schema);
		
		//ACTUAL
		final Dataset<Row> actualResult = query.compute(spark().sqlContext()).orderBy("book", "genre");
		final Dataset<Row> nullableActualResult = sqlContext().createDataFrame(actualResult.collectAsList(),
				actualResult.schema().asNullable());
		
		assertDataFrameEquals(expectedResult, nullableActualResult);
	}

	private void queryOnWpt(final DatabaseStatistics statistics, final Dataset<Row> fullDataset) {
		final Settings settings = new Settings.Builder("queryTest06_db").usingWPTNodes().build();
		final ClassLoader classLoader = getClass().getClassLoader();
		
		final Query query = new Query(classLoader.getResource("queryTestJoinToObject1.q").getPath(), statistics, settings);
		
		
		//EXPECTED
		StructType schema = DataTypes.createStructType(new StructField[]{
				DataTypes.createStructField("book", DataTypes.StringType, true),
				DataTypes.createStructField("genre", DataTypes.StringType, true),
				});
		Row row1 = RowFactory.create("<http://example.org/book1>", "Crime");
		Row row2 = RowFactory.create("<http://example.org/book1>", "Science");
		List<Row> rowList = ImmutableList.of(row1, row2);
		Dataset<Row> expectedResult = spark().createDataFrame(rowList, schema);
		
		//ACTUAL
		final Dataset<Row> actualResult = query.compute(spark().sqlContext()).orderBy("book", "genre");
		final Dataset<Row> nullableActualResult = sqlContext().createDataFrame(actualResult.collectAsList(),
				actualResult.schema().asNullable());
				
		assertDataFrameEquals(expectedResult, nullableActualResult);
	}

	private void queryOnIwpt(final DatabaseStatistics statistics, final Dataset<Row> fullDataset) {
		final Settings settings = new Settings.Builder("queryTest06_db").usingIWPTNodes().build();
		final ClassLoader classLoader = getClass().getClassLoader();
		
		final Query query = new Query(classLoader.getResource("queryTestJoinToObject1.q").getPath(), statistics, settings);
		

		//EXPECTED
		StructType schema = DataTypes.createStructType(new StructField[]{
				DataTypes.createStructField("book", DataTypes.StringType, true),
				DataTypes.createStructField("genre", DataTypes.StringType, true),
				});
		Row row1 = RowFactory.create("<http://example.org/book1>", "Crime");
		Row row2 = RowFactory.create("<http://example.org/book1>", "Science");
		List<Row> rowList = ImmutableList.of(row1, row2);
		Dataset<Row> expectedResult = spark().createDataFrame(rowList, schema);
		
		//ACTUAL
		final Dataset<Row> actualResult = query.compute(spark().sqlContext()).orderBy("book", "genre");
		final Dataset<Row> nullableActualResult = sqlContext().createDataFrame(actualResult.collectAsList(),
				actualResult.schema().asNullable());
				
		assertDataFrameEquals(expectedResult, nullableActualResult);
	}

	private void queryOnJwptOuter(final DatabaseStatistics statistics, final Dataset<Row> fullDataset) {
		final Settings settings = new Settings.Builder("queryTest06_db").usingJWPTOuterNodes().build();
		final ClassLoader classLoader = getClass().getClassLoader();
		
		final Query query = new Query(classLoader.getResource("queryTestJoinToObject1.q").getPath(), statistics, settings);
		
		
		//EXPECTED
		StructType schema = DataTypes.createStructType(new StructField[]{
				DataTypes.createStructField("book", DataTypes.StringType, true),
				DataTypes.createStructField("genre", DataTypes.StringType, true),
				});
		Row row1 = RowFactory.create("<http://example.org/book1>", "Crime");
		Row row2 = RowFactory.create("<http://example.org/book1>", "Science");
		List<Row> rowList = ImmutableList.of(row1, row2);
		Dataset<Row> expectedResult = spark().createDataFrame(rowList, schema);
		
		//ACTUAL
		final Dataset<Row> actualResult = query.compute(spark().sqlContext()).orderBy("book", "genre");
		final Dataset<Row> nullableActualResult = sqlContext().createDataFrame(actualResult.collectAsList(),
				actualResult.schema().asNullable());
				
		assertDataFrameEquals(expectedResult, nullableActualResult);
	}

	private void queryOnJwptLeftOuter(final DatabaseStatistics statistics, final Dataset<Row> fullDataset) {
		final Settings settings = new Settings.Builder("queryTest06_db").usingJWPTLeftouterNodes().build();
		final ClassLoader classLoader = getClass().getClassLoader();
		
		final Query query = new Query(classLoader.getResource("queryTestJoinToObject1.q").getPath(), statistics, settings);
		
		
		//EXPECTED
		StructType schema = DataTypes.createStructType(new StructField[]{
				DataTypes.createStructField("book", DataTypes.StringType, true),
				DataTypes.createStructField("genre", DataTypes.StringType, true),
				});
		Row row1 = RowFactory.create("<http://example.org/book1>", "Crime");
		Row row2 = RowFactory.create("<http://example.org/book1>", "Science");
		List<Row> rowList = ImmutableList.of(row1, row2);
		Dataset<Row> expectedResult = spark().createDataFrame(rowList, schema);
		
		//ACTUAL
		final Dataset<Row> actualResult = query.compute(spark().sqlContext()).orderBy("book", "genre");
		final Dataset<Row> nullableActualResult = sqlContext().createDataFrame(actualResult.collectAsList(),
				actualResult.schema().asNullable());
				
		assertDataFrameEquals(expectedResult, nullableActualResult);
	}

	private Dataset<Row> initializeDb(final DatabaseStatistics statistics) {
		spark().sql("DROP DATABASE IF EXISTS queryTest06_db CASCADE");
		spark().sql("CREATE DATABASE IF NOT EXISTS  queryTest06_db");
		spark().sql("USE queryTest06_db");

				
		// creates test tt table
		final TripleBean t1 = new TripleBean();
		t1.setS("<http://example.org/book1>");
		t1.setP("<http://example.org/genre>");
		t1.setO("Science");

		final TripleBean t2 = new TripleBean();
		t2.setS("<http://example.org/book1>");
		t2.setP("<http://example.org/genre>");
		t2.setO("Crime");

		final ArrayList<TripleBean> triplesList = new ArrayList<>();
		triplesList.add(t1);
		triplesList.add(t2);

		final Dataset<Row> ttDataset = spark().createDataset(triplesList, triplesEncoder).select("s", "p", "o").orderBy(
				"s", "p", "o");
		ttDataset.write().saveAsTable("tripletable");

		final loader.Settings loaderSettings =
				new loader.Settings.Builder("queryTest06_db").withInputPath((System.getProperty(
						"user.dir") + "\\target\\test_output\\JoinToObjectTest").replace('\\', '/'))
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
ex:book1		| ex:genre			| "Science"
ex:book1		| ex:genre			| "Crime"

================================================================================================================

QUERY:
-----------------------------------------------------------------------------------------------------------------
SELECT ?book ?genre
WHERE
{
	?book <http://example.org/genre> ?genre.
}
-----------------------------------------------------------------------------------------------------------------

RESULT:
-----------------------------------------------------------------------------------------------------------------
Expected:
+------------------------------------+
| book                      |   name |
+---------------------------+--------+
|<http://example.org/book1> |  Crime |
|<http://example.org/book1> |Science |
+---------------------------+--------+

Actual:
+------------------------------------+
| book                      |   name |
+---------------------------+--------+
|<http://example.org/book1> |  Crime |
|<http://example.org/book1> |Science |
+---------------------------+--------+
-----------------------------------------------------------------------------------------------------------------
*/
