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
public class JoinWithUnionTest extends JavaDataFrameSuiteBase implements Serializable {
	private static final long serialVersionUID = 1329L;
	private static final Encoder<TripleBean> triplesEncoder = Encoders.bean(TripleBean.class);

	@Test
//	@Ignore("Unions are not fully implemented yet.")
	public void queryTest() throws Exception {
		final DatabaseStatistics statistics = new DatabaseStatistics("queryTestJoinWithUnion1_db");
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
		});
		final Row row1 = RowFactory.create("Title1");
		final Row row2 = RowFactory.create("Title3");
		final Row row3 = RowFactory.create("Title2");
		final Row row4 = RowFactory.create("Title4");
		final List<Row> rowList = ImmutableList.of(row1, row2, row3, row4);
		return spark().createDataFrame(rowList, schema).orderBy("title");
	}

	private void queryOnTT(final DatabaseStatistics statistics, final Dataset<Row> expectedResult)  throws Exception {
		final Settings settings = new Settings.Builder("queryTestJoinWithUnion1_db").usingTTNodes().build();
		final ClassLoader classLoader = getClass().getClassLoader();
		
		final Query query = new Query(classLoader.getResource("queryTestJoinWithUnion1.q").getPath(), statistics, settings);

		//ACTUAL
		final Dataset<Row> actualResult = query.compute(spark().sqlContext()).orderBy("title");
		final Dataset<Row> nullableActualResult = sqlContext().createDataFrame(actualResult.collectAsList(),
				actualResult.schema().asNullable());
		System.out.print("JoinWithUnion: queryTest1");
		expectedResult.printSchema();
		expectedResult.show();

		nullableActualResult.printSchema();
		nullableActualResult.show();
		assertDataFrameEquals(expectedResult, nullableActualResult);
	}
	
	private void queryOnVp(final DatabaseStatistics statistics, final Dataset<Row> expectedResult)  throws Exception {
		final Settings settings = new Settings.Builder("queryTestJoinWithUnion1_db").usingVPNodes().build();
		final ClassLoader classLoader = getClass().getClassLoader();
		
		final Query query = new Query(classLoader.getResource("queryTestJoinWithUnion1.q").getPath(), statistics, settings);

		//ACTUAL
		final Dataset<Row> actualResult = query.compute(spark().sqlContext()).orderBy("title");
		final Dataset<Row> nullableActualResult = sqlContext().createDataFrame(actualResult.collectAsList(),
				actualResult.schema().asNullable());
		
		assertDataFrameEquals(expectedResult, nullableActualResult);
	}

	private void queryOnWpt(final DatabaseStatistics statistics, final Dataset<Row> expectedResult)  throws Exception {
		final Settings settings = new Settings.Builder("queryTestJoinWithUnion1_db").usingWPTNodes().build();
		final ClassLoader classLoader = getClass().getClassLoader();
		
		final Query query = new Query(classLoader.getResource("queryTestJoinWithUnion1.q").getPath(), statistics, settings);

		//ACTUAL
		final Dataset<Row> actualResult = query.compute(spark().sqlContext()).orderBy("title");
		final Dataset<Row> nullableActualResult = sqlContext().createDataFrame(actualResult.collectAsList(),
				actualResult.schema().asNullable());
		
		assertDataFrameEquals(expectedResult, nullableActualResult);
	}

	private void queryOnIwpt(final DatabaseStatistics statistics, final Dataset<Row> expectedResult)  throws Exception {
		final Settings settings = new Settings.Builder("queryTestJoinWithUnion1_db").usingIWPTNodes().build();
		final ClassLoader classLoader = getClass().getClassLoader();
		
		final Query query = new Query(classLoader.getResource("queryTestJoinWithUnion1.q").getPath(), statistics, settings);

		//ACTUAL
		final Dataset<Row> actualResult = query.compute(spark().sqlContext()).orderBy("title");
		final Dataset<Row> nullableActualResult = sqlContext().createDataFrame(actualResult.collectAsList(),
				actualResult.schema().asNullable());
		
		assertDataFrameEquals(expectedResult, nullableActualResult);
	}

	private void queryOnJwptOuter(final DatabaseStatistics statistics, final Dataset<Row> expectedResult)  throws Exception {
		final Settings settings = new Settings.Builder("queryTestJoinWithUnion1_db").usingJWPTOuterNodes().build();
		final ClassLoader classLoader = getClass().getClassLoader();
		
		final Query query = new Query(classLoader.getResource("queryTestJoinWithUnion1.q").getPath(), statistics, settings);

		//ACTUAL
		final Dataset<Row> actualResult = query.compute(spark().sqlContext()).orderBy("title");
		final Dataset<Row> nullableActualResult = sqlContext().createDataFrame(actualResult.collectAsList(),
				actualResult.schema().asNullable());
		
		assertDataFrameEquals(expectedResult, nullableActualResult);
	}

	private void queryOnJwptLeftOuter(final DatabaseStatistics statistics, final Dataset<Row> expectedResult)  throws Exception {
		final Settings settings = new Settings.Builder("queryTestJoinWithUnion1_db").usingJWPTLeftouterNodes().build();
		final ClassLoader classLoader = getClass().getClassLoader();
		
		final Query query = new Query(classLoader.getResource("queryTestJoinWithUnion1.q").getPath(), statistics, settings);

		//ACTUAL
		final Dataset<Row> actualResult = query.compute(spark().sqlContext()).orderBy("title");
		final Dataset<Row> nullableActualResult = sqlContext().createDataFrame(actualResult.collectAsList(),
				actualResult.schema().asNullable());
		
		assertDataFrameEquals(expectedResult, nullableActualResult);
	}

	private Dataset<Row> initializeDb(final DatabaseStatistics statistics) {
		spark().sql("DROP DATABASE IF EXISTS queryTestJoinWithUnion1_db CASCADE");
		spark().sql("CREATE DATABASE IF NOT EXISTS  queryTestJoinWithUnion1_db");
		spark().sql("USE queryTestJoinWithUnion1_db");

		// creates test tt table
		final TripleBean t1 = new TripleBean();
		t1.setS("<http://example.org/book1>");
		t1.setP("<http://example.org/1/title>");
		t1.setO("Title1");
		
		final TripleBean t2 = new TripleBean();
		t2.setS("<http://example.org/book2>");
		t2.setP("<http://example.org/2/title>");
		t2.setO("Title2");
		
		final TripleBean t3 = new TripleBean();
		t3.setS("<http://example.org/book3>");
		t3.setP("<http://example.org/1/title>");
		t3.setO("Title3");

		final TripleBean t4 = new TripleBean();
		t4.setS("<http://example.org/book3>");
		t4.setP("<http://example.org/2/title>");
		t4.setO("Title4");
		
		final TripleBean t5 = new TripleBean();
		t5.setS("<http://example.org/book1>");
		t5.setP("<http://example.org/1/author>");
		t5.setO("Author1");
		
		final TripleBean t6 = new TripleBean();
		t6.setS("<http://example.org/book2>");
		t6.setP("<http://example.org/2/author>");
		t6.setO("Author2");

		final ArrayList<TripleBean> triplesList = new ArrayList<>();
		triplesList.add(t1);
		triplesList.add(t2);
		triplesList.add(t3);
		triplesList.add(t4);
		triplesList.add(t5);
		triplesList.add(t6);

		final Dataset<Row> ttDataset = spark().createDataset(triplesList, triplesEncoder).select("s", "p", "o").orderBy(
				"s", "p", "o");
		ttDataset.write().saveAsTable("tripletable");
		
		final loader.Settings loaderSettings =
				new loader.Settings.Builder("queryTestJoinWithUnion1_db").withInputPath((System.getProperty(
						"user.dir") + "\\target\\test_output\\JoinWithUnionTest").replace('\\', '/'))
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
//	@Ignore("Unions are not fully implemented yet.")
	public void queryTest2() throws Exception {
		final DatabaseStatistics statistics = new DatabaseStatistics("queryTestJoinWithUnion2_db");
		initializeDb2(statistics);
		final Dataset<Row> expectedResult = createTest2ExpectedDataset();
		queryOnTT2(statistics, expectedResult);
		queryOnVp2(statistics, expectedResult);
		queryOnWpt2(statistics, expectedResult);
		queryOnIwpt2(statistics, expectedResult);
		queryOnJwptOuter2(statistics, expectedResult);
		queryOnJwptLeftOuter2(statistics, expectedResult);
	}

	private Dataset<Row> createTest2ExpectedDataset(){
		final StructType schema = DataTypes.createStructType(new StructField[]{
				DataTypes.createStructField("x", DataTypes.StringType, true),
				DataTypes.createStructField("y", DataTypes.StringType, true),
		});
		final Row row1 = RowFactory.create("Title1", null);
		final Row row2 = RowFactory.create("Title3", null);
		final Row row3 = RowFactory.create(null, "Title2");
		final Row row4 = RowFactory.create(null, "Title4");
		final List<Row> rowList = ImmutableList.of(row1, row2, row3, row4);
		return spark().createDataFrame(rowList, schema).orderBy("x", "y");
	}

	private void queryOnTT2(final DatabaseStatistics statistics, final Dataset<Row> expectedResult)  throws Exception {
		final Settings settings = new Settings.Builder("queryTestJoinWithUnion2_db").usingTTNodes().build();
		final ClassLoader classLoader = getClass().getClassLoader();
		
		final Query query = new Query(classLoader.getResource("queryTestJoinWithUnion2.q").getPath(), statistics, settings);

		//ACTUAL
		final Dataset<Row> actualResult = query.compute(spark().sqlContext()).orderBy("x", "y");
		final Dataset<Row> nullableActualResult = sqlContext().createDataFrame(actualResult.collectAsList(),
				actualResult.schema().asNullable());
		System.out.print("JoinWithUnion: queryTest2");
		expectedResult.printSchema();
		expectedResult.show();

		nullableActualResult.printSchema();
		nullableActualResult.show();
		assertDataFrameEquals(expectedResult, nullableActualResult);
	}
	
	private void queryOnVp2(final DatabaseStatistics statistics, final Dataset<Row> expectedResult)  throws Exception {
		final Settings settings = new Settings.Builder("queryTestJoinWithUnion2_db").usingVPNodes().build();
		final ClassLoader classLoader = getClass().getClassLoader();
		
		final Query query = new Query(classLoader.getResource("queryTestJoinWithUnion2.q").getPath(), statistics, settings);

		//ACTUAL
		final Dataset<Row> actualResult = query.compute(spark().sqlContext()).orderBy("x", "y");
		final Dataset<Row> nullableActualResult = sqlContext().createDataFrame(actualResult.collectAsList(),
				actualResult.schema().asNullable());
		
		assertDataFrameEquals(expectedResult, nullableActualResult);
	}

	private void queryOnWpt2(final DatabaseStatistics statistics, final Dataset<Row> expectedResult)  throws Exception {
		final Settings settings = new Settings.Builder("queryTestJoinWithUnion2_db").usingWPTNodes().build();
		final ClassLoader classLoader = getClass().getClassLoader();
		
		final Query query = new Query(classLoader.getResource("queryTestJoinWithUnion2.q").getPath(), statistics, settings);

		//ACTUAL
		final Dataset<Row> actualResult = query.compute(spark().sqlContext()).orderBy("x", "y");
		final Dataset<Row> nullableActualResult = sqlContext().createDataFrame(actualResult.collectAsList(),
				actualResult.schema().asNullable());
		
		assertDataFrameEquals(expectedResult, nullableActualResult);
	}

	private void queryOnIwpt2(final DatabaseStatistics statistics, final Dataset<Row> expectedResult)  throws Exception {
		final Settings settings = new Settings.Builder("queryTestJoinWithUnion2_db").usingIWPTNodes().build();
		final ClassLoader classLoader = getClass().getClassLoader();
		
		final Query query = new Query(classLoader.getResource("queryTestJoinWithUnion2.q").getPath(), statistics, settings);

		//ACTUAL
		final Dataset<Row> actualResult = query.compute(spark().sqlContext()).orderBy("x", "y");
		final Dataset<Row> nullableActualResult = sqlContext().createDataFrame(actualResult.collectAsList(),
				actualResult.schema().asNullable());
		
		assertDataFrameEquals(expectedResult, nullableActualResult);
	}

	private void queryOnJwptOuter2(final DatabaseStatistics statistics, final Dataset<Row> expectedResult)  throws Exception {
		final Settings settings = new Settings.Builder("queryTestJoinWithUnion2_db").usingJWPTOuterNodes().build();
		final ClassLoader classLoader = getClass().getClassLoader();
		
		final Query query = new Query(classLoader.getResource("queryTestJoinWithUnion2.q").getPath(), statistics, settings);

		//ACTUAL
		final Dataset<Row> actualResult = query.compute(spark().sqlContext()).orderBy("x", "y");
		final Dataset<Row> nullableActualResult = sqlContext().createDataFrame(actualResult.collectAsList(),
				actualResult.schema().asNullable());
		
		assertDataFrameEquals(expectedResult, nullableActualResult);
	}

	private void queryOnJwptLeftOuter2(final DatabaseStatistics statistics, final Dataset<Row> expectedResult)  throws Exception {
		final Settings settings = new Settings.Builder("queryTestJoinWithUnion2_db").usingJWPTLeftouterNodes().build();
		final ClassLoader classLoader = getClass().getClassLoader();
		
		final Query query = new Query(classLoader.getResource("queryTestJoinWithUnion2.q").getPath(), statistics, settings);

		//ACTUAL
		final Dataset<Row> actualResult = query.compute(spark().sqlContext()).orderBy("x", "y");
		final Dataset<Row> nullableActualResult = sqlContext().createDataFrame(actualResult.collectAsList(),
				actualResult.schema().asNullable());
		
		assertDataFrameEquals(expectedResult, nullableActualResult);
	}

	private Dataset<Row> initializeDb2(final DatabaseStatistics statistics) {
		spark().sql("DROP DATABASE IF EXISTS queryTestJoinWithUnion2_db CASCADE");
		spark().sql("CREATE DATABASE IF NOT EXISTS  queryTestJoinWithUnion2_db");
		spark().sql("USE queryTestJoinWithUnion2_db");

		// creates test tt table
		final TripleBean t1 = new TripleBean();
		t1.setS("<http://example.org/book1>");
		t1.setP("<http://example.org/1/title>");
		t1.setO("Title1");
		
		final TripleBean t2 = new TripleBean();
		t2.setS("<http://example.org/book2>");
		t2.setP("<http://example.org/2/title>");
		t2.setO("Title2");
		
		final TripleBean t3 = new TripleBean();
		t3.setS("<http://example.org/book3>");
		t3.setP("<http://example.org/1/title>");
		t3.setO("Title3");

		final TripleBean t4 = new TripleBean();
		t4.setS("<http://example.org/book3>");
		t4.setP("<http://example.org/2/title>");
		t4.setO("Title4");
		
		final TripleBean t5 = new TripleBean();
		t5.setS("<http://example.org/book1>");
		t5.setP("<http://example.org/1/author>");
		t5.setO("Author1");
		
		final TripleBean t6 = new TripleBean();
		t6.setS("<http://example.org/book2>");
		t6.setP("<http://example.org/2/author>");
		t6.setO("Author2");

		final ArrayList<TripleBean> triplesList = new ArrayList<>();
		triplesList.add(t1);
		triplesList.add(t2);
		triplesList.add(t3);
		triplesList.add(t4);
		triplesList.add(t5);
		triplesList.add(t6);

		final Dataset<Row> ttDataset = spark().createDataset(triplesList, triplesEncoder).select("s", "p", "o").orderBy(
				"s", "p", "o");
		ttDataset.write().saveAsTable("tripletable");
		
		final loader.Settings loaderSettings =
				new loader.Settings.Builder("queryTestJoinWithUnion2_db").withInputPath((System.getProperty(
						"user.dir") + "\\target\\test_output\\JoinWithUnionTest").replace('\\', '/'))
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
				spark(), JoinedWidePropertyTableLoader.JoinType.inner, statistics);*/
		jwptLeftOuterLoader.load();

		return ttDataset;
	}
	
	@Test
	public void queryTest3() throws Exception {
		final DatabaseStatistics statistics = new DatabaseStatistics("queryTestJoinWithUnion3_db");
		initializeDb3(statistics);
		final Dataset<Row> expectedResult = createTest3ExpectedDataset();

		queryOnTT3(statistics, expectedResult);
		queryOnVp3(statistics, expectedResult);
		queryOnWpt3(statistics, expectedResult);
		queryOnIwpt3(statistics, expectedResult);
		queryOnJwptOuter3(statistics, expectedResult);
		queryOnJwptLeftOuter3(statistics, expectedResult);
	}

	private Dataset<Row> createTest3ExpectedDataset(){
		final StructType schema = DataTypes.createStructType(new StructField[]{
				DataTypes.createStructField("title", DataTypes.StringType, true),
				DataTypes.createStructField("author", DataTypes.StringType, true),
		});
		final Row row1 = RowFactory.create("Title1", "Author1");
		final Row row2 = RowFactory.create("Title2", "Author2");
		final List<Row> rowList = ImmutableList.of(row1, row2);
		return spark().createDataFrame(rowList, schema).orderBy("title");
	}

	private void queryOnTT3(final DatabaseStatistics statistics, final Dataset<Row> expectedResult)  throws Exception {
		final Settings settings = new Settings.Builder("queryTestJoinWithUnion3_db").usingTTNodes().build();
		final ClassLoader classLoader = getClass().getClassLoader();
		
		final Query query = new Query(classLoader.getResource("queryTestJoinWithUnion3.q").getPath(), statistics, settings);

		//ACTUAL
		final Dataset<Row> actualResult = query.compute(spark().sqlContext()).orderBy("title");
		final Dataset<Row> nullableActualResult = sqlContext().createDataFrame(actualResult.collectAsList(),
				actualResult.schema().asNullable());
		System.out.print("JoinWithUnion: queryTest3");
		expectedResult.printSchema();
		expectedResult.show();

		nullableActualResult.printSchema();
		nullableActualResult.show();
		assertDataFrameEquals(expectedResult, nullableActualResult);
	}
	
	private void queryOnVp3(final DatabaseStatistics statistics, final Dataset<Row> expectedResult)  throws Exception {
		final Settings settings = new Settings.Builder("queryTestJoinWithUnion3_db").usingVPNodes().build();
		final ClassLoader classLoader = getClass().getClassLoader();
		
		final Query query = new Query(classLoader.getResource("queryTestJoinWithUnion3.q").getPath(), statistics, settings);

		//ACTUAL
		final Dataset<Row> actualResult = query.compute(spark().sqlContext()).orderBy("title");
		final Dataset<Row> nullableActualResult = sqlContext().createDataFrame(actualResult.collectAsList(),
				actualResult.schema().asNullable());
		
		assertDataFrameEquals(expectedResult, nullableActualResult);
	}

	private void queryOnWpt3(final DatabaseStatistics statistics, final Dataset<Row> expectedResult)  throws Exception {
		final Settings settings = new Settings.Builder("queryTestJoinWithUnion3_db").usingWPTNodes().build();
		final ClassLoader classLoader = getClass().getClassLoader();
		
		final Query query = new Query(classLoader.getResource("queryTestJoinWithUnion3.q").getPath(), statistics, settings);

		//ACTUAL
		final Dataset<Row> actualResult = query.compute(spark().sqlContext()).orderBy("title");
		final Dataset<Row> nullableActualResult = sqlContext().createDataFrame(actualResult.collectAsList(),
				actualResult.schema().asNullable());
		
		assertDataFrameEquals(expectedResult, nullableActualResult);
	}

	private void queryOnIwpt3(final DatabaseStatistics statistics, final Dataset<Row> expectedResult)  throws Exception {
		final Settings settings = new Settings.Builder("queryTestJoinWithUnion3_db").usingIWPTNodes().build();
		final ClassLoader classLoader = getClass().getClassLoader();
		
		final Query query = new Query(classLoader.getResource("queryTestJoinWithUnion3.q").getPath(), statistics, settings);

		//ACTUAL
		final Dataset<Row> actualResult = query.compute(spark().sqlContext()).orderBy("title");
		final Dataset<Row> nullableActualResult = sqlContext().createDataFrame(actualResult.collectAsList(),
				actualResult.schema().asNullable());
		
		assertDataFrameEquals(expectedResult, nullableActualResult);
	}

	private void queryOnJwptOuter3(final DatabaseStatistics statistics, final Dataset<Row> expectedResult)  throws Exception {
		final Settings settings = new Settings.Builder("queryTestJoinWithUnion3_db").usingJWPTOuterNodes().build();
		final ClassLoader classLoader = getClass().getClassLoader();
		
		final Query query = new Query(classLoader.getResource("queryTestJoinWithUnion3.q").getPath(), statistics, settings);

		//ACTUAL
		final Dataset<Row> actualResult = query.compute(spark().sqlContext()).orderBy("title");
		final Dataset<Row> nullableActualResult = sqlContext().createDataFrame(actualResult.collectAsList(),
				actualResult.schema().asNullable());
		
		assertDataFrameEquals(expectedResult, nullableActualResult);
	}

	private void queryOnJwptLeftOuter3(final DatabaseStatistics statistics, final Dataset<Row> expectedResult)  throws Exception {
		final Settings settings = new Settings.Builder("queryTestJoinWithUnion3_db").usingJWPTLeftouterNodes().build();
		final ClassLoader classLoader = getClass().getClassLoader();
		
		final Query query = new Query(classLoader.getResource("queryTestJoinWithUnion3.q").getPath(), statistics, settings);

		//ACTUAL
		final Dataset<Row> actualResult = query.compute(spark().sqlContext()).orderBy("title");
		final Dataset<Row> nullableActualResult = sqlContext().createDataFrame(actualResult.collectAsList(),
				actualResult.schema().asNullable());
		
		assertDataFrameEquals(expectedResult, nullableActualResult);
	}

	private Dataset<Row> initializeDb3(final DatabaseStatistics statistics) {
		spark().sql("DROP DATABASE IF EXISTS queryTestJoinWithUnion3_db CASCADE");
		spark().sql("CREATE DATABASE IF NOT EXISTS  queryTestJoinWithUnion3_db");
		spark().sql("USE queryTestJoinWithUnion3_db");

		// creates test tt table
		final TripleBean t1 = new TripleBean();
		t1.setS("<http://example.org/book1>");
		t1.setP("<http://example.org/1/title>");
		t1.setO("Title1");
		
		final TripleBean t2 = new TripleBean();
		t2.setS("<http://example.org/book2>");
		t2.setP("<http://example.org/2/title>");
		t2.setO("Title2");
		
		final TripleBean t3 = new TripleBean();
		t3.setS("<http://example.org/book3>");
		t3.setP("<http://example.org/1/title>");
		t3.setO("Title3");

		final TripleBean t4 = new TripleBean();
		t4.setS("<http://example.org/book3>");
		t4.setP("<http://example.org/2/title>");
		t4.setO("Title4");
		
		final TripleBean t5 = new TripleBean();
		t5.setS("<http://example.org/book1>");
		t5.setP("<http://example.org/1/author>");
		t5.setO("Author1");
		
		final TripleBean t6 = new TripleBean();
		t6.setS("<http://example.org/book2>");
		t6.setP("<http://example.org/2/author>");
		t6.setO("Author2");

		final ArrayList<TripleBean> triplesList = new ArrayList<>();
		triplesList.add(t1);
		triplesList.add(t2);
		triplesList.add(t3);
		triplesList.add(t4);
		triplesList.add(t5);
		triplesList.add(t6);

		final Dataset<Row> ttDataset = spark().createDataset(triplesList, triplesEncoder).select("s", "p", "o").orderBy(
				"s", "p", "o");
		ttDataset.write().saveAsTable("tripletable");
		
		final loader.Settings loaderSettings =
				new loader.Settings.Builder("queryTestJoinWithUnion3_db").withInputPath((System.getProperty(
						"user.dir") + "\\target\\test_output\\JoinWithUnionTest").replace('\\', '/'))
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
PREFIX ex1: <http://example.org/1/#>.
PREFIX ex2: <http://example.org/2/#>.

TABLE:
================================================================================================================
ex:book1		| ex1:title			| "Title1"
ex:book1		| ex1:author		| "Author1"

ex:book2		| ex2:title			| "Title2"
ex:book2		| ex2:author		| "Author2"

ex:book3		| ex1:title			| "Title3"
ex:book3		| ex2:title			| "Title4"
================================================================================================================

QUERY: Joining Patterns with UNION
-----------------------------------------------------------------------------------------------------------------
SELECT ?title
WHERE  { { ?book <http://example.org/1/title>  ?title } UNION { ?book <http://example.org/2/title>  ?title } }
-----------------------------------------------------------------------------------------------------------------

RESULT:
-----------------------------------------------------------------------------------------------------------------
Expected:
+------+
| title|
+------+
|Title1|
|Title3|
|Title2|
|Title4|
+------+

Actual:
+------+
| title|
+------+
|Title2|
|Title4|
+------+
java.lang.AssertionError: Not Equal Sample: 
(1,([Title3],[Title2])), (2,([Title2],[Title3]))

-----------------------------------------------------------------------------------------------------------------
*/

/*
PREFIX ex: <http://example.org/#>.
PREFIX ex1: <http://example.org/1/#>.
PREFIX ex2: <http://example.org/2/#>.

TABLE:
================================================================================================================
ex:book1		| ex1:title			| "Title1"
ex:book1		| ex1:author		| "Author1"

ex:book2		| ex2:title			| "Title2"
ex:book2		| ex2:author		| "Author2"

ex:book3		| ex1:title			| "Title3"
ex:book3		| ex2:title			| "Title4"
================================================================================================================

QUERY: Joining Patterns with UNION
-----------------------------------------------------------------------------------------------------------------
SELECT ?x ?y
WHERE  { { ?book <http://example.org/1/title>  ?x } UNION { ?book <http://example.org/2/title>  ?y } }
-----------------------------------------------------------------------------------------------------------------

RESULT:
-----------------------------------------------------------------------------------------------------------------
Expected:
+------+------+
|     x|     y|
+------+------+
|Title1|  null|
|Title3|  null|
|  null|Title2|
|  null|Title4|
+------+------+

Actual:
+------+------+
|     x|     y|
+------+------+
|  null|Title2|
|  null|Title4|
|Title1|  null|
|Title3|  null|
+------+------+

Error:
java.lang.AssertionError: Not Equal Sample: 
(0,([Title1,null],[null,Title2])), (1,([Title3,null],[null,Title4])), 
(2,([null,Title2],[Title1,null])), (3,([null,Title4],[Title3,null]))

-----------------------------------------------------------------------------------------------------------------
*/

/*
PREFIX ex: <http://example.org/#>.
PREFIX ex1: <http://example.org/1/#>.
PREFIX ex2: <http://example.org/2/#>.

TABLE:
================================================================================================================
ex:book1		| ex1:title			| "Title1"
ex:book1		| ex1:author		| "Author1"

ex:book2		| ex2:title			| "Title2"
ex:book2		| ex2:author		| "Author2"

ex:book3		| ex1:title			| "Title3"
ex:book3		| ex2:title			| "Title4"
================================================================================================================

QUERY: Joining Patterns with UNION
-----------------------------------------------------------------------------------------------------------------
SELECT ?title ?author
WHERE  { { ?book <http://example.org/1/title> ?title .  ?book <http://example.org/1/author> ?author }
         UNION
         { ?book <http://example.org/2/title> ?title .  ?book <http://example.org/2/author> ?author }
       }
-----------------------------------------------------------------------------------------------------------------

RESULT:
-----------------------------------------------------------------------------------------------------------------

Expected:
+------+-------+
| title| author|
+------+-------+
|Title1|Author1|
|Title2|Author2|
+------+-------+

Actual:
+------+-------+
| title| author|
+------+-------+
|Title2|Author2|
+------+-------+
-----------------------------------------------------------------------------------------------------------------
?
-----------------------------------------------------------------------------------------------------------------
*/
