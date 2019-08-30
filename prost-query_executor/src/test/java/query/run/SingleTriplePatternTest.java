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
public class SingleTriplePatternTest extends JavaDataFrameSuiteBase implements Serializable {
	private static final long serialVersionUID = 1329L;
	private static final Encoder<TripleBean> triplesEncoder = Encoders.bean(TripleBean.class);

	@Test
	public void queryTest() throws Exception {
		final DatabaseStatistics statistics = new DatabaseStatistics("queryTestSingleTriplePattern1_db");
		Dataset<Row> fullDataset = initializeDb(statistics);
		fullDataset = fullDataset.orderBy("s", "p", "o");
		queryOnTT(statistics, fullDataset);
//		queryOnVp(statistics, fullDataset);
//		queryOnWpt(statistics, fullDataset);
//		queryOnIwpt(statistics, fullDataset);
//		queryOnJwptOuter(statistics, fullDataset);
//		queryOnJwptLeftOuter(statistics, fullDataset);
	}

	private void queryOnTT(final DatabaseStatistics statistics, final Dataset<Row> fullDataset)  throws Exception {
		final Settings settings = new Settings.Builder("queryTestSingleTriplePattern1_db").usingTTNodes().build();
		final ClassLoader classLoader = getClass().getClassLoader();		
		final Query query = new Query(classLoader.getResource("queryTestSingleTriple1.q").getPath(), statistics, settings);
				
		//EXPECTED
		final StructType schema = DataTypes.createStructType(new StructField[]{
				DataTypes.createStructField("book", DataTypes.StringType, true),
				DataTypes.createStructField("p", DataTypes.StringType, true),
				DataTypes.createStructField("title", DataTypes.StringType, true),
				});
		final Row row1 = RowFactory.create("<http://example.org/book1>", "<http://example.org/title>", "Title1");
		final Row row2 = RowFactory.create("<http://example.org/book2>", "<http://example.org/title>", "Title2");
		final List<Row> rowList = ImmutableList.of(row1, row2);
		final Dataset<Row> expectedResult = spark().createDataFrame(rowList, schema);
		
		//ACTUAL
		final Dataset<Row> actualResult = query.compute(spark().sqlContext()).orderBy("book", "p", "title");
		final Dataset<Row> nullableActualResult = sqlContext().createDataFrame(actualResult.collectAsList(),
				actualResult.schema().asNullable());
		System.out.print("SingleTriplePatternTest: queryTest1");
		expectedResult.printSchema();
		expectedResult.show();	
		nullableActualResult.printSchema();
		nullableActualResult.show();
		assertDataFrameEquals(expectedResult, nullableActualResult);
	}
	

	
	private void queryOnVp(final DatabaseStatistics statistics, final Dataset<Row> fullDataset)  throws Exception {
		final Settings settings = new Settings.Builder("queryTestSingleTriplePattern1_db").usingVPNodes().build();
		final ClassLoader classLoader = getClass().getClassLoader();
		final Query query = new Query(classLoader.getResource("queryTestSingleTriple1.q").getPath(), statistics, settings);
				
		//EXPECTED
		final StructType schema = DataTypes.createStructType(new StructField[]{
				DataTypes.createStructField("book", DataTypes.StringType, true),
				});
		final Row row1 = RowFactory.create("<http://example.org/book1>");
		final Row row2 = RowFactory.create("<http://example.org/book2>");
		final List<Row> rowList = ImmutableList.of(row1, row2);
		final Dataset<Row> expectedResult = spark().createDataFrame(rowList, schema);
		
		//ACTUAL
		final Dataset<Row> actualResult = query.compute(spark().sqlContext()).orderBy("book");
		final Dataset<Row> nullableActualResult = sqlContext().createDataFrame(actualResult.collectAsList(),
				actualResult.schema().asNullable());
		
		assertDataFrameEquals(expectedResult, nullableActualResult);
	}
	
	private void queryOnWpt(final DatabaseStatistics statistics, final Dataset<Row> fullDataset)  throws Exception {
		final Settings settings = new Settings.Builder("queryTestSingleTriplePattern1_db").usingWPTNodes().build();
		final ClassLoader classLoader = getClass().getClassLoader();
		final Query query = new Query(classLoader.getResource("queryTestSingleTriple1.q").getPath(), statistics,
			      settings);
				
		//EXPECTED
		final StructType schema = DataTypes.createStructType(new StructField[]{
				DataTypes.createStructField("book", DataTypes.StringType, true),
				});
		final Row row1 = RowFactory.create("<http://example.org/book1>");
		final Row row2 = RowFactory.create("<http://example.org/book2>");
		final List<Row> rowList = ImmutableList.of(row1, row2);
		final Dataset<Row> expectedResult = spark().createDataFrame(rowList, schema);
		
		//ACTUAL
		final Dataset<Row> actualResult = query.compute(spark().sqlContext()).orderBy("book");
		final Dataset<Row> nullableActualResult = sqlContext().createDataFrame(actualResult.collectAsList(),
				actualResult.schema().asNullable());
		
		assertDataFrameEquals(expectedResult, nullableActualResult);
	}

	private void queryOnIwpt(final DatabaseStatistics statistics, final Dataset<Row> fullDataset)  throws Exception {
		final Settings settings = new Settings.Builder("queryTestSingleTriplePattern1_db").usingIWPTNodes().build();
		final ClassLoader classLoader = getClass().getClassLoader();
		final Query query = new Query(classLoader.getResource("queryTestSingleTriple1.q").getPath(), statistics,
			      settings);
				
		//EXPECTED
		final StructType schema = DataTypes.createStructType(new StructField[]{
				DataTypes.createStructField("book", DataTypes.StringType, true),
				});
		final Row row1 = RowFactory.create("<http://example.org/book1>");
		final Row row2 = RowFactory.create("<http://example.org/book2>");
		final List<Row> rowList = ImmutableList.of(row1, row2);
		final Dataset<Row> expectedResult = spark().createDataFrame(rowList, schema);
		
		//ACTUAL
		final Dataset<Row> actualResult = query.compute(spark().sqlContext()).orderBy("book");
		final Dataset<Row> nullableActualResult = sqlContext().createDataFrame(actualResult.collectAsList(),
				actualResult.schema().asNullable());
		
		assertDataFrameEquals(expectedResult, nullableActualResult);
	}

	private void queryOnJwptOuter(final DatabaseStatistics statistics, final Dataset<Row> fullDataset)  throws Exception {
		final Settings settings = new Settings.Builder("queryTestSingleTriplePattern1_db").usingJWPTOuterNodes().build();
		final ClassLoader classLoader = getClass().getClassLoader();
		final Query query = new Query(classLoader.getResource("queryTestSingleTriple1.q").getPath(), statistics,
			      settings);
				
		//EXPECTED
		final StructType schema = DataTypes.createStructType(new StructField[]{
				DataTypes.createStructField("book", DataTypes.StringType, true),
				});
		final Row row1 = RowFactory.create("<http://example.org/book1>");
		final Row row2 = RowFactory.create("<http://example.org/book2>");
		final List<Row> rowList = ImmutableList.of(row1, row2);
		final Dataset<Row> expectedResult = spark().createDataFrame(rowList, schema);
		
		//ACTUAL
		final Dataset<Row> actualResult = query.compute(spark().sqlContext()).orderBy("book");
		final Dataset<Row> nullableActualResult = sqlContext().createDataFrame(actualResult.collectAsList(),
				actualResult.schema().asNullable());
		
		assertDataFrameEquals(expectedResult, nullableActualResult);
	}

	private void queryOnJwptLeftOuter(final DatabaseStatistics statistics, final Dataset<Row> fullDataset)  throws Exception {
		final Settings settings = new Settings.Builder("queryTestSingleTriplePattern1_db").usingJWPTLeftouterNodes().build();
		final ClassLoader classLoader = getClass().getClassLoader();
		final Query query = new Query(classLoader.getResource("queryTestSingleTriple1.q").getPath(), statistics,
			      settings);
				
		//EXPECTED
		final StructType schema = DataTypes.createStructType(new StructField[]{
				DataTypes.createStructField("book", DataTypes.StringType, true),
				});
		final Row row1 = RowFactory.create("<http://example.org/book1>");
		final Row row2 = RowFactory.create("<http://example.org/book2>");
		final List<Row> rowList = ImmutableList.of(row1, row2);
		final Dataset<Row> expectedResult = spark().createDataFrame(rowList, schema);
		
		//ACTUAL
		final Dataset<Row> actualResult = query.compute(spark().sqlContext()).orderBy("book");
		final Dataset<Row> nullableActualResult = sqlContext().createDataFrame(actualResult.collectAsList(),
				actualResult.schema().asNullable());
		
		assertDataFrameEquals(expectedResult, nullableActualResult);
	}
	

	private Dataset<Row> initializeDb(final DatabaseStatistics statistics) {
		spark().sql("DROP DATABASE IF EXISTS queryTestSingleTriplePattern1_db CASCADE");
		spark().sql("CREATE DATABASE IF NOT EXISTS  queryTestSingleTriplePattern1_db");
		spark().sql("USE queryTestSingleTriplePattern1_db");

				
		// creates test tt table
		final TripleBean t1 = new TripleBean();
		t1.setS("<http://example.org/book1>");
		t1.setP("<http://example.org/title>");
		t1.setO("Title1");

		final TripleBean t2 = new TripleBean();
		t2.setS("<http://example.org/book2>");
		t2.setP("<http://example.org/title>");
		t2.setO("Title2");

		final ArrayList<TripleBean> triplesList = new ArrayList<>();
		triplesList.add(t1);
		triplesList.add(t2);

		final Dataset<Row> ttDataset = spark().createDataset(triplesList, triplesEncoder).select("s", "p", "o").orderBy(
				"s", "p", "o");
		ttDataset.write().saveAsTable("tripletable");

		final loader.Settings loaderSettings =
				new loader.Settings.Builder("queryTestSingleTriplePattern1_db").withInputPath((System.getProperty(
						"user.dir") + "\\target\\test_output\\SingleTriplePatternTest").replace('\\', '/'))
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
	public void queryTest2() throws Exception {
		final DatabaseStatistics statistics = new DatabaseStatistics("queryTestSingleTriplePattern2_db");
		Dataset<Row> fullDataset = initializeDb2(statistics);
		fullDataset = fullDataset.orderBy("s", "p", "o");
		queryOnTT2(statistics, fullDataset);
		queryOnVp2(statistics, fullDataset);
		queryOnWpt2(statistics, fullDataset);
		queryOnIwpt2(statistics, fullDataset);
		queryOnJwptOuter2(statistics, fullDataset);
		queryOnJwptLeftOuter2(statistics, fullDataset);
	}
	
	  
	private void queryOnTT2(final DatabaseStatistics statistics, final Dataset<Row> fullDataset)  throws Exception {
		final Settings settings = new Settings.Builder("queryTestSingleTriplePattern2_db").usingTTNodes().build();
		final ClassLoader classLoader = getClass().getClassLoader();
		final Query query = new Query(classLoader.getResource("queryTestSingleTriple2.q").getPath(), statistics,
			      settings);
				
		//EXPECTED
		final StructType schema = DataTypes.createStructType(new StructField[]{
				DataTypes.createStructField("p", DataTypes.StringType, true),
				});
		final Row row1 = RowFactory.create("<http://example.org/title>");
		final Row row2 = RowFactory.create("<http://example.org/title>");
		final List<Row> rowList = ImmutableList.of(row1, row2);
		final Dataset<Row> expectedResult = spark().createDataFrame(rowList, schema);
		
		//ACTUAL
		final Dataset<Row> actualResult = query.compute(spark().sqlContext()).orderBy("p");
		final Dataset<Row> nullableActualResult = sqlContext().createDataFrame(actualResult.collectAsList(),
				actualResult.schema().asNullable());
		
		System.out.print("SingleTriplePatternTest: queryTest2");
		expectedResult.printSchema();
		expectedResult.show();	
		nullableActualResult.printSchema();
		nullableActualResult.show();		
		assertDataFrameEquals(expectedResult, nullableActualResult);
	}
	
	private void queryOnVp2(final DatabaseStatistics statistics, final Dataset<Row> fullDataset)  throws Exception {
		final Settings settings = new Settings.Builder("queryTestSingleTriplePattern2_db").usingVPNodes().build();
		final ClassLoader classLoader = getClass().getClassLoader();
		final Query query = new Query(classLoader.getResource("queryTestSingleTriple2.q").getPath(), statistics,
			      settings);
				
		//EXPECTED
		final StructType schema = DataTypes.createStructType(new StructField[]{
				DataTypes.createStructField("p", DataTypes.StringType, true),
				});
		final Row row1 = RowFactory.create("<http://example.org/title>");
		final Row row2 = RowFactory.create("<http://example.org/title>");
		final List<Row> rowList = ImmutableList.of(row1, row2);
		final Dataset<Row> expectedResult = spark().createDataFrame(rowList, schema);
		
		//ACTUAL
		final Dataset<Row> actualResult = query.compute(spark().sqlContext()).orderBy("p");
		final Dataset<Row> nullableActualResult = sqlContext().createDataFrame(actualResult.collectAsList(),
				actualResult.schema().asNullable());
		
		assertDataFrameEquals(expectedResult, nullableActualResult);
	}

	private void queryOnWpt2(final DatabaseStatistics statistics, final Dataset<Row> fullDataset)  throws Exception {
		final Settings settings = new Settings.Builder("queryTestSingleTriplePattern2_db").usingWPTNodes().build();
		final ClassLoader classLoader = getClass().getClassLoader();
		final Query query = new Query(classLoader.getResource("queryTestSingleTriple2.q").getPath(), statistics,
			      settings);
				
		//EXPECTED
		final StructType schema = DataTypes.createStructType(new StructField[]{
				DataTypes.createStructField("p", DataTypes.StringType, true),
				});
		final Row row1 = RowFactory.create("<http://example.org/title>");
		final Row row2 = RowFactory.create("<http://example.org/title>");
		final List<Row> rowList = ImmutableList.of(row1, row2);
		final Dataset<Row> expectedResult = spark().createDataFrame(rowList, schema);
		
		//ACTUAL
		final Dataset<Row> actualResult = query.compute(spark().sqlContext()).orderBy("p");
		final Dataset<Row> nullableActualResult = sqlContext().createDataFrame(actualResult.collectAsList(),
				actualResult.schema().asNullable());
				
		assertDataFrameEquals(expectedResult, nullableActualResult);
	}

	private void queryOnIwpt2(final DatabaseStatistics statistics, final Dataset<Row> fullDataset)  throws Exception {
		final Settings settings = new Settings.Builder("queryTestSingleTriplePattern2_db").usingIWPTNodes().build();
		final ClassLoader classLoader = getClass().getClassLoader();
		final Query query = new Query(classLoader.getResource("queryTestSingleTriple2.q").getPath(), statistics,
			      settings);
				
		//EXPECTED
		final StructType schema = DataTypes.createStructType(new StructField[]{
				DataTypes.createStructField("p", DataTypes.StringType, true),
				});
		final Row row1 = RowFactory.create("<http://example.org/title>");
		final Row row2 = RowFactory.create("<http://example.org/title>");
		final List<Row> rowList = ImmutableList.of(row1, row2);
		final Dataset<Row> expectedResult = spark().createDataFrame(rowList, schema);
		
		//ACTUAL
		final Dataset<Row> actualResult = query.compute(spark().sqlContext()).orderBy("p");
		final Dataset<Row> nullableActualResult = sqlContext().createDataFrame(actualResult.collectAsList(),
				actualResult.schema().asNullable());
				
		assertDataFrameEquals(expectedResult, nullableActualResult);
	}

	private void queryOnJwptOuter2(final DatabaseStatistics statistics, final Dataset<Row> fullDataset)  throws Exception {
		final Settings settings = new Settings.Builder("queryTestSingleTriplePattern2_db").usingJWPTOuterNodes().build();
		final ClassLoader classLoader = getClass().getClassLoader();
		final Query query = new Query(classLoader.getResource("queryTestSingleTriple2.q").getPath(), statistics,
			      settings);
				
		//EXPECTED
		final StructType schema = DataTypes.createStructType(new StructField[]{
				DataTypes.createStructField("p", DataTypes.StringType, true),
				});
		final Row row1 = RowFactory.create("<http://example.org/title>");
		final Row row2 = RowFactory.create("<http://example.org/title>");
		final List<Row> rowList = ImmutableList.of(row1, row2);
		final Dataset<Row> expectedResult = spark().createDataFrame(rowList, schema);
		
		//ACTUAL
		final Dataset<Row> actualResult = query.compute(spark().sqlContext()).orderBy("p");
		final Dataset<Row> nullableActualResult = sqlContext().createDataFrame(actualResult.collectAsList(),
				actualResult.schema().asNullable());
				
		assertDataFrameEquals(expectedResult, nullableActualResult);
	}

	private void queryOnJwptLeftOuter2(final DatabaseStatistics statistics, final Dataset<Row> fullDataset)  throws Exception {
		final Settings settings = new Settings.Builder("queryTestSingleTriplePattern2_db").usingJWPTLeftouterNodes().build();
		final ClassLoader classLoader = getClass().getClassLoader();
		final Query query = new Query(classLoader.getResource("queryTestSingleTriple2.q").getPath(), statistics,
			      settings);
				
		//EXPECTED
		final StructType schema = DataTypes.createStructType(new StructField[]{
				DataTypes.createStructField("p", DataTypes.StringType, true),
				});
		final Row row1 = RowFactory.create("<http://example.org/title>");
		final Row row2 = RowFactory.create("<http://example.org/title>");
		final List<Row> rowList = ImmutableList.of(row1, row2);
		final Dataset<Row> expectedResult = spark().createDataFrame(rowList, schema);
		
		//ACTUAL
		final Dataset<Row> actualResult = query.compute(spark().sqlContext()).orderBy("p");
		final Dataset<Row> nullableActualResult = sqlContext().createDataFrame(actualResult.collectAsList(),
				actualResult.schema().asNullable());
				
		assertDataFrameEquals(expectedResult, nullableActualResult);
	}

	private Dataset<Row> initializeDb2(final DatabaseStatistics statistics) {
		spark().sql("DROP DATABASE IF EXISTS queryTestSingleTriplePattern2_db CASCADE");
		spark().sql("CREATE DATABASE IF NOT EXISTS  queryTestSingleTriplePattern2_db");
		spark().sql("USE queryTestSingleTriplePattern2_db");

				
		// creates test tt table
		final TripleBean t1 = new TripleBean();
		t1.setS("<http://example.org/book1>");
		t1.setP("<http://example.org/title>");
		t1.setO("Title1");

		final TripleBean t2 = new TripleBean();
		t2.setS("<http://example.org/book2>");
		t2.setP("<http://example.org/title>");
		t2.setO("Title2");

		final ArrayList<TripleBean> triplesList = new ArrayList<>();
		triplesList.add(t1);
		triplesList.add(t2);

		final Dataset<Row> ttDataset = spark().createDataset(triplesList, triplesEncoder).select("s", "p", "o").orderBy(
				"s", "p", "o");
		ttDataset.write().saveAsTable("tripletable");

		final loader.Settings loaderSettings =
				new loader.Settings.Builder("queryTestSingleTriplePattern2_db").withInputPath((System.getProperty(
						"user.dir") + "\\target\\test_output\\SingleTriplePatternTest").replace('\\', '/'))
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
	public void queryTest3() throws Exception {
		final DatabaseStatistics statistics = new DatabaseStatistics("queryTestSingleTriplePattern3_db");
		Dataset<Row> fullDataset = initializeDb3(statistics);
		fullDataset = fullDataset.orderBy("s", "p", "o");
		queryOnTT3(statistics, fullDataset);
		queryOnVp3(statistics, fullDataset);
		queryOnWpt3(statistics, fullDataset);
		queryOnIwpt3(statistics, fullDataset);
		queryOnJwptOuter3(statistics, fullDataset);
		queryOnJwptLeftOuter3(statistics, fullDataset);
	}

	private void queryOnTT3(final DatabaseStatistics statistics, final Dataset<Row> fullDataset)  throws Exception {
		final Settings settings = new Settings.Builder("queryTestSingleTriplePattern3_db").usingTTNodes().build();
		final ClassLoader classLoader = getClass().getClassLoader();
		final Query query = new Query(classLoader.getResource("queryTestSingleTriple3.q").getPath(), statistics,
			      settings);
				
		//EXPECTED
		final StructType schema = DataTypes.createStructType(new StructField[]{
				DataTypes.createStructField("title", DataTypes.StringType, true),
				});
		final Row row1 = RowFactory.create("Title1");
		final Row row2 = RowFactory.create("Title2");
		final List<Row> rowList = ImmutableList.of(row1, row2);
		final Dataset<Row> expectedResult = spark().createDataFrame(rowList, schema);
		
		//ACTUAL
		final Dataset<Row> actualResult = query.compute(spark().sqlContext()).orderBy("title");
		final Dataset<Row> nullableActualResult = sqlContext().createDataFrame(actualResult.collectAsList(),
				actualResult.schema().asNullable());
		
		System.out.print("SingleTriplePatternTest: queryTest3");
		expectedResult.printSchema();
		expectedResult.show();	
		nullableActualResult.printSchema();
		nullableActualResult.show();
		assertDataFrameEquals(expectedResult, nullableActualResult);
	}
	
	private void queryOnVp3(final DatabaseStatistics statistics, final Dataset<Row> fullDataset)  throws Exception {
		final Settings settings = new Settings.Builder("queryTestSingleTriplePattern3_db").usingVPNodes().build();
		final ClassLoader classLoader = getClass().getClassLoader();
		final Query query = new Query(classLoader.getResource("queryTestSingleTriple3.q").getPath(), statistics,
			      settings);
				
		//EXPECTED
		final StructType schema = DataTypes.createStructType(new StructField[]{
				DataTypes.createStructField("title", DataTypes.StringType, true),
				});
		final Row row1 = RowFactory.create("Title1");
		final Row row2 = RowFactory.create("Title2");
		final List<Row> rowList = ImmutableList.of(row1, row2);
		final Dataset<Row> expectedResult = spark().createDataFrame(rowList, schema);
		
		//ACTUAL
		final Dataset<Row> actualResult = query.compute(spark().sqlContext()).orderBy("title");
		final Dataset<Row> nullableActualResult = sqlContext().createDataFrame(actualResult.collectAsList(),
				actualResult.schema().asNullable());
	}

	private void queryOnWpt3(final DatabaseStatistics statistics, final Dataset<Row> fullDataset)  throws Exception {
		final Settings settings = new Settings.Builder("queryTestSingleTriplePattern3_db").usingWPTNodes().build();
		final ClassLoader classLoader = getClass().getClassLoader();
		final Query query = new Query(classLoader.getResource("queryTestSingleTriple3.q").getPath(), statistics,
			      settings);
				
		//EXPECTED
		final StructType schema = DataTypes.createStructType(new StructField[]{
				DataTypes.createStructField("title", DataTypes.StringType, true),
				});
		final Row row1 = RowFactory.create("Title1");
		final Row row2 = RowFactory.create("Title2");
		final List<Row> rowList = ImmutableList.of(row1, row2);
		final Dataset<Row> expectedResult = spark().createDataFrame(rowList, schema);
		
		//ACTUAL
		final Dataset<Row> actualResult = query.compute(spark().sqlContext()).orderBy("title");
		final Dataset<Row> nullableActualResult = sqlContext().createDataFrame(actualResult.collectAsList(),
				actualResult.schema().asNullable());
	}

	private void queryOnIwpt3(final DatabaseStatistics statistics, final Dataset<Row> fullDataset)  throws Exception {
		final Settings settings = new Settings.Builder("queryTestSingleTriplePattern3_db").usingIWPTNodes().build();
		final ClassLoader classLoader = getClass().getClassLoader();
		final Query query = new Query(classLoader.getResource("queryTestSingleTriple3.q").getPath(), statistics,
			      settings);
				
		//EXPECTED
		final StructType schema = DataTypes.createStructType(new StructField[]{
				DataTypes.createStructField("title", DataTypes.StringType, true),
				});
		final Row row1 = RowFactory.create("Title1");
		final Row row2 = RowFactory.create("Title2");
		final List<Row> rowList = ImmutableList.of(row1, row2);
		final Dataset<Row> expectedResult = spark().createDataFrame(rowList, schema);
		
		//ACTUAL
		final Dataset<Row> actualResult = query.compute(spark().sqlContext()).orderBy("title");
		final Dataset<Row> nullableActualResult = sqlContext().createDataFrame(actualResult.collectAsList(),
				actualResult.schema().asNullable());
	}

	private void queryOnJwptOuter3(final DatabaseStatistics statistics, final Dataset<Row> fullDataset)  throws Exception {
		final Settings settings = new Settings.Builder("queryTestSingleTriplePattern3_db").usingJWPTOuterNodes().build();
		final ClassLoader classLoader = getClass().getClassLoader();
		final Query query = new Query(classLoader.getResource("queryTestSingleTriple3.q").getPath(), statistics,
			      settings);
				
		//EXPECTED
		final StructType schema = DataTypes.createStructType(new StructField[]{
				DataTypes.createStructField("title", DataTypes.StringType, true),
				});
		final Row row1 = RowFactory.create("Title1");
		final Row row2 = RowFactory.create("Title2");
		final List<Row> rowList = ImmutableList.of(row1, row2);
		final Dataset<Row> expectedResult = spark().createDataFrame(rowList, schema);
		
		//ACTUAL
		final Dataset<Row> actualResult = query.compute(spark().sqlContext()).orderBy("title");
		final Dataset<Row> nullableActualResult = sqlContext().createDataFrame(actualResult.collectAsList(),
				actualResult.schema().asNullable());
	}

	private void queryOnJwptLeftOuter3(final DatabaseStatistics statistics, final Dataset<Row> fullDataset)  throws Exception {
		final Settings settings = new Settings.Builder("queryTestSingleTriplePattern3_db").usingJWPTLeftouterNodes().build();
		final ClassLoader classLoader = getClass().getClassLoader();
		final Query query = new Query(classLoader.getResource("queryTestSingleTriple3.q").getPath(), statistics,
			      settings);
				
		//EXPECTED
		final StructType schema = DataTypes.createStructType(new StructField[]{
				DataTypes.createStructField("title", DataTypes.StringType, true),
				});
		final Row row1 = RowFactory.create("Title1");
		final Row row2 = RowFactory.create("Title2");
		final List<Row> rowList = ImmutableList.of(row1, row2);
		final Dataset<Row> expectedResult = spark().createDataFrame(rowList, schema);
		
		//ACTUAL
		final Dataset<Row> actualResult = query.compute(spark().sqlContext()).orderBy("title");
		final Dataset<Row> nullableActualResult = sqlContext().createDataFrame(actualResult.collectAsList(),
				actualResult.schema().asNullable());
	}

	private Dataset<Row> initializeDb3(final DatabaseStatistics statistics) {
		spark().sql("DROP DATABASE IF EXISTS queryTestSingleTriplePattern3_db CASCADE");
		spark().sql("CREATE DATABASE IF NOT EXISTS  queryTestSingleTriplePattern3_db");
		spark().sql("USE queryTestSingleTriplePattern3_db");

				
		// creates test tt table
		final TripleBean t1 = new TripleBean();
		t1.setS("<http://example.org/book1>");
		t1.setP("<http://example.org/title>");
		t1.setO("Title1");

		final TripleBean t2 = new TripleBean();
		t2.setS("<http://example.org/book2>");
		t2.setP("<http://example.org/title>");
		t2.setO("Title2");

		final ArrayList<TripleBean> triplesList = new ArrayList<>();
		triplesList.add(t1);
		triplesList.add(t2);

		final Dataset<Row> ttDataset = spark().createDataset(triplesList, triplesEncoder).select("s", "p", "o").orderBy(
				"s", "p", "o");
		ttDataset.write().saveAsTable("tripletable");

		final loader.Settings loaderSettings =
				new loader.Settings.Builder("queryTestSingleTriplePattern3_db").withInputPath((System.getProperty(
						"user.dir") + "\\target\\test_output\\SingleTriplePatternTest").replace('\\', '/'))
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
	public void queryTest4() throws Exception {
		final DatabaseStatistics statistics = new DatabaseStatistics("queryTestSingleTriplePattern4_db");
		Dataset<Row> fullDataset = initializeDb4(statistics);
		fullDataset = fullDataset.orderBy("s", "p", "o");
		queryOnTT4(statistics, fullDataset);
		queryOnVp4(statistics, fullDataset);
		queryOnWpt4(statistics, fullDataset);
		queryOnIwpt4(statistics, fullDataset);
		queryOnJwptOuter4(statistics, fullDataset);
		queryOnJwptLeftOuter4(statistics, fullDataset);
	}

	private void queryOnTT4(final DatabaseStatistics statistics, final Dataset<Row> fullDataset)  throws Exception {
		final Settings settings = new Settings.Builder("queryTestSingleTriplePattern4_db").usingTTNodes().build();
		final ClassLoader classLoader = getClass().getClassLoader();
		final Query query = new Query(classLoader.getResource("queryTestSingleTriple4.q").getPath(), statistics,
			      settings);
				
		//EXPECTED
		final StructType schema = DataTypes.createStructType(new StructField[]{
				DataTypes.createStructField("book", DataTypes.StringType, true),
				DataTypes.createStructField("p", DataTypes.StringType, true),
				});
		final Row row1 = RowFactory.create("<http://example.org/book1>", "<http://example.org/title>");
		final Row row2 = RowFactory.create("<http://example.org/book2>", "<http://example.org/title>");
		final List<Row> rowList = ImmutableList.of(row1, row2);
		final Dataset<Row> expectedResult = spark().createDataFrame(rowList, schema);
		
		//ACTUAL
		final Dataset<Row> actualResult = query.compute(spark().sqlContext()).orderBy("book", "p");
		final Dataset<Row> nullableActualResult = sqlContext().createDataFrame(actualResult.collectAsList(),
				actualResult.schema().asNullable());
		
		System.out.print("SingleTriplePatternTest: queryTest4");
		expectedResult.printSchema();
		expectedResult.show();	
		nullableActualResult.printSchema();
		nullableActualResult.show();
		assertDataFrameEquals(expectedResult, nullableActualResult);
	}
	
	private void queryOnVp4(final DatabaseStatistics statistics, final Dataset<Row> fullDataset)  throws Exception {
		final Settings settings = new Settings.Builder("queryTestSingleTriplePattern4_db").usingVPNodes().build();
		final ClassLoader classLoader = getClass().getClassLoader();
		final Query query = new Query(classLoader.getResource("queryTestSingleTriple4.q").getPath(), statistics,
			      settings);
				
		//EXPECTED
		final StructType schema = DataTypes.createStructType(new StructField[]{
				DataTypes.createStructField("book", DataTypes.StringType, true),
				DataTypes.createStructField("p", DataTypes.StringType, true),
				});
		final Row row1 = RowFactory.create("<http://example.org/book1>", "<http://example.org/title>");
		final Row row2 = RowFactory.create("<http://example.org/book2>", "<http://example.org/title>");
		final List<Row> rowList = ImmutableList.of(row1, row2);
		final Dataset<Row> expectedResult = spark().createDataFrame(rowList, schema);
		
		//ACTUAL
		final Dataset<Row> actualResult = query.compute(spark().sqlContext()).orderBy("book", "p");
		final Dataset<Row> nullableActualResult = sqlContext().createDataFrame(actualResult.collectAsList(),
				actualResult.schema().asNullable());
	}

	private void queryOnWpt4(final DatabaseStatistics statistics, final Dataset<Row> fullDataset)  throws Exception {
		final Settings settings = new Settings.Builder("queryTestSingleTriplePattern4_db").usingWPTNodes().build();
		final ClassLoader classLoader = getClass().getClassLoader();
		final Query query = new Query(classLoader.getResource("queryTestSingleTriple4.q").getPath(), statistics,
			      settings);
				
		//EXPECTED
		final StructType schema = DataTypes.createStructType(new StructField[]{
				DataTypes.createStructField("book", DataTypes.StringType, true),
				DataTypes.createStructField("p", DataTypes.StringType, true),
				});
		final Row row1 = RowFactory.create("<http://example.org/book1>", "<http://example.org/title>");
		final Row row2 = RowFactory.create("<http://example.org/book2>", "<http://example.org/title>");
		final List<Row> rowList = ImmutableList.of(row1, row2);
		final Dataset<Row> expectedResult = spark().createDataFrame(rowList, schema);
		
		//ACTUAL
		final Dataset<Row> actualResult = query.compute(spark().sqlContext()).orderBy("book", "p");
		final Dataset<Row> nullableActualResult = sqlContext().createDataFrame(actualResult.collectAsList(),
				actualResult.schema().asNullable());
	}

	private void queryOnIwpt4(final DatabaseStatistics statistics, final Dataset<Row> fullDataset)  throws Exception {
		final Settings settings = new Settings.Builder("queryTestSingleTriplePattern4_db").usingIWPTNodes().build();
		final ClassLoader classLoader = getClass().getClassLoader();
		final Query query = new Query(classLoader.getResource("queryTestSingleTriple4.q").getPath(), statistics,
			      settings);
				
		//EXPECTED
		final StructType schema = DataTypes.createStructType(new StructField[]{
				DataTypes.createStructField("book", DataTypes.StringType, true),
				DataTypes.createStructField("p", DataTypes.StringType, true),
				});
		final Row row1 = RowFactory.create("<http://example.org/book1>", "<http://example.org/title>");
		final Row row2 = RowFactory.create("<http://example.org/book2>", "<http://example.org/title>");
		final List<Row> rowList = ImmutableList.of(row1, row2);
		final Dataset<Row> expectedResult = spark().createDataFrame(rowList, schema);
		
		//ACTUAL
		final Dataset<Row> actualResult = query.compute(spark().sqlContext()).orderBy("book", "p");
		final Dataset<Row> nullableActualResult = sqlContext().createDataFrame(actualResult.collectAsList(),
				actualResult.schema().asNullable());
	}

	private void queryOnJwptOuter4(final DatabaseStatistics statistics, final Dataset<Row> fullDataset)  throws Exception {
		final Settings settings = new Settings.Builder("queryTestSingleTriplePattern4_db").usingJWPTOuterNodes().build();
		final ClassLoader classLoader = getClass().getClassLoader();
		final Query query = new Query(classLoader.getResource("queryTestSingleTriple4.q").getPath(), statistics,
			      settings);
				
		//EXPECTED
		final StructType schema = DataTypes.createStructType(new StructField[]{
				DataTypes.createStructField("book", DataTypes.StringType, true),
				DataTypes.createStructField("p", DataTypes.StringType, true),
				});
		final Row row1 = RowFactory.create("<http://example.org/book1>", "<http://example.org/title>");
		final Row row2 = RowFactory.create("<http://example.org/book2>", "<http://example.org/title>");
		final List<Row> rowList = ImmutableList.of(row1, row2);
		final Dataset<Row> expectedResult = spark().createDataFrame(rowList, schema);
		
		//ACTUAL
		final Dataset<Row> actualResult = query.compute(spark().sqlContext()).orderBy("book", "p");
		final Dataset<Row> nullableActualResult = sqlContext().createDataFrame(actualResult.collectAsList(),
				actualResult.schema().asNullable());
	}

	private void queryOnJwptLeftOuter4(final DatabaseStatistics statistics, final Dataset<Row> fullDataset)  throws Exception {
		final Settings settings = new Settings.Builder("queryTestSingleTriplePattern4_db").usingJWPTLeftouterNodes().build();
		final ClassLoader classLoader = getClass().getClassLoader();
		final Query query = new Query(classLoader.getResource("queryTestSingleTriple4.q").getPath(), statistics,
			      settings);
				
		//EXPECTED
		final StructType schema = DataTypes.createStructType(new StructField[]{
				DataTypes.createStructField("book", DataTypes.StringType, true),
				DataTypes.createStructField("p", DataTypes.StringType, true),
				});
		final Row row1 = RowFactory.create("<http://example.org/book1>", "<http://example.org/title>");
		final Row row2 = RowFactory.create("<http://example.org/book2>", "<http://example.org/title>");
		final List<Row> rowList = ImmutableList.of(row1, row2);
		final Dataset<Row> expectedResult = spark().createDataFrame(rowList, schema);
		
		//ACTUAL
		final Dataset<Row> actualResult = query.compute(spark().sqlContext()).orderBy("book", "p");
		final Dataset<Row> nullableActualResult = sqlContext().createDataFrame(actualResult.collectAsList(),
				actualResult.schema().asNullable());
	}

	private Dataset<Row> initializeDb4(final DatabaseStatistics statistics) {
		spark().sql("DROP DATABASE IF EXISTS queryTestSingleTriplePattern4_db CASCADE");
		spark().sql("CREATE DATABASE IF NOT EXISTS  queryTestSingleTriplePattern4_db");
		spark().sql("USE queryTestSingleTriplePattern4_db");

				
		// creates test tt table
		final TripleBean t1 = new TripleBean();
		t1.setS("<http://example.org/book1>");
		t1.setP("<http://example.org/title>");
		t1.setO("Title1");

		final TripleBean t2 = new TripleBean();
		t2.setS("<http://example.org/book2>");
		t2.setP("<http://example.org/title>");
		t2.setO("Title2");

		final ArrayList<TripleBean> triplesList = new ArrayList<>();
		triplesList.add(t1);
		triplesList.add(t2);

		final Dataset<Row> ttDataset = spark().createDataset(triplesList, triplesEncoder).select("s", "p", "o").orderBy(
				"s", "p", "o");
		ttDataset.write().saveAsTable("tripletable");

		final loader.Settings loaderSettings =
				new loader.Settings.Builder("queryTestSingleTriplePattern4_db").withInputPath((System.getProperty(
						"user.dir") + "\\target\\test_output\\SingleTriplePatternTest").replace('\\', '/'))
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
	public void queryTest5() throws Exception {
		final DatabaseStatistics statistics = new DatabaseStatistics("queryTestSingleTriplePattern5_db");
		Dataset<Row> fullDataset = initializeDb5(statistics);
		fullDataset = fullDataset.orderBy("s", "p", "o");
		queryOnTT5(statistics, fullDataset);
		queryOnVp5(statistics, fullDataset);
		queryOnWpt5(statistics, fullDataset);
		queryOnIwpt5(statistics, fullDataset);
		queryOnJwptOuter5(statistics, fullDataset);
		queryOnJwptLeftOuter5(statistics, fullDataset);
	}

	private void queryOnTT5(final DatabaseStatistics statistics, final Dataset<Row> fullDataset)  throws Exception {
		final Settings settings = new Settings.Builder("queryTestSingleTriplePattern5_db").usingTTNodes().build();
		final ClassLoader classLoader = getClass().getClassLoader();
		final Query query = new Query(classLoader.getResource("queryTestSingleTriple5.q").getPath(), statistics,
			      settings);
				
		//EXPECTED
		final StructType schema = DataTypes.createStructType(new StructField[]{
				DataTypes.createStructField("book", DataTypes.StringType, true),
				DataTypes.createStructField("title", DataTypes.StringType, true),
				});
		final Row row1 = RowFactory.create("<http://example.org/book1>", "Title1");
		final Row row2 = RowFactory.create("<http://example.org/book2>", "Title2");
		final List<Row> rowList = ImmutableList.of(row1, row2);
		final Dataset<Row> expectedResult = spark().createDataFrame(rowList, schema);
		
		//ACTUAL
		final Dataset<Row> actualResult = query.compute(spark().sqlContext()).orderBy("book", "title");
		final Dataset<Row> nullableActualResult = sqlContext().createDataFrame(actualResult.collectAsList(),
				actualResult.schema().asNullable());
		
		System.out.print("SingleTriplePatternTest: queryTest5");
		expectedResult.printSchema();
		expectedResult.show();	
		nullableActualResult.printSchema();
		nullableActualResult.show();
		assertDataFrameEquals(expectedResult, nullableActualResult);
	}
	
	private void queryOnVp5(final DatabaseStatistics statistics, final Dataset<Row> fullDataset)  throws Exception {
		final Settings settings = new Settings.Builder("queryTestSingleTriplePattern5_db").usingVPNodes().build();
		final ClassLoader classLoader = getClass().getClassLoader();
		final Query query = new Query(classLoader.getResource("queryTestSingleTriple5.q").getPath(), statistics,
			      settings);
				
		//EXPECTED
		final StructType schema = DataTypes.createStructType(new StructField[]{
				DataTypes.createStructField("book", DataTypes.StringType, true),
				DataTypes.createStructField("title", DataTypes.StringType, true),
				});
		final Row row1 = RowFactory.create("<http://example.org/book1>", "Title1");
		final Row row2 = RowFactory.create("<http://example.org/book2>", "Title2");
		final List<Row> rowList = ImmutableList.of(row1, row2);
		final Dataset<Row> expectedResult = spark().createDataFrame(rowList, schema);
		
		//ACTUAL
		final Dataset<Row> actualResult = query.compute(spark().sqlContext()).orderBy("book", "title");
		final Dataset<Row> nullableActualResult = sqlContext().createDataFrame(actualResult.collectAsList(),
				actualResult.schema().asNullable());
	}

	private void queryOnWpt5(final DatabaseStatistics statistics, final Dataset<Row> fullDataset)  throws Exception {
		final Settings settings = new Settings.Builder("queryTestSingleTriplePattern5_db").usingWPTNodes().build();
		final ClassLoader classLoader = getClass().getClassLoader();
		final Query query = new Query(classLoader.getResource("queryTestSingleTriple5.q").getPath(), statistics,
			      settings);
				
		//EXPECTED
		final StructType schema = DataTypes.createStructType(new StructField[]{
				DataTypes.createStructField("book", DataTypes.StringType, true),
				DataTypes.createStructField("title", DataTypes.StringType, true),
				});
		final Row row1 = RowFactory.create("<http://example.org/book1>", "Title1");
		final Row row2 = RowFactory.create("<http://example.org/book2>", "Title2");
		final List<Row> rowList = ImmutableList.of(row1, row2);
		final Dataset<Row> expectedResult = spark().createDataFrame(rowList, schema);
		
		//ACTUAL
		final Dataset<Row> actualResult = query.compute(spark().sqlContext()).orderBy("book", "title");
		final Dataset<Row> nullableActualResult = sqlContext().createDataFrame(actualResult.collectAsList(),
				actualResult.schema().asNullable());
	}

	private void queryOnIwpt5(final DatabaseStatistics statistics, final Dataset<Row> fullDataset)  throws Exception {
		final Settings settings = new Settings.Builder("queryTestSingleTriplePattern5_db").usingIWPTNodes().build();
		final ClassLoader classLoader = getClass().getClassLoader();
		final Query query = new Query(classLoader.getResource("queryTestSingleTriple5.q").getPath(), statistics,
			      settings);
				
		//EXPECTED
		final StructType schema = DataTypes.createStructType(new StructField[]{
				DataTypes.createStructField("book", DataTypes.StringType, true),
				DataTypes.createStructField("title", DataTypes.StringType, true),
				});
		final Row row1 = RowFactory.create("<http://example.org/book1>", "Title1");
		final Row row2 = RowFactory.create("<http://example.org/book2>", "Title2");
		final List<Row> rowList = ImmutableList.of(row1, row2);
		final Dataset<Row> expectedResult = spark().createDataFrame(rowList, schema);
		
		//ACTUAL
		final Dataset<Row> actualResult = query.compute(spark().sqlContext()).orderBy("book", "title");
		final Dataset<Row> nullableActualResult = sqlContext().createDataFrame(actualResult.collectAsList(),
				actualResult.schema().asNullable());
	}

	private void queryOnJwptOuter5(final DatabaseStatistics statistics, final Dataset<Row> fullDataset)  throws Exception {
		final Settings settings = new Settings.Builder("queryTestSingleTriplePattern5_db").usingJWPTOuterNodes().build();
		final ClassLoader classLoader = getClass().getClassLoader();
		final Query query = new Query(classLoader.getResource("queryTestSingleTriple5.q").getPath(), statistics,
			      settings);
				
		//EXPECTED
		final StructType schema = DataTypes.createStructType(new StructField[]{
				DataTypes.createStructField("book", DataTypes.StringType, true),
				DataTypes.createStructField("title", DataTypes.StringType, true),
				});
		final Row row1 = RowFactory.create("<http://example.org/book1>", "Title1");
		final Row row2 = RowFactory.create("<http://example.org/book2>", "Title2");
		final List<Row> rowList = ImmutableList.of(row1, row2);
		final Dataset<Row> expectedResult = spark().createDataFrame(rowList, schema);
		
		//ACTUAL
		final Dataset<Row> actualResult = query.compute(spark().sqlContext()).orderBy("book", "title");
		final Dataset<Row> nullableActualResult = sqlContext().createDataFrame(actualResult.collectAsList(),
				actualResult.schema().asNullable());
	}

	private void queryOnJwptLeftOuter5(final DatabaseStatistics statistics, final Dataset<Row> fullDataset)  throws Exception {
		final Settings settings = new Settings.Builder("queryTestSingleTriplePattern5_db").usingJWPTLeftouterNodes().build();
		final ClassLoader classLoader = getClass().getClassLoader();
		final Query query = new Query(classLoader.getResource("queryTestSingleTriple5.q").getPath(), statistics,
			      settings);
				
		//EXPECTED
		final StructType schema = DataTypes.createStructType(new StructField[]{
				DataTypes.createStructField("book", DataTypes.StringType, true),
				DataTypes.createStructField("title", DataTypes.StringType, true),
				});
		final Row row1 = RowFactory.create("<http://example.org/book1>", "Title1");
		final Row row2 = RowFactory.create("<http://example.org/book2>", "Title2");
		final List<Row> rowList = ImmutableList.of(row1, row2);
		final Dataset<Row> expectedResult = spark().createDataFrame(rowList, schema);
		
		//ACTUAL
		final Dataset<Row> actualResult = query.compute(spark().sqlContext()).orderBy("book", "title");
		final Dataset<Row> nullableActualResult = sqlContext().createDataFrame(actualResult.collectAsList(),
				actualResult.schema().asNullable());
	}

	private Dataset<Row> initializeDb5(final DatabaseStatistics statistics) {
		spark().sql("DROP DATABASE IF EXISTS queryTestSingleTriplePattern5_db CASCADE");
		spark().sql("CREATE DATABASE IF NOT EXISTS  queryTestSingleTriplePattern5_db");
		spark().sql("USE queryTestSingleTriplePattern5_db");

				
		// creates test tt table
		final TripleBean t1 = new TripleBean();
		t1.setS("<http://example.org/book1>");
		t1.setP("<http://example.org/title>");
		t1.setO("Title1");

		final TripleBean t2 = new TripleBean();
		t2.setS("<http://example.org/book2>");
		t2.setP("<http://example.org/title>");
		t2.setO("Title2");

		final ArrayList<TripleBean> triplesList = new ArrayList<>();
		triplesList.add(t1);
		triplesList.add(t2);

		final Dataset<Row> ttDataset = spark().createDataset(triplesList, triplesEncoder).select("s", "p", "o").orderBy(
				"s", "p", "o");
		ttDataset.write().saveAsTable("tripletable");

		final loader.Settings loaderSettings =
				new loader.Settings.Builder("queryTestSingleTriplePattern5_db").withInputPath((System.getProperty(
						"user.dir") + "\\target\\test_output\\SingleTriplePatternTest").replace('\\', '/'))
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
	public void queryTest6() throws Exception {
		final DatabaseStatistics statistics = new DatabaseStatistics("queryTestSingleTriplePattern6_db");
		Dataset<Row> fullDataset = initializeDb6(statistics);
		fullDataset = fullDataset.orderBy("s", "p", "o");
		queryOnTT6(statistics, fullDataset);
		queryOnVp6(statistics, fullDataset);
		queryOnWpt6(statistics, fullDataset);
		queryOnIwpt6(statistics, fullDataset);
		queryOnJwptOuter6(statistics, fullDataset);
		queryOnJwptLeftOuter6(statistics, fullDataset);
	}

	private void queryOnTT6(final DatabaseStatistics statistics, final Dataset<Row> fullDataset)  throws Exception {
		final Settings settings = new Settings.Builder("queryTestSingleTriplePattern6_db").usingTTNodes().build();
		final ClassLoader classLoader = getClass().getClassLoader();
		final Query query = new Query(classLoader.getResource("queryTestSingleTriple6.q").getPath(), statistics,
			      settings);
				
		//EXPECTED
		final StructType schema = DataTypes.createStructType(new StructField[]{
				DataTypes.createStructField("p", DataTypes.StringType, true),
				DataTypes.createStructField("title", DataTypes.StringType, true),
				});
		final Row row1 = RowFactory.create("<http://example.org/title>", "Title1");
		final Row row2 = RowFactory.create("<http://example.org/title>", "Title2");
		final List<Row> rowList = ImmutableList.of(row1, row2);
		final Dataset<Row> expectedResult = spark().createDataFrame(rowList, schema);
		
		//ACTUAL
		final Dataset<Row> actualResult = query.compute(spark().sqlContext()).orderBy("p", "title");
		final Dataset<Row> nullableActualResult = sqlContext().createDataFrame(actualResult.collectAsList(),
				actualResult.schema().asNullable());
		
		System.out.print("SingleTriplePatternTest: queryTest6");
		expectedResult.printSchema();
		expectedResult.show();	
		nullableActualResult.printSchema();
		nullableActualResult.show();
		assertDataFrameEquals(expectedResult, nullableActualResult);
	}
	
	private void queryOnVp6(final DatabaseStatistics statistics, final Dataset<Row> fullDataset)  throws Exception {
		final Settings settings = new Settings.Builder("queryTestSingleTriplePattern6_db").usingVPNodes().build();
		final ClassLoader classLoader = getClass().getClassLoader();
		final Query query = new Query(classLoader.getResource("queryTestSingleTriple6.q").getPath(), statistics,
			      settings);
				
		//EXPECTED
		final StructType schema = DataTypes.createStructType(new StructField[]{
				DataTypes.createStructField("p", DataTypes.StringType, true),
				DataTypes.createStructField("title", DataTypes.StringType, true),
				});
		final Row row1 = RowFactory.create("<http://example.org/title>", "Title1");
		final Row row2 = RowFactory.create("<http://example.org/title>", "Title2");
		final List<Row> rowList = ImmutableList.of(row1, row2);
		final Dataset<Row> expectedResult = spark().createDataFrame(rowList, schema);
		
		//ACTUAL
		final Dataset<Row> actualResult = query.compute(spark().sqlContext()).orderBy("p", "title");
		final Dataset<Row> nullableActualResult = sqlContext().createDataFrame(actualResult.collectAsList(),
				actualResult.schema().asNullable());
	}

	private void queryOnWpt6(final DatabaseStatistics statistics, final Dataset<Row> fullDataset)  throws Exception {
		final Settings settings = new Settings.Builder("queryTestSingleTriplePattern6_db").usingWPTNodes().build();
		final ClassLoader classLoader = getClass().getClassLoader();
		final Query query = new Query(classLoader.getResource("queryTestSingleTriple6.q").getPath(), statistics,
			      settings);
				
		//EXPECTED
		final StructType schema = DataTypes.createStructType(new StructField[]{
				DataTypes.createStructField("p", DataTypes.StringType, true),
				DataTypes.createStructField("title", DataTypes.StringType, true),
				});
		final Row row1 = RowFactory.create("<http://example.org/title>", "Title1");
		final Row row2 = RowFactory.create("<http://example.org/title>", "Title2");
		final List<Row> rowList = ImmutableList.of(row1, row2);
		final Dataset<Row> expectedResult = spark().createDataFrame(rowList, schema);
		
		//ACTUAL
		final Dataset<Row> actualResult = query.compute(spark().sqlContext()).orderBy("p", "title");
		final Dataset<Row> nullableActualResult = sqlContext().createDataFrame(actualResult.collectAsList(),
				actualResult.schema().asNullable());
	}

	private void queryOnIwpt6(final DatabaseStatistics statistics, final Dataset<Row> fullDataset)  throws Exception {
		final Settings settings = new Settings.Builder("queryTestSingleTriplePattern6_db").usingIWPTNodes().build();
		final ClassLoader classLoader = getClass().getClassLoader();
		final Query query = new Query(classLoader.getResource("queryTestSingleTriple6.q").getPath(), statistics,
			      settings);
				
		//EXPECTED
		final StructType schema = DataTypes.createStructType(new StructField[]{
				DataTypes.createStructField("p", DataTypes.StringType, true),
				DataTypes.createStructField("title", DataTypes.StringType, true),
				});
		final Row row1 = RowFactory.create("<http://example.org/title>", "Title1");
		final Row row2 = RowFactory.create("<http://example.org/title>", "Title2");
		final List<Row> rowList = ImmutableList.of(row1, row2);
		final Dataset<Row> expectedResult = spark().createDataFrame(rowList, schema);
		
		//ACTUAL
		final Dataset<Row> actualResult = query.compute(spark().sqlContext()).orderBy("p", "title");
		final Dataset<Row> nullableActualResult = sqlContext().createDataFrame(actualResult.collectAsList(),
				actualResult.schema().asNullable());
	}

	private void queryOnJwptOuter6(final DatabaseStatistics statistics, final Dataset<Row> fullDataset)  throws Exception {
		final Settings settings = new Settings.Builder("queryTestSingleTriplePattern6_db").usingJWPTOuterNodes().build();
		final ClassLoader classLoader = getClass().getClassLoader();
		final Query query = new Query(classLoader.getResource("queryTestSingleTriple6.q").getPath(), statistics,
			      settings);
				
		//EXPECTED
		final StructType schema = DataTypes.createStructType(new StructField[]{
				DataTypes.createStructField("p", DataTypes.StringType, true),
				DataTypes.createStructField("title", DataTypes.StringType, true),
				});
		final Row row1 = RowFactory.create("<http://example.org/title>", "Title1");
		final Row row2 = RowFactory.create("<http://example.org/title>", "Title2");
		final List<Row> rowList = ImmutableList.of(row1, row2);
		final Dataset<Row> expectedResult = spark().createDataFrame(rowList, schema);
		
		//ACTUAL
		final Dataset<Row> actualResult = query.compute(spark().sqlContext()).orderBy("p", "title");
		final Dataset<Row> nullableActualResult = sqlContext().createDataFrame(actualResult.collectAsList(),
				actualResult.schema().asNullable());
	}

	private void queryOnJwptLeftOuter6(final DatabaseStatistics statistics, final Dataset<Row> fullDataset)  throws Exception {
		final Settings settings = new Settings.Builder("queryTestSingleTriplePattern6_db").usingJWPTLeftouterNodes().build();
		final ClassLoader classLoader = getClass().getClassLoader();
		final Query query = new Query(classLoader.getResource("queryTestSingleTriple6.q").getPath(), statistics,
			      settings);
				
		//EXPECTED
		final StructType schema = DataTypes.createStructType(new StructField[]{
				DataTypes.createStructField("p", DataTypes.StringType, true),
				DataTypes.createStructField("title", DataTypes.StringType, true),
				});
		final Row row1 = RowFactory.create("<http://example.org/title>", "Title1");
		final Row row2 = RowFactory.create("<http://example.org/title>", "Title2");
		final List<Row> rowList = ImmutableList.of(row1, row2);
		final Dataset<Row> expectedResult = spark().createDataFrame(rowList, schema);
		
		//ACTUAL
		final Dataset<Row> actualResult = query.compute(spark().sqlContext()).orderBy("p", "title");
		final Dataset<Row> nullableActualResult = sqlContext().createDataFrame(actualResult.collectAsList(),
				actualResult.schema().asNullable());
	}

	private Dataset<Row> initializeDb6(final DatabaseStatistics statistics) {
		spark().sql("DROP DATABASE IF EXISTS queryTestSingleTriplePattern6_db CASCADE");
		spark().sql("CREATE DATABASE IF NOT EXISTS  queryTestSingleTriplePattern6_db");
		spark().sql("USE queryTestSingleTriplePattern6_db");

				
		// creates test tt table
		final TripleBean t1 = new TripleBean();
		t1.setS("<http://example.org/book1>");
		t1.setP("<http://example.org/title>");
		t1.setO("Title1");

		final TripleBean t2 = new TripleBean();
		t2.setS("<http://example.org/book2>");
		t2.setP("<http://example.org/title>");
		t2.setO("Title2");

		final ArrayList<TripleBean> triplesList = new ArrayList<>();
		triplesList.add(t1);
		triplesList.add(t2);

		final Dataset<Row> ttDataset = spark().createDataset(triplesList, triplesEncoder).select("s", "p", "o").orderBy(
				"s", "p", "o");
		ttDataset.write().saveAsTable("tripletable");

		final loader.Settings loaderSettings =
				new loader.Settings.Builder("queryTestSingleTriplePattern6_db").withInputPath((System.getProperty(
						"user.dir") + "\\target\\test_output\\SingleTriplePatternTest").replace('\\', '/'))
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
	public void queryTest7() throws Exception {
		final DatabaseStatistics statistics = new DatabaseStatistics("queryTestSingleTriplePattern7_db");
		Dataset<Row> fullDataset = initializeDb7(statistics);
		fullDataset = fullDataset.orderBy("s", "p", "o");
		queryOnTT7(statistics, fullDataset);
		queryOnVp7(statistics, fullDataset);
		queryOnWpt7(statistics, fullDataset);
		queryOnIwpt7(statistics, fullDataset);
		queryOnJwptOuter7(statistics, fullDataset);
		queryOnJwptLeftOuter7(statistics, fullDataset);
	}

	private void queryOnTT7(final DatabaseStatistics statistics, final Dataset<Row> fullDataset)  throws Exception {
		final Settings settings = new Settings.Builder("queryTestSingleTriplePattern7_db").usingTTNodes().build();
		final ClassLoader classLoader = getClass().getClassLoader();
		final Query query = new Query(classLoader.getResource("queryTestSingleTriple7.q").getPath(), statistics,
			      settings);
				
		//EXPECTED
		final StructType schema = DataTypes.createStructType(new StructField[]{
				DataTypes.createStructField("book", DataTypes.StringType, true),
				DataTypes.createStructField("p", DataTypes.StringType, true),
				DataTypes.createStructField("title", DataTypes.StringType, true),
				});
		final Row row1 = RowFactory.create("<http://example.org/book1>", "<http://example.org/title>", "Title1");
		final Row row2 = RowFactory.create("<http://example.org/book2>", "<http://example.org/title>", "Title2");
		final List<Row> rowList = ImmutableList.of(row1, row2);
		final Dataset<Row> expectedResult = spark().createDataFrame(rowList, schema);
		
		//ACTUAL
		final Dataset<Row> actualResult = query.compute(spark().sqlContext()).orderBy("book", "p", "title");
		final Dataset<Row> nullableActualResult = sqlContext().createDataFrame(actualResult.collectAsList(),
				actualResult.schema().asNullable());
		
		System.out.print("SingleTriplePatternTest: queryTest7");
		expectedResult.printSchema();
		expectedResult.show();	
		nullableActualResult.printSchema();
		nullableActualResult.show();
		assertDataFrameEquals(expectedResult, nullableActualResult);
	}
	
	private void queryOnVp7(final DatabaseStatistics statistics, final Dataset<Row> fullDataset)  throws Exception {
		final Settings settings = new Settings.Builder("queryTestSingleTriplePattern7_db").usingVPNodes().build();
		final ClassLoader classLoader = getClass().getClassLoader();
		final Query query = new Query(classLoader.getResource("queryTestSingleTriple7.q").getPath(), statistics,
			      settings);
				
		//EXPECTED
		final StructType schema = DataTypes.createStructType(new StructField[]{
				DataTypes.createStructField("book", DataTypes.StringType, true),
				DataTypes.createStructField("p", DataTypes.StringType, true),
				DataTypes.createStructField("title", DataTypes.StringType, true),
				});
		final Row row1 = RowFactory.create("<http://example.org/book1>", "<http://example.org/title>", "Title1");
		final Row row2 = RowFactory.create("<http://example.org/book2>", "<http://example.org/title>", "Title2");
		final List<Row> rowList = ImmutableList.of(row1, row2);
		final Dataset<Row> expectedResult = spark().createDataFrame(rowList, schema);
		
		//ACTUAL
		final Dataset<Row> actualResult = query.compute(spark().sqlContext()).orderBy("book", "p", "title");
		final Dataset<Row> nullableActualResult = sqlContext().createDataFrame(actualResult.collectAsList(),
				actualResult.schema().asNullable());
	}

	private void queryOnWpt7(final DatabaseStatistics statistics, final Dataset<Row> fullDataset)  throws Exception {
		final Settings settings = new Settings.Builder("queryTestSingleTriplePattern7_db").usingWPTNodes().build();
		final ClassLoader classLoader = getClass().getClassLoader();
		final Query query = new Query(classLoader.getResource("queryTestSingleTriple7.q").getPath(), statistics,
			      settings);
				
		//EXPECTED
		final StructType schema = DataTypes.createStructType(new StructField[]{
				DataTypes.createStructField("book", DataTypes.StringType, true),
				DataTypes.createStructField("p", DataTypes.StringType, true),
				DataTypes.createStructField("title", DataTypes.StringType, true),
				});
		final Row row1 = RowFactory.create("<http://example.org/book1>", "<http://example.org/title>", "Title1");
		final Row row2 = RowFactory.create("<http://example.org/book2>", "<http://example.org/title>", "Title2");
		final List<Row> rowList = ImmutableList.of(row1, row2);
		final Dataset<Row> expectedResult = spark().createDataFrame(rowList, schema);
		
		//ACTUAL
		final Dataset<Row> actualResult = query.compute(spark().sqlContext()).orderBy("book", "p", "title");
		final Dataset<Row> nullableActualResult = sqlContext().createDataFrame(actualResult.collectAsList(),
				actualResult.schema().asNullable());
	}

	private void queryOnIwpt7(final DatabaseStatistics statistics, final Dataset<Row> fullDataset)  throws Exception {
		final Settings settings = new Settings.Builder("queryTestSingleTriplePattern7_db").usingIWPTNodes().build();
		final ClassLoader classLoader = getClass().getClassLoader();
		final Query query = new Query(classLoader.getResource("queryTestSingleTriple7.q").getPath(), statistics,
			      settings);
				
		//EXPECTED
		final StructType schema = DataTypes.createStructType(new StructField[]{
				DataTypes.createStructField("book", DataTypes.StringType, true),
				DataTypes.createStructField("p", DataTypes.StringType, true),
				DataTypes.createStructField("title", DataTypes.StringType, true),
				});
		final Row row1 = RowFactory.create("<http://example.org/book1>", "<http://example.org/title>", "Title1");
		final Row row2 = RowFactory.create("<http://example.org/book2>", "<http://example.org/title>", "Title2");
		final List<Row> rowList = ImmutableList.of(row1, row2);
		final Dataset<Row> expectedResult = spark().createDataFrame(rowList, schema);
		
		//ACTUAL
		final Dataset<Row> actualResult = query.compute(spark().sqlContext()).orderBy("book", "p", "title");
		final Dataset<Row> nullableActualResult = sqlContext().createDataFrame(actualResult.collectAsList(),
				actualResult.schema().asNullable());
	}

	private void queryOnJwptOuter7(final DatabaseStatistics statistics, final Dataset<Row> fullDataset)  throws Exception {
		final Settings settings = new Settings.Builder("queryTestSingleTriplePattern7_db").usingJWPTOuterNodes().build();
		final ClassLoader classLoader = getClass().getClassLoader();
		final Query query = new Query(classLoader.getResource("queryTestSingleTriple7.q").getPath(), statistics,
			      settings);
				
		//EXPECTED
		final StructType schema = DataTypes.createStructType(new StructField[]{
				DataTypes.createStructField("book", DataTypes.StringType, true),
				DataTypes.createStructField("p", DataTypes.StringType, true),
				DataTypes.createStructField("title", DataTypes.StringType, true),
				});
		final Row row1 = RowFactory.create("<http://example.org/book1>", "<http://example.org/title>", "Title1");
		final Row row2 = RowFactory.create("<http://example.org/book2>", "<http://example.org/title>", "Title2");
		final List<Row> rowList = ImmutableList.of(row1, row2);
		final Dataset<Row> expectedResult = spark().createDataFrame(rowList, schema);
		
		//ACTUAL
		final Dataset<Row> actualResult = query.compute(spark().sqlContext()).orderBy("book", "p", "title");
		final Dataset<Row> nullableActualResult = sqlContext().createDataFrame(actualResult.collectAsList(),
				actualResult.schema().asNullable());
	}

	private void queryOnJwptLeftOuter7(final DatabaseStatistics statistics, final Dataset<Row> fullDataset)  throws Exception {
		final Settings settings = new Settings.Builder("queryTestSingleTriplePattern7_db").usingJWPTLeftouterNodes().build();
		final ClassLoader classLoader = getClass().getClassLoader();
		final Query query = new Query(classLoader.getResource("queryTestSingleTriple7.q").getPath(), statistics,
			      settings);
				
		//EXPECTED
		final StructType schema = DataTypes.createStructType(new StructField[]{
				DataTypes.createStructField("book", DataTypes.StringType, true),
				DataTypes.createStructField("p", DataTypes.StringType, true),
				DataTypes.createStructField("title", DataTypes.StringType, true),
				});
		final Row row1 = RowFactory.create("<http://example.org/book1>", "<http://example.org/title>", "Title1");
		final Row row2 = RowFactory.create("<http://example.org/book2>", "<http://example.org/title>", "Title2");
		final List<Row> rowList = ImmutableList.of(row1, row2);
		final Dataset<Row> expectedResult = spark().createDataFrame(rowList, schema);
		
		//ACTUAL
		final Dataset<Row> actualResult = query.compute(spark().sqlContext()).orderBy("book", "p", "title");
		final Dataset<Row> nullableActualResult = sqlContext().createDataFrame(actualResult.collectAsList(),
				actualResult.schema().asNullable());
	}

	private Dataset<Row> initializeDb7(final DatabaseStatistics statistics) {
		spark().sql("DROP DATABASE IF EXISTS queryTestSingleTriplePattern7_db CASCADE");
		spark().sql("CREATE DATABASE IF NOT EXISTS  queryTestSingleTriplePattern7_db");
		spark().sql("USE queryTestSingleTriplePattern7_db");

				
		// creates test tt table
		final TripleBean t1 = new TripleBean();
		t1.setS("<http://example.org/book1>");
		t1.setP("<http://example.org/title>");
		t1.setO("Title1");

		final TripleBean t2 = new TripleBean();
		t2.setS("<http://example.org/book2>");
		t2.setP("<http://example.org/title>");
		t2.setO("Title2");

		final ArrayList<TripleBean> triplesList = new ArrayList<>();
		triplesList.add(t1);
		triplesList.add(t2);

		final Dataset<Row> ttDataset = spark().createDataset(triplesList, triplesEncoder).select("s", "p", "o").orderBy(
				"s", "p", "o");
		ttDataset.write().saveAsTable("tripletable");

		final loader.Settings loaderSettings =
				new loader.Settings.Builder("queryTestSingleTriplePattern7_db").withInputPath((System.getProperty(
						"user.dir") + "\\target\\test_output\\SingleTriplePatternTest").replace('\\', '/'))
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
ex:book2		| ex:title			| "Title2"
================================================================================================================

1. QUERY:
-----------------------------------------------------------------------------------------------------------------
SELECT ?book
WHERE
{?book ?p ?title.}
-----------------------------------------------------------------------------------------------------------------

1. RESULT:
-----------------------------------------------------------------------------------------------------------------
Expected:


Actual:
-----------------------------------------------------------------------------------------------------------------

2. QUERY:
-----------------------------------------------------------------------------------------------------------------
SELECT ?p
WHERE
{?book ?p ?title.}
-----------------------------------------------------------------------------------------------------------------

2. RESULT:
-----------------------------------------------------------------------------------------------------------------
Expected:


Actual:
-----------------------------------------------------------------------------------------------------------------

3. QUERY:
-----------------------------------------------------------------------------------------------------------------
SELECT ?title
WHERE
{?book ?p ?title.}
-----------------------------------------------------------------------------------------------------------------

3. RESULT:
-----------------------------------------------------------------------------------------------------------------
Expected:
+------+
| title|
+------+
|Title1|
|Title2|
+------+

Actual:
-----------------------------------------------------------------------------------------------------------------

4. QUERY:
-----------------------------------------------------------------------------------------------------------------
SELECT ?book ?p
WHERE
{?book ?p ?title.}
-----------------------------------------------------------------------------------------------------------------

4. RESULT:
-----------------------------------------------------------------------------------------------------------------
Expected:


Actual:
-----------------------------------------------------------------------------------------------------------------

5. QUERY:
-----------------------------------------------------------------------------------------------------------------
SELECT ?book ?title
WHERE
{?book ?p ?title.}
-----------------------------------------------------------------------------------------------------------------

5. RESULT:
-----------------------------------------------------------------------------------------------------------------
Expected:
+--------------------+------+
|                book| title|
+--------------------+------+
|<http://example.o...|Title1|
|<http://example.o...|Title2|
+--------------------+------+

Actual:
-----------------------------------------------------------------------------------------------------------------

6. QUERY:
-----------------------------------------------------------------------------------------------------------------
SELECT ?p ?title
WHERE
{?book ?p ?title.}
-----------------------------------------------------------------------------------------------------------------

6. RESULT:
-----------------------------------------------------------------------------------------------------------------
Expected:


Actual:
-----------------------------------------------------------------------------------------------------------------

7. QUERY:
-----------------------------------------------------------------------------------------------------------------
SELECT *
WHERE
{?book ?p ?title.}
-----------------------------------------------------------------------------------------------------------------

7. RESULT:
-----------------------------------------------------------------------------------------------------------------
Expected:


Actual:
-----------------------------------------------------------------------------------------------------------------

*/