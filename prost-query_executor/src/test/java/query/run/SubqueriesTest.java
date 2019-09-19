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
 * This class tests represents the highest level of testing, i.e. given a query
 * it checks that results are correctly and consistently returned according to
 * ALL supported logical partitioning strategies (at the moment WPT, IWPT, JWPT,
 * and VP?), i.e. these tests verify are about SPARQL semantics.
 *
 * @author Kristin Plettau
 */
public class SubqueriesTest extends JavaDataFrameSuiteBase implements Serializable {
	private static final long serialVersionUID = 1329L;
	private static final Encoder<TripleBean> triplesEncoder = Encoders.bean(TripleBean.class);
	
	@Test
	public void queryTest1() throws Exception {
		final DatabaseStatistics statistics = new DatabaseStatistics("queryTestSubquery1_db");
		Dataset<Row> fullDataset = initializeDb1(statistics);
		fullDataset = fullDataset.orderBy("s", "p", "o");
		queryOnTT1(statistics, fullDataset);
		queryOnVp1(statistics, fullDataset);
		queryOnWpt1(statistics, fullDataset);
		queryOnIwpt1(statistics, fullDataset);
		queryOnJwptOuter1(statistics, fullDataset);
		queryOnJwptLeftOuter1(statistics, fullDataset);
	}	
	private void queryOnTT1(final DatabaseStatistics statistics, final Dataset<Row> fullDataset)  throws Exception {
		final Settings settings = new Settings.Builder("queryTestSubquery1_db").usingTTNodes().usingCharacteristicSets().build();
		final ClassLoader classLoader = getClass().getClassLoader();		
		final Query query = new Query(classLoader.getResource("queryTestSubquery1.q").getPath(), statistics, settings);
		
		//EXPECTED
		StructType schema = DataTypes.createStructType(new StructField[]{
				DataTypes.createStructField("person", DataTypes.StringType, true),
				DataTypes.createStructField("shortName", DataTypes.StringType, true),
				});
		Row row1 = RowFactory.create("<http://example.org/bob>", "B. Abc");
		Row row2 = RowFactory.create("<http://example.org/carol>", "C. Abc");
		List<Row> rowList = ImmutableList.of(row1, row2);
		Dataset<Row> expectedResult = spark().createDataFrame(rowList, schema);
		
		//ACTUAL
		final Dataset<Row> actualResult = query.compute(spark().sqlContext()).orderBy("person", "shortName");
		final Dataset<Row> nullableActualResult = sqlContext().createDataFrame(actualResult.collectAsList(),
				actualResult.schema().asNullable());
		System.out.print("SubqueriesTest: queryTest1");
		expectedResult.printSchema();
		expectedResult.show();

		nullableActualResult.printSchema();
		nullableActualResult.show();
		assertDataFrameEquals(expectedResult, nullableActualResult);
	}
	
	private void queryOnVp1(final DatabaseStatistics statistics, final Dataset<Row>  fullDataset) throws Exception {
		final Settings settings = new Settings.Builder("queryTestSubquery1_db").usingVPNodes().build();
		final ClassLoader classLoader = getClass().getClassLoader();	
		final Query query = new Query(classLoader.getResource("queryTestSubquery1.q").getPath(), statistics, settings);
		
		//EXPECTED
		StructType schema = DataTypes.createStructType(new StructField[]{
				DataTypes.createStructField("person", DataTypes.StringType, true),
				DataTypes.createStructField("shortName", DataTypes.StringType, true),
				});
		Row row1 = RowFactory.create("<http://example.org/bob>", "B. Abc");
		Row row2 = RowFactory.create("<http://example.org/carol>", "C. Abc");
		List<Row> rowList = ImmutableList.of(row1, row2);
		Dataset<Row> expectedResult = spark().createDataFrame(rowList, schema);
		
		//ACTUAL
		final Dataset<Row> actualResult = query.compute(spark().sqlContext()).orderBy("person", "shortName");
		final Dataset<Row> nullableActualResult = sqlContext().createDataFrame(actualResult.collectAsList(),
				actualResult.schema().asNullable());
		
		assertDataFrameEquals(expectedResult, nullableActualResult);
	}

	private void queryOnWpt1(final DatabaseStatistics statistics, final Dataset<Row>  fullDataset) throws Exception {
		final Settings settings = new Settings.Builder("queryTestSubquery1_db").usingWPTNodes().build();
		final ClassLoader classLoader = getClass().getClassLoader();		
		final Query query = new Query(classLoader.getResource("queryTestSubquery1.q").getPath(), statistics, settings);
		
		//EXPECTED
		StructType schema = DataTypes.createStructType(new StructField[]{
				DataTypes.createStructField("person", DataTypes.StringType, true),
				DataTypes.createStructField("shortName", DataTypes.StringType, true),
				});
		Row row1 = RowFactory.create("<http://example.org/bob>", "B. Abc");
		Row row2 = RowFactory.create("<http://example.org/carol>", "C. Abc");
		List<Row> rowList = ImmutableList.of(row1, row2);
		Dataset<Row> expectedResult = spark().createDataFrame(rowList, schema);
		
		//ACTUAL
		final Dataset<Row> actualResult = query.compute(spark().sqlContext()).orderBy("person", "shortName");
		final Dataset<Row> nullableActualResult = sqlContext().createDataFrame(actualResult.collectAsList(),
				actualResult.schema().asNullable());				
		
		assertDataFrameEquals(expectedResult, nullableActualResult);
	}

	private void queryOnIwpt1(final DatabaseStatistics statistics, final Dataset<Row>  fullDataset) throws Exception {
		final Settings settings = new Settings.Builder("queryTestSubquery1_db").usingIWPTNodes().build();
		final ClassLoader classLoader = getClass().getClassLoader();		
		final Query query = new Query(classLoader.getResource("queryTestSubquery1.q").getPath(), statistics, settings);

		//EXPECTED
		StructType schema = DataTypes.createStructType(new StructField[]{
				DataTypes.createStructField("person", DataTypes.StringType, true),
				DataTypes.createStructField("shortName", DataTypes.StringType, true),
				});
		Row row1 = RowFactory.create("<http://example.org/bob>", "B. Abc");
		Row row2 = RowFactory.create("<http://example.org/carol>", "C. Abc");
		List<Row> rowList = ImmutableList.of(row1, row2);
		Dataset<Row> expectedResult = spark().createDataFrame(rowList, schema);
		
		//ACTUAL
		final Dataset<Row> actualResult = query.compute(spark().sqlContext()).orderBy("person", "shortName");
		final Dataset<Row> nullableActualResult = sqlContext().createDataFrame(actualResult.collectAsList(),
				actualResult.schema().asNullable());
		
		assertDataFrameEquals(expectedResult, nullableActualResult);
	}

	private void queryOnJwptOuter1(final DatabaseStatistics statistics, final Dataset<Row>  fullDataset) throws Exception {
		final Settings settings = new Settings.Builder("queryTestSubquery1_db").usingJWPTOuterNodes().build();
		final ClassLoader classLoader = getClass().getClassLoader();		
		final Query query = new Query(classLoader.getResource("queryTestSubquery1.q").getPath(), statistics, settings);	
		
		//EXPECTED
		StructType schema = DataTypes.createStructType(new StructField[]{
				DataTypes.createStructField("person", DataTypes.StringType, true),
				DataTypes.createStructField("shortName", DataTypes.StringType, true),
				});
		Row row1 = RowFactory.create("<http://example.org/bob>", "B. Abc");
		Row row2 = RowFactory.create("<http://example.org/carol>", "C. Abc");
		List<Row> rowList = ImmutableList.of(row1, row2);
		Dataset<Row> expectedResult = spark().createDataFrame(rowList, schema);
		
		//ACTUAL
		final Dataset<Row> actualResult = query.compute(spark().sqlContext()).orderBy("person", "shortName");
		final Dataset<Row> nullableActualResult = sqlContext().createDataFrame(actualResult.collectAsList(),
				actualResult.schema().asNullable());
		
		assertDataFrameEquals(expectedResult, nullableActualResult);
	}

	private void queryOnJwptLeftOuter1(final DatabaseStatistics statistics, final Dataset<Row>  fullDataset) throws Exception {
		final Settings settings = new Settings.Builder("queryTestSubquery1_db").usingJWPTLeftouterNodes().build();
		final ClassLoader classLoader = getClass().getClassLoader();		
		final Query query = new Query(classLoader.getResource("queryTestSubquery1.q").getPath(), statistics, settings);
			
		//EXPECTED
		StructType schema = DataTypes.createStructType(new StructField[]{
				DataTypes.createStructField("person", DataTypes.StringType, true),
				DataTypes.createStructField("shortName", DataTypes.StringType, true),
				});
		Row row1 = RowFactory.create("<http://example.org/bob>", "B. Abc");
		Row row2 = RowFactory.create("<http://example.org/carol>", "C. Abc");
		List<Row> rowList = ImmutableList.of(row1, row2);
		Dataset<Row> expectedResult = spark().createDataFrame(rowList, schema);
		
		//ACTUAL
		final Dataset<Row> actualResult = query.compute(spark().sqlContext()).orderBy("person", "shortName");
		final Dataset<Row> nullableActualResult = sqlContext().createDataFrame(actualResult.collectAsList(),
				actualResult.schema().asNullable());
		
		assertDataFrameEquals(expectedResult, nullableActualResult);
	}

	private Dataset<Row> initializeDb1(final DatabaseStatistics statistics) {
		spark().sql("DROP DATABASE IF EXISTS queryTestSubquery1_db CASCADE");
		spark().sql("CREATE DATABASE IF NOT EXISTS  queryTestSubquery1_db");
		spark().sql("USE queryTestSubquery1_db");

				
		// creates test tt table
		final TripleBean t1 = new TripleBean();
		t1.setS("<http://example.org/alice>");
		t1.setP("<http://example.org/name>");
		t1.setO("Alice");
		
		final TripleBean t2 = new TripleBean();
		t2.setS("<http://example.org/alice>");
		t2.setP("<http://example.org/name>");
		t2.setO("Alice Abc");
		
		final TripleBean t3 = new TripleBean();
		t3.setS("<http://example.org/alice>");
		t3.setP("<http://example.org/name>");
		t3.setO("A. Abc");
		
		final TripleBean t4 = new TripleBean();
		t4.setS("<http://example.org/bob>");
		t4.setP("<http://example.org/name>");
		t4.setO("Bob");
		
		final TripleBean t5 = new TripleBean();
		t5.setS("<http://example.org/bob>");
		t5.setP("<http://example.org/name>");
		t5.setO("Bob Abc");
		
		final TripleBean t6 = new TripleBean();
		t6.setS("<http://example.org/bob>");
		t6.setP("<http://example.org/name>");
		t6.setO("B. Abc");
		
		final TripleBean t7 = new TripleBean();
		t7.setS("<http://example.org/carol>");
		t7.setP("<http://example.org/name>");
		t7.setO("Carol");
		
		final TripleBean t8 = new TripleBean();
		t8.setS("<http://example.org/carol>");
		t8.setP("<http://example.org/name>");
		t8.setO("Carol Abc");
		
		final TripleBean t9 = new TripleBean();
		t9.setS("<http://example.org/carol>");
		t9.setP("<http://example.org/name>");
		t9.setO("C. Abc");
		
		final TripleBean t10 = new TripleBean();
		t10.setS("<http://example.org/alice>");
		t10.setP("<http://example.org/knows>");
		t10.setO("<http://example.org/bob>");
		
		final TripleBean t11 = new TripleBean();
		t11.setS("<http://example.org/alice>");
		t11.setP("<http://example.org/knows>");
		t11.setO("<http://example.org/carol>");

		
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
		triplesList.add(t10);
		triplesList.add(t11);


		final Dataset<Row> ttDataset = spark().createDataset(triplesList, triplesEncoder).select("s", "p", "o").orderBy(
				"s", "p", "o");
		ttDataset.write().saveAsTable("tripletable");
		
		final loader.Settings loaderSettings =
				new loader.Settings.Builder("queryTestSubquery1_db").withInputPath((System.getProperty(
						"user.dir") + "\\target\\test_output\\SubqueriesTest").replace('\\', '/'))
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
		final DatabaseStatistics statistics = new DatabaseStatistics("queryTestSubquery2_db");
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
		final Settings settings = new Settings.Builder("queryTestSubquery2_db").usingTTNodes().usingCharacteristicSets().build();
		final ClassLoader classLoader = getClass().getClassLoader();		
		final Query query = new Query(classLoader.getResource("queryTestSubquery2.q").getPath(), statistics, settings);
		
		//EXPECTED
		StructType schema = DataTypes.createStructType(new StructField[]{
				DataTypes.createStructField("person", DataTypes.StringType, true),
				DataTypes.createStructField("age", DataTypes.StringType, true),
				});
		Row row1 = RowFactory.create("<http://example.org/carol>", "35");
		List<Row> rowList = ImmutableList.of(row1);
		Dataset<Row> expectedResult = spark().createDataFrame(rowList, schema);
		
		//ACTUAL
		final Dataset<Row> actualResult = query.compute(spark().sqlContext()).orderBy("person", "age");
		final Dataset<Row> nullableActualResult = sqlContext().createDataFrame(actualResult.collectAsList(),
				actualResult.schema().asNullable());
		System.out.print("SubqueriesTest: queryTest2");
		expectedResult.printSchema();
		expectedResult.show();

		nullableActualResult.printSchema();
		nullableActualResult.show();
		assertDataFrameEquals(expectedResult, nullableActualResult);
	}
	
	private void queryOnVp2(final DatabaseStatistics statistics, final Dataset<Row>  fullDataset) throws Exception {
		final Settings settings = new Settings.Builder("queryTestSubquery2_db").usingVPNodes().build();
		final ClassLoader classLoader = getClass().getClassLoader();	
		final Query query = new Query(classLoader.getResource("queryTestSubquery2.q").getPath(), statistics, settings);
		
		//EXPECTED
		StructType schema = DataTypes.createStructType(new StructField[]{
				DataTypes.createStructField("person", DataTypes.StringType, true),
				DataTypes.createStructField("age", DataTypes.StringType, true),
				});
		Row row1 = RowFactory.create("<http://example.org/carol>", "35");
		List<Row> rowList = ImmutableList.of(row1);
		Dataset<Row> expectedResult = spark().createDataFrame(rowList, schema);
		
		//ACTUAL
		final Dataset<Row> actualResult = query.compute(spark().sqlContext()).orderBy("person", "age");
		final Dataset<Row> nullableActualResult = sqlContext().createDataFrame(actualResult.collectAsList(),
				actualResult.schema().asNullable());
		
		assertDataFrameEquals(expectedResult, nullableActualResult);
	}

	private void queryOnWpt2(final DatabaseStatistics statistics, final Dataset<Row>  fullDataset) throws Exception {
		final Settings settings = new Settings.Builder("queryTestSubquery2_db").usingWPTNodes().build();
		final ClassLoader classLoader = getClass().getClassLoader();		
		final Query query = new Query(classLoader.getResource("queryTestSubquery2.q").getPath(), statistics, settings);
		
		//EXPECTED
		StructType schema = DataTypes.createStructType(new StructField[]{
				DataTypes.createStructField("person", DataTypes.StringType, true),
				DataTypes.createStructField("age", DataTypes.StringType, true),
				});
		Row row1 = RowFactory.create("<http://example.org/carol>", "35");
		List<Row> rowList = ImmutableList.of(row1);
		Dataset<Row> expectedResult = spark().createDataFrame(rowList, schema);
		
		//ACTUAL
		final Dataset<Row> actualResult = query.compute(spark().sqlContext()).orderBy("person", "age");
		final Dataset<Row> nullableActualResult = sqlContext().createDataFrame(actualResult.collectAsList(),
				actualResult.schema().asNullable());				
		
		assertDataFrameEquals(expectedResult, nullableActualResult);
	}

	private void queryOnIwpt2(final DatabaseStatistics statistics, final Dataset<Row>  fullDataset) throws Exception {
		final Settings settings = new Settings.Builder("queryTestSubquery2_db").usingIWPTNodes().build();
		final ClassLoader classLoader = getClass().getClassLoader();		
		final Query query = new Query(classLoader.getResource("queryTestSubquery2.q").getPath(), statistics, settings);

		//EXPECTED
		StructType schema = DataTypes.createStructType(new StructField[]{
				DataTypes.createStructField("person", DataTypes.StringType, true),
				DataTypes.createStructField("age", DataTypes.StringType, true),
				});
		Row row1 = RowFactory.create("<http://example.org/carol>", "35");
		List<Row> rowList = ImmutableList.of(row1);
		Dataset<Row> expectedResult = spark().createDataFrame(rowList, schema);
		
		//ACTUAL
		final Dataset<Row> actualResult = query.compute(spark().sqlContext()).orderBy("person", "age");
		final Dataset<Row> nullableActualResult = sqlContext().createDataFrame(actualResult.collectAsList(),
				actualResult.schema().asNullable());
		
		assertDataFrameEquals(expectedResult, nullableActualResult);
	}

	private void queryOnJwptOuter2(final DatabaseStatistics statistics, final Dataset<Row>  fullDataset) throws Exception {
		final Settings settings = new Settings.Builder("queryTestSubquery2_db").usingJWPTOuterNodes().build();
		final ClassLoader classLoader = getClass().getClassLoader();		
		final Query query = new Query(classLoader.getResource("queryTestSubquery2.q").getPath(), statistics, settings);	
		
		//EXPECTED
		StructType schema = DataTypes.createStructType(new StructField[]{
				DataTypes.createStructField("person", DataTypes.StringType, true),
				DataTypes.createStructField("age", DataTypes.StringType, true),
				});
		Row row1 = RowFactory.create("<http://example.org/carol>", "35");
		List<Row> rowList = ImmutableList.of(row1);
		Dataset<Row> expectedResult = spark().createDataFrame(rowList, schema);
		
		//ACTUAL
		final Dataset<Row> actualResult = query.compute(spark().sqlContext()).orderBy("person", "age");
		final Dataset<Row> nullableActualResult = sqlContext().createDataFrame(actualResult.collectAsList(),
				actualResult.schema().asNullable());
		
		assertDataFrameEquals(expectedResult, nullableActualResult);
	}

	private void queryOnJwptLeftOuter2(final DatabaseStatistics statistics, final Dataset<Row>  fullDataset) throws Exception {
		final Settings settings = new Settings.Builder("queryTestSubquery2_db").usingJWPTLeftouterNodes().build();
		final ClassLoader classLoader = getClass().getClassLoader();		
		final Query query = new Query(classLoader.getResource("queryTestSubquery2.q").getPath(), statistics, settings);
			
		//EXPECTED
		StructType schema = DataTypes.createStructType(new StructField[]{
				DataTypes.createStructField("person", DataTypes.StringType, true),
				DataTypes.createStructField("age", DataTypes.StringType, true),
				});
		Row row1 = RowFactory.create("<http://example.org/carol>", "35");
		List<Row> rowList = ImmutableList.of(row1);
		Dataset<Row> expectedResult = spark().createDataFrame(rowList, schema);
		
		//ACTUAL
		final Dataset<Row> actualResult = query.compute(spark().sqlContext()).orderBy("person", "age");
		final Dataset<Row> nullableActualResult = sqlContext().createDataFrame(actualResult.collectAsList(),
				actualResult.schema().asNullable());
		
		assertDataFrameEquals(expectedResult, nullableActualResult);
	}

	private Dataset<Row> initializeDb2(final DatabaseStatistics statistics) {
		spark().sql("DROP DATABASE IF EXISTS queryTestSubquery2_db CASCADE");
		spark().sql("CREATE DATABASE IF NOT EXISTS  queryTestSubquery2_db");
		spark().sql("USE queryTestSubquery2_db");

				
		// creates test tt table
		final TripleBean t1 = new TripleBean();
		t1.setS("<http://example.org/alice>");
		t1.setP("<http://example.org/age>");
		t1.setO("25");
		
		final TripleBean t2 = new TripleBean();
		t2.setS("<http://example.org/bob>");
		t2.setP("<http://example.org/age>");
		t2.setO("22");
		
		final TripleBean t3 = new TripleBean();
		t3.setS("<http://example.org/carol>");
		t3.setP("<http://example.org/age>");
		t3.setO("35");
		
		final TripleBean t4 = new TripleBean();
		t4.setS("<http://example.org/alice>");
		t4.setP("<http://example.org/knows>");
		t4.setO("<http://example.org/bob>");
		
		final TripleBean t5 = new TripleBean();
		t5.setS("<http://example.org/alice>");
		t5.setP("<http://example.org/knows>");
		t5.setO("<http://example.org/carol>");

		
		final ArrayList<TripleBean> triplesList = new ArrayList<>();
		triplesList.add(t1);
		triplesList.add(t2);
		triplesList.add(t3);
		triplesList.add(t4);
		triplesList.add(t5);


		final Dataset<Row> ttDataset = spark().createDataset(triplesList, triplesEncoder).select("s", "p", "o").orderBy(
				"s", "p", "o");
		ttDataset.write().saveAsTable("tripletable");
		
		final loader.Settings loaderSettings =
				new loader.Settings.Builder("queryTestSubquery2_db").withInputPath((System.getProperty(
						"user.dir") + "\\target\\test_output\\SubqueriesTest").replace('\\', '/'))
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
		final DatabaseStatistics statistics = new DatabaseStatistics("queryTestSubquery3_db");
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
		final Settings settings = new Settings.Builder("queryTestSubquery3_db").usingTTNodes().usingCharacteristicSets().build();
		final ClassLoader classLoader = getClass().getClassLoader();		
		final Query query = new Query(classLoader.getResource("queryTestSubquery3.q").getPath(), statistics, settings);
		
		//EXPECTED
		StructType schema = DataTypes.createStructType(new StructField[]{
				DataTypes.createStructField("person", DataTypes.StringType, true),
				DataTypes.createStructField("result", DataTypes.StringType, true),
				});
		Row row1 = RowFactory.create("<http://example.org/bob>", "25");
		Row row2 = RowFactory.create("<http://example.org/carol>", "30");
		List<Row> rowList = ImmutableList.of(row1, row2);
		Dataset<Row> expectedResult = spark().createDataFrame(rowList, schema);
		
		//ACTUAL
		final Dataset<Row> actualResult = query.compute(spark().sqlContext()).orderBy("person", "result");
		final Dataset<Row> nullableActualResult = sqlContext().createDataFrame(actualResult.collectAsList(),
				actualResult.schema().asNullable());
		System.out.print("SubqueriesTest: queryTest3");
		expectedResult.printSchema();
		expectedResult.show();

		nullableActualResult.printSchema();
		nullableActualResult.show();
		assertDataFrameEquals(expectedResult, nullableActualResult);
	}
	
	private void queryOnVp3(final DatabaseStatistics statistics, final Dataset<Row>  fullDataset) throws Exception {
		final Settings settings = new Settings.Builder("queryTestSubquery3_db").usingVPNodes().build();
		final ClassLoader classLoader = getClass().getClassLoader();	
		final Query query = new Query(classLoader.getResource("queryTestSubquery3.q").getPath(), statistics, settings);
		
		//EXPECTED
		StructType schema = DataTypes.createStructType(new StructField[]{
				DataTypes.createStructField("person", DataTypes.StringType, true),
				DataTypes.createStructField("result", DataTypes.StringType, true),
				});
		Row row1 = RowFactory.create("<http://example.org/bob>", "25");
		Row row2 = RowFactory.create("<http://example.org/carol>", "30");
		List<Row> rowList = ImmutableList.of(row1, row2);
		Dataset<Row> expectedResult = spark().createDataFrame(rowList, schema);
		
		//ACTUAL
		final Dataset<Row> actualResult = query.compute(spark().sqlContext()).orderBy("person", "result");
		final Dataset<Row> nullableActualResult = sqlContext().createDataFrame(actualResult.collectAsList(),
				actualResult.schema().asNullable());
		
		assertDataFrameEquals(expectedResult, nullableActualResult);
	}

	private void queryOnWpt3(final DatabaseStatistics statistics, final Dataset<Row>  fullDataset) throws Exception {
		final Settings settings = new Settings.Builder("queryTestSubquery3_db").usingWPTNodes().build();
		final ClassLoader classLoader = getClass().getClassLoader();		
		final Query query = new Query(classLoader.getResource("queryTestSubquery3.q").getPath(), statistics, settings);
		
		//EXPECTED
		StructType schema = DataTypes.createStructType(new StructField[]{
				DataTypes.createStructField("person", DataTypes.StringType, true),
				DataTypes.createStructField("result", DataTypes.StringType, true),
				});
		Row row1 = RowFactory.create("<http://example.org/bob>", "25");
		Row row2 = RowFactory.create("<http://example.org/carol>", "30");
		List<Row> rowList = ImmutableList.of(row1, row2);
		Dataset<Row> expectedResult = spark().createDataFrame(rowList, schema);
		
		//ACTUAL
		final Dataset<Row> actualResult = query.compute(spark().sqlContext()).orderBy("person", "result");
		final Dataset<Row> nullableActualResult = sqlContext().createDataFrame(actualResult.collectAsList(),
				actualResult.schema().asNullable());				
		
		assertDataFrameEquals(expectedResult, nullableActualResult);
	}

	private void queryOnIwpt3(final DatabaseStatistics statistics, final Dataset<Row>  fullDataset) throws Exception {
		final Settings settings = new Settings.Builder("queryTestSubquery3_db").usingIWPTNodes().build();
		final ClassLoader classLoader = getClass().getClassLoader();		
		final Query query = new Query(classLoader.getResource("queryTestSubquery3.q").getPath(), statistics, settings);

		//EXPECTED
		StructType schema = DataTypes.createStructType(new StructField[]{
				DataTypes.createStructField("person", DataTypes.StringType, true),
				DataTypes.createStructField("result", DataTypes.StringType, true),
				});
		Row row1 = RowFactory.create("<http://example.org/bob>", "25");
		Row row2 = RowFactory.create("<http://example.org/carol>", "30");
		List<Row> rowList = ImmutableList.of(row1, row2);
		Dataset<Row> expectedResult = spark().createDataFrame(rowList, schema);
		
		//ACTUAL
		final Dataset<Row> actualResult = query.compute(spark().sqlContext()).orderBy("person", "result");
		final Dataset<Row> nullableActualResult = sqlContext().createDataFrame(actualResult.collectAsList(),
				actualResult.schema().asNullable());
		
		assertDataFrameEquals(expectedResult, nullableActualResult);
	}

	private void queryOnJwptOuter3(final DatabaseStatistics statistics, final Dataset<Row>  fullDataset) throws Exception {
		final Settings settings = new Settings.Builder("queryTestSubquery3_db").usingJWPTOuterNodes().build();
		final ClassLoader classLoader = getClass().getClassLoader();		
		final Query query = new Query(classLoader.getResource("queryTestSubquery3.q").getPath(), statistics, settings);	
		
		//EXPECTED
		StructType schema = DataTypes.createStructType(new StructField[]{
				DataTypes.createStructField("person", DataTypes.StringType, true),
				DataTypes.createStructField("result", DataTypes.StringType, true),
				});
		Row row1 = RowFactory.create("<http://example.org/bob>", "25");
		Row row2 = RowFactory.create("<http://example.org/carol>", "30");
		List<Row> rowList = ImmutableList.of(row1, row2);
		Dataset<Row> expectedResult = spark().createDataFrame(rowList, schema);
		
		//ACTUAL
		final Dataset<Row> actualResult = query.compute(spark().sqlContext()).orderBy("person", "result");
		final Dataset<Row> nullableActualResult = sqlContext().createDataFrame(actualResult.collectAsList(),
				actualResult.schema().asNullable());
		
		assertDataFrameEquals(expectedResult, nullableActualResult);
	}

	private void queryOnJwptLeftOuter3(final DatabaseStatistics statistics, final Dataset<Row>  fullDataset) throws Exception {
		final Settings settings = new Settings.Builder("queryTestSubquery3_db").usingJWPTLeftouterNodes().build();
		final ClassLoader classLoader = getClass().getClassLoader();		
		final Query query = new Query(classLoader.getResource("queryTestSubquery3.q").getPath(), statistics, settings);
			
		//EXPECTED
		StructType schema = DataTypes.createStructType(new StructField[]{
				DataTypes.createStructField("person", DataTypes.StringType, true),
				DataTypes.createStructField("result", DataTypes.StringType, true),
				});
		Row row1 = RowFactory.create("<http://example.org/bob>", "25");
		Row row2 = RowFactory.create("<http://example.org/carol>", "30");
		List<Row> rowList = ImmutableList.of(row1, row2);
		Dataset<Row> expectedResult = spark().createDataFrame(rowList, schema);
		
		//ACTUAL
		final Dataset<Row> actualResult = query.compute(spark().sqlContext()).orderBy("person", "result");
		final Dataset<Row> nullableActualResult = sqlContext().createDataFrame(actualResult.collectAsList(),
				actualResult.schema().asNullable());
		
		assertDataFrameEquals(expectedResult, nullableActualResult);
	}

	private Dataset<Row> initializeDb3(final DatabaseStatistics statistics) {
		spark().sql("DROP DATABASE IF EXISTS queryTestSubquery3_db CASCADE");
		spark().sql("CREATE DATABASE IF NOT EXISTS  queryTestSubquery3_db");
		spark().sql("USE queryTestSubquery3_db");

				
		// creates test tt table
		final TripleBean t1 = new TripleBean();
		t1.setS("<http://example.org/alice>");
		t1.setP("<http://example.org/firstValue>");
		t1.setO("10");
		
		final TripleBean t2 = new TripleBean();
		t2.setS("<http://example.org/alice>");
		t2.setP("<http://example.org/secondValue>");
		t2.setO("10");
		
		final TripleBean t3 = new TripleBean();
		t3.setS("<http://example.org/bob>");
		t3.setP("<http://example.org/firstValue>");
		t3.setO("10");
		
		final TripleBean t4 = new TripleBean();
		t4.setS("<http://example.org/bob>");
		t4.setP("<http://example.org/secondValue>");
		t4.setO("15");
		
		final TripleBean t5 = new TripleBean();
		t5.setS("<http://example.org/carol>");
		t5.setP("<http://example.org/firstValue>");
		t5.setO("10");
		
		final TripleBean t6 = new TripleBean();
		t6.setS("<http://example.org/carol>");
		t6.setP("<http://example.org/secondValue>");
		t6.setO("20");
		
		final TripleBean t7 = new TripleBean();
		t7.setS("<http://example.org/alice>");
		t7.setP("<http://example.org/knows>");
		t7.setO("<http://example.org/bob>");
		
		final TripleBean t8 = new TripleBean();
		t8.setS("<http://example.org/alice>");
		t8.setP("<http://example.org/knows>");
		t8.setO("<http://example.org/carol>");

		
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
				new loader.Settings.Builder("queryTestSubquery3_db").withInputPath((System.getProperty(
						"user.dir") + "\\target\\test_output\\SubqueriesTest").replace('\\', '/'))
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
ex:alice		| ex:name			| "Alice"
ex:alice		| ex:name			| "Alice Abc"
ex:alice		| ex:name			| "A. Abc"

ex:bob			| ex:name			| "Bob"
ex:bob			| ex:name			| "Bob Abc"
ex:bob			| ex:name			| "B. Abc"

ex:carol		| ex:name			| "Carol"
ex:carol		| ex:name			| "Carol Abc"
ex:carol		| ex:name			| "C. Abc"

ex:alice		| ex:knows			| ex:bob
ex:alice		| ex:knows			| ex:carol
================================================================================================================

-----------------------------------------------------------------------------------------------------------------
SELECT ?person ?shortName
WHERE 
{
	<http://example.org/alice> <http://example.org/knows> ?person .
	{
		SELECT ?person (MIN(?name) AS ?shortName)
		WHERE 
		{ ?person <http://example.org/name> ?name .} 
		GROUP BY ?person
	}
}
-----------------------------------------------------------------------------------------------------------------
RESULT:
-----------------------------------------------------------------------------------------------------------------
Outer query:
+--------------------------+
|                    person|
+--------------------------+
|  <http://example.org/bob>|
|<http://example.org/carol>|
+--------------------------+

Inner query:
+--------------------------+----------+
|                    person| shortName|
+--------------------------+----------+
|<http://example.org/alice>|    A. Abc|
|  <http://example.org/bob>|    B. Abc|
|<http://example.org/carol>|    C. Abc|
+--------------------------+----------+

Expected:
+--------------------------+----------+
|                    person| shortName|
+--------------------------+----------+
|  <http://example.org/bob>|    B. Abc|
|<http://example.org/carol>|    C. Abc|
+--------------------------+----------+

Actual:

-----------------------------------------------------------------------------------------------------------------

PREFIX ex: <http://example.org/#>.

TABLE:
================================================================================================================
ex:alice		| ex:age			| "25"
ex:bob			| ex:age			| "22"
ex:carol		| ex:age			| "35"

ex:alice		| ex:knows			| ex:bob
ex:alice		| ex:knows			| ex:carol
================================================================================================================

SELECT ?person ?age
WHERE 
{
	<http://example.org/alice> <http://example.org/knows> ?person .
	{
		SELECT ?person ?age
		WHERE 
		{ ?person <http://example.org/age> ?age.FILTER(?age >= 25)} 
	}
}
-----------------------------------------------------------------------------------------------------------------
RESULT:
-----------------------------------------------------------------------------------------------------------------
Outer query:
+--------------------------+
|                    person|
+--------------------------+
|  <http://example.org/bob>|
|<http://example.org/carol>|
+--------------------------+

Inner query:
+--------------------------+----+
|                    person| age|
+--------------------------+----+
|<http://example.org/alice>|  25|
|<http://example.org/carol>|  35|
+--------------------------+----+

Expected:
+--------------------------+----+
|                    person| age|
+--------------------------+----+
|<http://example.org/carol>|  35|
+--------------------------+----+

Actual:

-----------------------------------------------------------------------------------------------------------------

PREFIX ex: <http://example.org/#>.

TABLE:
================================================================================================================
ex:alice		| ex:firstValue			| "10"
ex:alice		| ex:secondValue		| "10"

ex:bob			| ex:firstValue			| "10"
ex:bob			| ex:secondValue		| "15"

ex:carol		| ex:firstValue			| "10"
ex:carol		| ex:secondValue		| "20"

ex:alice		| ex:knows			| ex:bob
ex:alice		| ex:knows			| ex:carol
================================================================================================================

SELECT ?person ?result
WHERE 
{
	<http://example.org/alice> <http://example.org/knows> ?person .
	{
		SELECT ?person ((?a+?b) AS ?result)
		WHERE 
		{ ?person <http://example.org/firstValue> ?a.
		  ?person <http://example.org/secondValue> ?b
		} 
	}
}
-----------------------------------------------------------------------------------------------------------------
RESULT:
-----------------------------------------------------------------------------------------------------------------
Outer query:
+--------------------------+
|                    person|
+--------------------------+
|  <http://example.org/bob>|
|<http://example.org/carol>|
+--------------------------+

Inner query:
+--------------------------+-------+
|                    person| result|
+--------------------------+-------+
|<http://example.org/alice>|     20|
|  <http://example.org/bob>|     25|
|<http://example.org/carol>|     30|
+--------------------------+-------+

Expected:
+--------------------------+-------+
|                    person| result|
+--------------------------+-------+
|  <http://example.org/bob>|     25|
|<http://example.org/carol>|     30|
+--------------------------+-------+

Actual:

-----------------------------------------------------------------------------------------------------------------

*/
