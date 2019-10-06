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
public class ArithmeticOperationsTest extends JavaDataFrameSuiteBase implements Serializable {
	private static final long serialVersionUID = 1329L;
	private static final Encoder<TripleBean> triplesEncoder = Encoders.bean(TripleBean.class);

	@Test
	@Ignore("Operation not yet implemented.")
	public void queryTest1() throws Exception {
		final DatabaseStatistics statistics = new DatabaseStatistics("queryTestArithmeticOperation1_db");
		initializeDb(statistics);
		queryOnTT(statistics);
		queryOnVp(statistics);
		queryOnWpt(statistics);
		queryOnIwpt(statistics);
		queryOnJwptOuter(statistics);
		queryOnJwptLeftOuter(statistics);
	}

	private void queryOnTT(final DatabaseStatistics statistics) throws Exception {
		final Settings settings = new Settings.Builder("queryTestArithmeticOperation1_db").usingTTNodes().usingCharacteristicSets().build();
		final ClassLoader classLoader = getClass().getClassLoader();

		final Query query = new Query(classLoader.getResource("queryTestArithmeticOperation1.q").getPath(), statistics, settings);

		//EXPECTED
		final StructType schema = DataTypes.createStructType(new StructField[]{
				DataTypes.createStructField("result", DataTypes.StringType, true),
		});
		final Row row1 = RowFactory.create("6");
		final List<Row> rowList = ImmutableList.of(row1);
		final Dataset<Row> expectedResult = spark().createDataFrame(rowList, schema);

		//ACTUAL
		final Dataset<Row> actualResult = query.compute(spark().sqlContext()).orderBy("result");
		final Dataset<Row> nullableActualResult = sqlContext().createDataFrame(actualResult.collectAsList(),
				actualResult.schema().asNullable());
		System.out.print("ArithmeticOperationsTest: queryTest1");
		expectedResult.printSchema();
		expectedResult.show();

		nullableActualResult.printSchema();
		nullableActualResult.show();
		assertDataFrameEquals(expectedResult, nullableActualResult);
	}

	private void queryOnVp(final DatabaseStatistics statistics) throws Exception {
		final Settings settings = new Settings.Builder("queryTestArithmeticOperation1_db").usingVPNodes().build();
		final ClassLoader classLoader = getClass().getClassLoader();

		final Query query = new Query(classLoader.getResource("queryTestArithmeticOperation1.q").getPath(), statistics, settings);

		//EXPECTED
		final StructType schema = DataTypes.createStructType(new StructField[]{
				DataTypes.createStructField("result", DataTypes.StringType, true),
		});
		final Row row1 = RowFactory.create("6");
		final List<Row> rowList = ImmutableList.of(row1);
		final Dataset<Row> expectedResult = spark().createDataFrame(rowList, schema);

		//ACTUAL
		final Dataset<Row> actualResult = query.compute(spark().sqlContext()).orderBy("result");
		final Dataset<Row> nullableActualResult = sqlContext().createDataFrame(actualResult.collectAsList(),
				actualResult.schema().asNullable());

		assertDataFrameEquals(expectedResult, nullableActualResult);
	}

	private void queryOnWpt(final DatabaseStatistics statistics) throws Exception {
		final Settings settings = new Settings.Builder("queryTestArithmeticOperation1_db").usingWPTNodes().build();
		final ClassLoader classLoader = getClass().getClassLoader();

		final Query query = new Query(classLoader.getResource("queryTestArithmeticOperation1.q").getPath(), statistics, settings);

		//EXPECTED
		final StructType schema = DataTypes.createStructType(new StructField[]{
				DataTypes.createStructField("result", DataTypes.StringType, true),
		});
		final Row row1 = RowFactory.create("6");
		final List<Row> rowList = ImmutableList.of(row1);
		final Dataset<Row> expectedResult = spark().createDataFrame(rowList, schema);

		//ACTUAL
		final Dataset<Row> actualResult = query.compute(spark().sqlContext()).orderBy("result");
		final Dataset<Row> nullableActualResult = sqlContext().createDataFrame(actualResult.collectAsList(),
				actualResult.schema().asNullable());

		assertDataFrameEquals(expectedResult, nullableActualResult);
	}

	private void queryOnIwpt(final DatabaseStatistics statistics) throws Exception {
		final Settings settings = new Settings.Builder("queryTestArithmeticOperation1_db").usingIWPTNodes().build();
		final ClassLoader classLoader = getClass().getClassLoader();

		final Query query = new Query(classLoader.getResource("queryTestArithmeticOperation1.q").getPath(), statistics, settings);

		//EXPECTED
		final StructType schema = DataTypes.createStructType(new StructField[]{
				DataTypes.createStructField("result", DataTypes.StringType, true),
		});
		final Row row1 = RowFactory.create("6");
		final List<Row> rowList = ImmutableList.of(row1);
		final Dataset<Row> expectedResult = spark().createDataFrame(rowList, schema);

		//ACTUAL
		final Dataset<Row> actualResult = query.compute(spark().sqlContext()).orderBy("result");
		final Dataset<Row> nullableActualResult = sqlContext().createDataFrame(actualResult.collectAsList(),
				actualResult.schema().asNullable());

		assertDataFrameEquals(expectedResult, nullableActualResult);
	}

	private void queryOnJwptOuter(final DatabaseStatistics statistics) throws Exception {
		final Settings settings = new Settings.Builder("queryTestArithmeticOperation1_db").usingJWPTOuterNodes().build();
		final ClassLoader classLoader = getClass().getClassLoader();

		final Query query = new Query(classLoader.getResource("queryTestArithmeticOperation1.q").getPath(), statistics, settings);

		//EXPECTED
		final StructType schema = DataTypes.createStructType(new StructField[]{
				DataTypes.createStructField("result", DataTypes.StringType, true),
		});
		final Row row1 = RowFactory.create("6");
		final List<Row> rowList = ImmutableList.of(row1);
		final Dataset<Row> expectedResult = spark().createDataFrame(rowList, schema);

		//ACTUAL
		final Dataset<Row> actualResult = query.compute(spark().sqlContext()).orderBy("result");
		final Dataset<Row> nullableActualResult = sqlContext().createDataFrame(actualResult.collectAsList(),
				actualResult.schema().asNullable());

		assertDataFrameEquals(expectedResult, nullableActualResult);
	}

	private void queryOnJwptLeftOuter(final DatabaseStatistics statistics) throws Exception {
		final Settings settings = new Settings.Builder("queryTestArithmeticOperation1_db").usingJWPTLeftouterNodes().build();
		final ClassLoader classLoader = getClass().getClassLoader();

		final Query query = new Query(classLoader.getResource("queryTestArithmeticOperation1.q").getPath(), statistics, settings);

		//EXPECTED
		final StructType schema = DataTypes.createStructType(new StructField[]{
				DataTypes.createStructField("result", DataTypes.StringType, true),
		});
		final Row row1 = RowFactory.create("6");
		final List<Row> rowList = ImmutableList.of(row1);
		final Dataset<Row> expectedResult = spark().createDataFrame(rowList, schema);

		//ACTUAL
		final Dataset<Row> actualResult = query.compute(spark().sqlContext()).orderBy("result");
		final Dataset<Row> nullableActualResult = sqlContext().createDataFrame(actualResult.collectAsList(),
				actualResult.schema().asNullable());

		assertDataFrameEquals(expectedResult, nullableActualResult);
	}

	private void initializeDb(final DatabaseStatistics statistics) {
		spark().sql("DROP DATABASE IF EXISTS queryTestArithmeticOperation1_db CASCADE");
		spark().sql("CREATE DATABASE IF NOT EXISTS  queryTestArithmeticOperation1_db");
		spark().sql("USE queryTestArithmeticOperation1_db");

		// creates test tt table
		final TripleBean t1 = new TripleBean();
		t1.setS("<http://example.org/ValueOne>");
		t1.setP("<http://example.org/firstValue>");
		t1.setO("4");

		final TripleBean t2 = new TripleBean();
		t2.setS("<http://example.org/ValueTwo>");
		t2.setP("<http://example.org/secondValue>");
		t2.setO("2");

		final ArrayList<TripleBean> triplesList = new ArrayList<>();
		triplesList.add(t1);
		triplesList.add(t2);

		final Dataset<Row> ttDataset = spark().createDataset(triplesList, triplesEncoder).select("s", "p", "o").orderBy(
				"s", "p", "o");
		ttDataset.write().saveAsTable("tripletable");

		final loader.Settings loaderSettings =
				new loader.Settings.Builder("queryTestArithmeticOperation1_db").withInputPath((System.getProperty(
						"user.dir") + "\\target\\test_output\\ArithmeticOperationsTest").replace('\\', '/'))
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

	@Test
	@Ignore("Operation not yet implemented.")
	public void queryTest2() throws Exception {
		final DatabaseStatistics statistics = new DatabaseStatistics("queryTestArithmeticOperation2_db");
		initializeDb2(statistics);
		queryOnTT2(statistics);
		queryOnVp2(statistics);
		queryOnWpt2(statistics);
		queryOnIwpt2(statistics);
		queryOnJwptOuter2(statistics);
		queryOnJwptLeftOuter2(statistics);
	}

	private void queryOnTT2(final DatabaseStatistics statistics) throws Exception {
		final Settings settings = new Settings.Builder("queryTestArithmeticOperation2_db").usingTTNodes().usingCharacteristicSets().build();
		final ClassLoader classLoader = getClass().getClassLoader();

		final Query query = new Query(classLoader.getResource("queryTestArithmeticOperation2.q").getPath(), statistics, settings);

		//EXPECTED
		final StructType schema = DataTypes.createStructType(new StructField[]{
				DataTypes.createStructField("result", DataTypes.StringType, true),
		});
		final Row row1 = RowFactory.create("2");
		final List<Row> rowList = ImmutableList.of(row1);
		final Dataset<Row> expectedResult = spark().createDataFrame(rowList, schema);

		//ACTUAL
		final Dataset<Row> actualResult = query.compute(spark().sqlContext()).orderBy("result");
		final Dataset<Row> nullableActualResult = sqlContext().createDataFrame(actualResult.collectAsList(),
				actualResult.schema().asNullable());
		System.out.print("ArithmeticOperationsTest: queryTest2");
		expectedResult.printSchema();
		expectedResult.show();

		nullableActualResult.printSchema();
		nullableActualResult.show();
		assertDataFrameEquals(expectedResult, nullableActualResult);
	}

	private void queryOnVp2(final DatabaseStatistics statistics) throws Exception {
		final Settings settings = new Settings.Builder("queryTestArithmeticOperation2_db").usingVPNodes().build();
		final ClassLoader classLoader = getClass().getClassLoader();

		final Query query = new Query(classLoader.getResource("queryTestArithmeticOperation2.q").getPath(), statistics, settings);

		//EXPECTED
		final StructType schema = DataTypes.createStructType(new StructField[]{
				DataTypes.createStructField("result", DataTypes.StringType, true),
		});
		final Row row1 = RowFactory.create("2");
		final List<Row> rowList = ImmutableList.of(row1);
		final Dataset<Row> expectedResult = spark().createDataFrame(rowList, schema);

		//ACTUAL
		final Dataset<Row> actualResult = query.compute(spark().sqlContext()).orderBy("result");
		final Dataset<Row> nullableActualResult = sqlContext().createDataFrame(actualResult.collectAsList(),
				actualResult.schema().asNullable());

		assertDataFrameEquals(expectedResult, nullableActualResult);
	}

	private void queryOnWpt2(final DatabaseStatistics statistics) throws Exception {
		final Settings settings = new Settings.Builder("queryTestArithmeticOperation2_db").usingWPTNodes().build();
		final ClassLoader classLoader = getClass().getClassLoader();

		final Query query = new Query(classLoader.getResource("queryTestArithmeticOperation2.q").getPath(), statistics, settings);

		//EXPECTED
		final StructType schema = DataTypes.createStructType(new StructField[]{
				DataTypes.createStructField("result", DataTypes.StringType, true),
		});
		final Row row1 = RowFactory.create("2");
		final List<Row> rowList = ImmutableList.of(row1);
		final Dataset<Row> expectedResult = spark().createDataFrame(rowList, schema);

		//ACTUAL
		final Dataset<Row> actualResult = query.compute(spark().sqlContext()).orderBy("result");
		final Dataset<Row> nullableActualResult = sqlContext().createDataFrame(actualResult.collectAsList(),
				actualResult.schema().asNullable());

		assertDataFrameEquals(expectedResult, nullableActualResult);
	}

	private void queryOnIwpt2(final DatabaseStatistics statistics) throws Exception {
		final Settings settings = new Settings.Builder("queryTestArithmeticOperation2_db").usingIWPTNodes().build();
		final ClassLoader classLoader = getClass().getClassLoader();

		final Query query = new Query(classLoader.getResource("queryTestArithmeticOperation2.q").getPath(), statistics, settings);

		//EXPECTED
		final StructType schema = DataTypes.createStructType(new StructField[]{
				DataTypes.createStructField("result", DataTypes.StringType, true),
		});
		final Row row1 = RowFactory.create("2");
		final List<Row> rowList = ImmutableList.of(row1);
		final Dataset<Row> expectedResult = spark().createDataFrame(rowList, schema);

		//ACTUAL
		final Dataset<Row> actualResult = query.compute(spark().sqlContext()).orderBy("result");
		final Dataset<Row> nullableActualResult = sqlContext().createDataFrame(actualResult.collectAsList(),
				actualResult.schema().asNullable());

		assertDataFrameEquals(expectedResult, nullableActualResult);
	}

	private void queryOnJwptOuter2(final DatabaseStatistics statistics) throws Exception {
		final Settings settings = new Settings.Builder("queryTestArithmeticOperation2_db").usingJWPTOuterNodes().build();
		final ClassLoader classLoader = getClass().getClassLoader();

		final Query query = new Query(classLoader.getResource("queryTestArithmeticOperation2.q").getPath(), statistics, settings);

		//EXPECTED
		final StructType schema = DataTypes.createStructType(new StructField[]{
				DataTypes.createStructField("result", DataTypes.StringType, true),
		});
		final Row row1 = RowFactory.create("2");
		final List<Row> rowList = ImmutableList.of(row1);
		final Dataset<Row> expectedResult = spark().createDataFrame(rowList, schema);

		//ACTUAL
		final Dataset<Row> actualResult = query.compute(spark().sqlContext()).orderBy("result");
		final Dataset<Row> nullableActualResult = sqlContext().createDataFrame(actualResult.collectAsList(),
				actualResult.schema().asNullable());

		assertDataFrameEquals(expectedResult, nullableActualResult);
	}

	private void queryOnJwptLeftOuter2(final DatabaseStatistics statistics) throws Exception {
		final Settings settings = new Settings.Builder("queryTestArithmeticOperation2_db").usingJWPTLeftouterNodes().build();
		final ClassLoader classLoader = getClass().getClassLoader();

		final Query query = new Query(classLoader.getResource("queryTestArithmeticOperation2.q").getPath(), statistics, settings);

		//EXPECTED
		final StructType schema = DataTypes.createStructType(new StructField[]{
				DataTypes.createStructField("result", DataTypes.StringType, true),
		});
		final Row row1 = RowFactory.create("2");
		final List<Row> rowList = ImmutableList.of(row1);
		final Dataset<Row> expectedResult = spark().createDataFrame(rowList, schema);

		//ACTUAL
		final Dataset<Row> actualResult = query.compute(spark().sqlContext()).orderBy("result");
		final Dataset<Row> nullableActualResult = sqlContext().createDataFrame(actualResult.collectAsList(),
				actualResult.schema().asNullable());

		assertDataFrameEquals(expectedResult, nullableActualResult);
	}

	private void initializeDb2(final DatabaseStatistics statistics) {
		spark().sql("DROP DATABASE IF EXISTS queryTestArithmeticOperation2_db CASCADE");
		spark().sql("CREATE DATABASE IF NOT EXISTS  queryTestArithmeticOperation2_db");
		spark().sql("USE queryTestArithmeticOperation2_db");

		// creates test tt table
		final TripleBean t1 = new TripleBean();
		t1.setS("<http://example.org/ValueOne>");
		t1.setP("<http://example.org/firstValue>");
		t1.setO("4");

		final TripleBean t2 = new TripleBean();
		t2.setS("<http://example.org/ValueTwo>");
		t2.setP("<http://example.org/secondValue>");
		t2.setO("2");

		final ArrayList<TripleBean> triplesList = new ArrayList<>();
		triplesList.add(t1);
		triplesList.add(t2);

		final Dataset<Row> ttDataset = spark().createDataset(triplesList, triplesEncoder).select("s", "p", "o").orderBy(
				"s", "p", "o");
		ttDataset.write().saveAsTable("tripletable");

		final loader.Settings loaderSettings =
				new loader.Settings.Builder("queryTestArithmeticOperation2_db").withInputPath((System.getProperty(
						"user.dir") + "\\target\\test_output\\ArithmeticOperationsTest").replace('\\', '/'))
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

	@Test
	@Ignore("Operation not yet implemented.")
	public void queryTest3() throws Exception {
		final DatabaseStatistics statistics = new DatabaseStatistics("queryTestArithmeticOperation3_db");
		initializeDb3(statistics);
		queryOnTT3(statistics);
		queryOnVp3(statistics);
		queryOnWpt3(statistics);
		queryOnIwpt3(statistics);
		queryOnJwptOuter3(statistics);
		queryOnJwptLeftOuter3(statistics);
	}

	private void queryOnTT3(final DatabaseStatistics statistics) throws Exception {
		final Settings settings = new Settings.Builder("queryTestArithmeticOperation3_db").usingTTNodes().usingCharacteristicSets().build();
		final ClassLoader classLoader = getClass().getClassLoader();
		final Query query = new Query(classLoader.getResource("queryTestArithmeticOperation3.q").getPath(), statistics, settings);

		//EXPECTED
		final StructType schema = DataTypes.createStructType(new StructField[]{
				DataTypes.createStructField("result", DataTypes.StringType, true),
		});
		final Row row1 = RowFactory.create("8");
		final List<Row> rowList = ImmutableList.of(row1);
		final Dataset<Row> expectedResult = spark().createDataFrame(rowList, schema);

		//ACTUAL
		final Dataset<Row> actualResult = query.compute(spark().sqlContext()).orderBy("result");
		final Dataset<Row> nullableActualResult = sqlContext().createDataFrame(actualResult.collectAsList(),
				actualResult.schema().asNullable());
		System.out.print("ArithmeticOperationsTest: queryTest3");
		expectedResult.printSchema();
		expectedResult.show();

		nullableActualResult.printSchema();
		nullableActualResult.show();
		assertDataFrameEquals(expectedResult, nullableActualResult);
	}

	private void queryOnVp3(final DatabaseStatistics statistics) throws Exception {
		final Settings settings = new Settings.Builder("queryTestArithmeticOperation3_db").usingVPNodes().build();
		final ClassLoader classLoader = getClass().getClassLoader();

		final Query query = new Query(classLoader.getResource("queryTestArithmeticOperation3.q").getPath(), statistics, settings);

		//EXPECTED
		final StructType schema = DataTypes.createStructType(new StructField[]{
				DataTypes.createStructField("result", DataTypes.StringType, true),
		});
		final Row row1 = RowFactory.create("8");
		final List<Row> rowList = ImmutableList.of(row1);
		final Dataset<Row> expectedResult = spark().createDataFrame(rowList, schema);

		//ACTUAL
		final Dataset<Row> actualResult = query.compute(spark().sqlContext()).orderBy("result");
		final Dataset<Row> nullableActualResult = sqlContext().createDataFrame(actualResult.collectAsList(),
				actualResult.schema().asNullable());

		assertDataFrameEquals(expectedResult, nullableActualResult);
	}

	private void queryOnWpt3(final DatabaseStatistics statistics) throws Exception {
		final Settings settings = new Settings.Builder("queryTestArithmeticOperation3_db").usingWPTNodes().build();
		final ClassLoader classLoader = getClass().getClassLoader();

		final Query query = new Query(classLoader.getResource("queryTestArithmeticOperation3.q").getPath(), statistics, settings);

		//EXPECTED
		final StructType schema = DataTypes.createStructType(new StructField[]{
				DataTypes.createStructField("result", DataTypes.StringType, true),
		});
		final Row row1 = RowFactory.create("8");
		final List<Row> rowList = ImmutableList.of(row1);
		final Dataset<Row> expectedResult = spark().createDataFrame(rowList, schema);

		//ACTUAL
		final Dataset<Row> actualResult = query.compute(spark().sqlContext()).orderBy("result");
		final Dataset<Row> nullableActualResult = sqlContext().createDataFrame(actualResult.collectAsList(),
				actualResult.schema().asNullable());

		assertDataFrameEquals(expectedResult, nullableActualResult);
	}

	private void queryOnIwpt3(final DatabaseStatistics statistics) throws Exception {
		final Settings settings = new Settings.Builder("queryTestArithmeticOperation3_db").usingIWPTNodes().build();
		final ClassLoader classLoader = getClass().getClassLoader();

		final Query query = new Query(classLoader.getResource("queryTestArithmeticOperation3.q").getPath(), statistics, settings);

		//EXPECTED
		final StructType schema = DataTypes.createStructType(new StructField[]{
				DataTypes.createStructField("result", DataTypes.StringType, true),
		});
		final Row row1 = RowFactory.create("8");
		final List<Row> rowList = ImmutableList.of(row1);
		final Dataset<Row> expectedResult = spark().createDataFrame(rowList, schema);

		//ACTUAL
		final Dataset<Row> actualResult = query.compute(spark().sqlContext()).orderBy("result");
		final Dataset<Row> nullableActualResult = sqlContext().createDataFrame(actualResult.collectAsList(),
				actualResult.schema().asNullable());

		assertDataFrameEquals(expectedResult, nullableActualResult);
	}

	private void queryOnJwptOuter3(final DatabaseStatistics statistics) throws Exception {
		final Settings settings = new Settings.Builder("queryTestArithmeticOperation3_db").usingJWPTOuterNodes().build();
		final ClassLoader classLoader = getClass().getClassLoader();

		final Query query = new Query(classLoader.getResource("queryTestArithmeticOperation3.q").getPath(), statistics, settings);

		//EXPECTED
		final StructType schema = DataTypes.createStructType(new StructField[]{
				DataTypes.createStructField("result", DataTypes.StringType, true),
		});
		final Row row1 = RowFactory.create("8");
		final List<Row> rowList = ImmutableList.of(row1);
		final Dataset<Row> expectedResult = spark().createDataFrame(rowList, schema);

		//ACTUAL
		final Dataset<Row> actualResult = query.compute(spark().sqlContext()).orderBy("result");
		final Dataset<Row> nullableActualResult = sqlContext().createDataFrame(actualResult.collectAsList(),
				actualResult.schema().asNullable());

		assertDataFrameEquals(expectedResult, nullableActualResult);
	}

	private void queryOnJwptLeftOuter3(final DatabaseStatistics statistics) throws Exception {
		final Settings settings = new Settings.Builder("queryTestArithmeticOperation3_db").usingJWPTLeftouterNodes().build();
		final ClassLoader classLoader = getClass().getClassLoader();

		final Query query = new Query(classLoader.getResource("queryTestArithmeticOperation3.q").getPath(), statistics, settings);

		//EXPECTED
		final StructType schema = DataTypes.createStructType(new StructField[]{
				DataTypes.createStructField("result", DataTypes.StringType, true),
		});
		final Row row1 = RowFactory.create("8");
		final List<Row> rowList = ImmutableList.of(row1);
		final Dataset<Row> expectedResult = spark().createDataFrame(rowList, schema);

		//ACTUAL
		final Dataset<Row> actualResult = query.compute(spark().sqlContext()).orderBy("result");
		final Dataset<Row> nullableActualResult = sqlContext().createDataFrame(actualResult.collectAsList(),
				actualResult.schema().asNullable());

		assertDataFrameEquals(expectedResult, nullableActualResult);
	}

	private void initializeDb3(final DatabaseStatistics statistics) {
		spark().sql("DROP DATABASE IF EXISTS queryTestArithmeticOperation3_db CASCADE");
		spark().sql("CREATE DATABASE IF NOT EXISTS  queryTestArithmeticOperation3_db");
		spark().sql("USE queryTestArithmeticOperation3_db");

		// creates test tt table
		final TripleBean t1 = new TripleBean();
		t1.setS("<http://example.org/ValueOne>");
		t1.setP("<http://example.org/firstValue>");
		t1.setO("4");

		final TripleBean t2 = new TripleBean();
		t2.setS("<http://example.org/ValueTwo>");
		t2.setP("<http://example.org/secondValue>");
		t2.setO("2");

		final ArrayList<TripleBean> triplesList = new ArrayList<>();
		triplesList.add(t1);
		triplesList.add(t2);

		final Dataset<Row> ttDataset = spark().createDataset(triplesList, triplesEncoder).select("s", "p", "o").orderBy(
				"s", "p", "o");
		ttDataset.write().saveAsTable("tripletable");

		final loader.Settings loaderSettings =
				new loader.Settings.Builder("queryTestArithmeticOperation3_db").withInputPath((System.getProperty(
						"user.dir") + "\\target\\test_output\\ArithmeticOperationsTest").replace('\\', '/'))
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
	}

	@Test
	@Ignore("Operation not yet implemented.")
	public void queryTest4() throws Exception {
		final DatabaseStatistics statistics = new DatabaseStatistics("queryTestArithmeticOperation4_db");
		initializeDb4(statistics);
		queryOnTT4(statistics);
		queryOnVp4(statistics);
		queryOnWpt4(statistics);
		queryOnIwpt4(statistics);
		queryOnJwptOuter4(statistics);
		queryOnJwptLeftOuter4(statistics);
	}

	private void queryOnTT4(final DatabaseStatistics statistics) throws Exception {
		final Settings settings = new Settings.Builder("queryTestArithmeticOperation4_db").usingTTNodes().usingCharacteristicSets().build();
		final ClassLoader classLoader = getClass().getClassLoader();
		final Query query = new Query(classLoader.getResource("queryTestArithmeticOperation4.q").getPath(), statistics, settings);

		//EXPECTED
		final StructType schema = DataTypes.createStructType(new StructField[]{
				DataTypes.createStructField("result", DataTypes.StringType, true),
		});
		final Row row1 = RowFactory.create("2");
		final List<Row> rowList = ImmutableList.of(row1);
		final Dataset<Row> expectedResult = spark().createDataFrame(rowList, schema);

		//ACTUAL
		final Dataset<Row> actualResult = query.compute(spark().sqlContext()).orderBy("result");
		final Dataset<Row> nullableActualResult = sqlContext().createDataFrame(actualResult.collectAsList(),
				actualResult.schema().asNullable());
		System.out.print("ArithmeticOperationsTest: queryTest4");
		expectedResult.printSchema();
		expectedResult.show();

		nullableActualResult.printSchema();
		nullableActualResult.show();
		assertDataFrameEquals(expectedResult, nullableActualResult);
	}

	private void queryOnVp4(final DatabaseStatistics statistics) throws Exception {
		final Settings settings = new Settings.Builder("queryTestArithmeticOperation4_db").usingVPNodes().build();
		final ClassLoader classLoader = getClass().getClassLoader();
		final Query query = new Query(classLoader.getResource("queryTestArithmeticOperation4.q").getPath(), statistics, settings);

		//EXPECTED
		final StructType schema = DataTypes.createStructType(new StructField[]{
				DataTypes.createStructField("result", DataTypes.StringType, true),
		});
		final Row row1 = RowFactory.create("2");
		final List<Row> rowList = ImmutableList.of(row1);
		final Dataset<Row> expectedResult = spark().createDataFrame(rowList, schema);

		//ACTUAL
		final Dataset<Row> actualResult = query.compute(spark().sqlContext()).orderBy("result");
		final Dataset<Row> nullableActualResult = sqlContext().createDataFrame(actualResult.collectAsList(),
				actualResult.schema().asNullable());

		assertDataFrameEquals(expectedResult, nullableActualResult);
	}

	private void queryOnWpt4(final DatabaseStatistics statistics) throws Exception {
		final Settings settings = new Settings.Builder("queryTestArithmeticOperation4_db").usingWPTNodes().build();
		final ClassLoader classLoader = getClass().getClassLoader();
		final Query query = new Query(classLoader.getResource("queryTestArithmeticOperation4.q").getPath(), statistics, settings);

		//EXPECTED
		final StructType schema = DataTypes.createStructType(new StructField[]{
				DataTypes.createStructField("result", DataTypes.StringType, true),
		});
		final Row row1 = RowFactory.create("2");
		final List<Row> rowList = ImmutableList.of(row1);
		final Dataset<Row> expectedResult = spark().createDataFrame(rowList, schema);

		//ACTUAL
		final Dataset<Row> actualResult = query.compute(spark().sqlContext()).orderBy("result");
		final Dataset<Row> nullableActualResult = sqlContext().createDataFrame(actualResult.collectAsList(),
				actualResult.schema().asNullable());

		assertDataFrameEquals(expectedResult, nullableActualResult);
	}

	private void queryOnIwpt4(final DatabaseStatistics statistics) throws Exception {
		final Settings settings = new Settings.Builder("queryTestArithmeticOperation4_db").usingIWPTNodes().build();
		final ClassLoader classLoader = getClass().getClassLoader();
		final Query query = new Query(classLoader.getResource("queryTestArithmeticOperation4.q").getPath(), statistics, settings);

		//EXPECTED
		final StructType schema = DataTypes.createStructType(new StructField[]{
				DataTypes.createStructField("result", DataTypes.StringType, true),
		});
		final Row row1 = RowFactory.create("2");
		final List<Row> rowList = ImmutableList.of(row1);
		final Dataset<Row> expectedResult = spark().createDataFrame(rowList, schema);

		//ACTUAL
		final Dataset<Row> actualResult = query.compute(spark().sqlContext()).orderBy("result");
		final Dataset<Row> nullableActualResult = sqlContext().createDataFrame(actualResult.collectAsList(),
				actualResult.schema().asNullable());

		assertDataFrameEquals(expectedResult, nullableActualResult);
	}

	private void queryOnJwptOuter4(final DatabaseStatistics statistics) throws Exception {
		final Settings settings = new Settings.Builder("queryTestArithmeticOperation4_db").usingJWPTOuterNodes().build();
		final ClassLoader classLoader = getClass().getClassLoader();
		final Query query = new Query(classLoader.getResource("queryTestArithmeticOperation4.q").getPath(), statistics, settings);

		//EXPECTED
		final StructType schema = DataTypes.createStructType(new StructField[]{
				DataTypes.createStructField("result", DataTypes.StringType, true),
		});
		final Row row1 = RowFactory.create("2");
		final List<Row> rowList = ImmutableList.of(row1);
		final Dataset<Row> expectedResult = spark().createDataFrame(rowList, schema);

		//ACTUAL
		final Dataset<Row> actualResult = query.compute(spark().sqlContext()).orderBy("result");
		final Dataset<Row> nullableActualResult = sqlContext().createDataFrame(actualResult.collectAsList(),
				actualResult.schema().asNullable());

		assertDataFrameEquals(expectedResult, nullableActualResult);
	}

	private void queryOnJwptLeftOuter4(final DatabaseStatistics statistics) throws Exception {
		final Settings settings = new Settings.Builder("queryTestArithmeticOperation4_db").usingJWPTLeftouterNodes().build();
		final ClassLoader classLoader = getClass().getClassLoader();
		final Query query = new Query(classLoader.getResource("queryTestArithmeticOperation4.q").getPath(), statistics, settings);

		//EXPECTED
		final StructType schema = DataTypes.createStructType(new StructField[]{
				DataTypes.createStructField("result", DataTypes.StringType, true),
		});
		final Row row1 = RowFactory.create("2");
		final List<Row> rowList = ImmutableList.of(row1);
		final Dataset<Row> expectedResult = spark().createDataFrame(rowList, schema);

		//ACTUAL
		final Dataset<Row> actualResult = query.compute(spark().sqlContext()).orderBy("result");
		final Dataset<Row> nullableActualResult = sqlContext().createDataFrame(actualResult.collectAsList(),
				actualResult.schema().asNullable());

		assertDataFrameEquals(expectedResult, nullableActualResult);
	}

	private void initializeDb4(final DatabaseStatistics statistics) {
		spark().sql("DROP DATABASE IF EXISTS queryTestArithmeticOperation4_db CASCADE");
		spark().sql("CREATE DATABASE IF NOT EXISTS  queryTestArithmeticOperation4_db");
		spark().sql("USE queryTestArithmeticOperation4_db");

		// creates test tt table
		final TripleBean t1 = new TripleBean();
		t1.setS("<http://example.org/ValueOne>");
		t1.setP("<http://example.org/firstValue>");
		t1.setO("4");

		final TripleBean t2 = new TripleBean();
		t2.setS("<http://example.org/ValueTwo>");
		t2.setP("<http://example.org/secondValue>");
		t2.setO("2");

		final ArrayList<TripleBean> triplesList = new ArrayList<>();
		triplesList.add(t1);
		triplesList.add(t2);

		final Dataset<Row> ttDataset = spark().createDataset(triplesList, triplesEncoder).select("s", "p", "o").orderBy(
				"s", "p", "o");
		ttDataset.write().saveAsTable("tripletable");

		final loader.Settings loaderSettings =
				new loader.Settings.Builder("queryTestArithmeticOperation4_db").withInputPath((System.getProperty(
						"user.dir") + "\\target\\test_output\\ArithmeticOperationsTest").replace('\\', '/'))
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
ex:ValueOne		| ex:firstValue			| "4"
ex:ValueTwo		| ex:secondValue		| "2"
================================================================================================================

QUERY:
-----------------------------------------------------------------------------------------------------------------
SELECT ((?a+?b) AS ?result)
WHERE
{
    ?value <http://example.org/firstValue> ?a .
    ?value <http://example.org/secondValue> ?b .
}
-----------------------------------------------------------------------------------------------------------------

RESULT:
-----------------------------------------------------------------------------------------------------------------
Expected:
+------+
|result|
+------+
|     6|
+------+

Actual:

-----------------------------------------------------------------------------------------------------------------

QUERY:
-----------------------------------------------------------------------------------------------------------------
SELECT ((?a-?b) AS ?result)
WHERE
{
    ?value <http://example.org/firstValue> ?a .
    ?value <http://example.org/secondValue> ?b .
}
-----------------------------------------------------------------------------------------------------------------

RESULT:
-----------------------------------------------------------------------------------------------------------------
Expected:
+------+
|result|
+------+
|     2|
+------+

Actual:

-----------------------------------------------------------------------------------------------------------------

QUERY:
-----------------------------------------------------------------------------------------------------------------
SELECT ((?a*?b) AS ?result)
WHERE
{
    ?value <http://example.org/firstValue> ?a .
    ?value <http://example.org/secondValue> ?b .
}
-----------------------------------------------------------------------------------------------------------------

RESULT:
-----------------------------------------------------------------------------------------------------------------
Expected:
+------+
|result|
+------+
|     8|
+------+

Actual:

-----------------------------------------------------------------------------------------------------------------

QUERY:
-----------------------------------------------------------------------------------------------------------------
SELECT ((?a/?b) AS ?result)
WHERE
{
    ?value <http://example.org/firstValue> ?a .
    ?value <http://example.org/secondValue> ?b .
}
-----------------------------------------------------------------------------------------------------------------

RESULT:
-----------------------------------------------------------------------------------------------------------------
Expected:
+------+
|result|
+------+
|     2|
+------+

Actual:

-----------------------------------------------------------------------------------------------------------------
*/
