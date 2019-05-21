package loader;

import java.io.File;

import com.holdenkarau.spark.testing.JavaDataFrameSuiteBase;
import loader.utilities.HdfsUtilities;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import stats.DatabaseStatistics;

class JWPTLoaderTest extends JavaDataFrameSuiteBase {
	//TODO complete tests

	public void outerJWPT() throws Exception {
		final ClassLoader classLoader = getClass().getClassLoader();
		final File jwptTest = new File(
				classLoader.getResource("jwptTest.nt").getFile());
		HdfsUtilities.putFileToHDFS(jwptTest.getAbsolutePath(), System.getProperty("user.dir") +
				"\\target\\test_output\\jwptTest", jsc());

		spark().sql("DROP DATABASE IF EXISTS jwptTest_db CASCADE");


		final Settings settings = new Settings.Builder("jwptTest_db").withInputPath((System.getProperty("user.dir") + "\\target"
				+ "\\test_output\\jwptTest").replace('\\', '/')).droppingDuplicateTriples().computePropertyStatistics().build();
		final DatabaseStatistics statistics = new DatabaseStatistics("jwptTest_db");

		final TripleTableLoader ttLoader = new TripleTableLoader(settings, spark(),statistics);
		ttLoader.load();

		//TODO when statistics can be loaded from any model, vp will not be needed anymore. (It's only loaded to
		// compute statistics
		final VerticalPartitioningLoader vpLoader = new VerticalPartitioningLoader(settings, spark(), statistics);
		vpLoader.load();

		final JoinedWidePropertyTableLoader jwptLoader = new JoinedWidePropertyTableLoader(settings, spark(),
				JoinedWidePropertyTableLoader.JoinType.outer,statistics);
		jwptLoader.load();


		spark().sql("USE jwptTest_db");
		final Dataset<Row> actualJWPT = spark().sql("SELECT * FROM joined_wide_property_table_outer ORDER BY r");
		//actualJWPT.collectAsList();
	}

	public void innerJWPT() throws Exception {
		final ClassLoader classLoader = getClass().getClassLoader();
		final File jwptTest = new File(
				classLoader.getResource("jwptTest.nt").getFile());
		HdfsUtilities.putFileToHDFS(jwptTest.getAbsolutePath(), System.getProperty("user.dir") +
				"\\target\\test_output\\jwptTest", jsc());

		spark().sql("DROP DATABASE IF EXISTS jwptTest_db CASCADE");

		final Settings settings = new Settings.Builder("jwptTest_db").withInputPath((System.getProperty("user.dir") + "\\target"
				+ "\\test_output\\jwptTest").replace('\\', '/')).droppingDuplicateTriples().computePropertyStatistics().build();

		final DatabaseStatistics statistics = new DatabaseStatistics("jwptTest_db");

		final TripleTableLoader ttLoader = new TripleTableLoader(settings, spark(),statistics);
		ttLoader.load();

		//TODO when statistics can be loaded from any model, vp will not be needed anymore. (It's only loaded to
		// compute statistics
		final VerticalPartitioningLoader vpLoader = new VerticalPartitioningLoader(settings, spark(), statistics);
		vpLoader.load();

		final JoinedWidePropertyTableLoader jwptLoader = new JoinedWidePropertyTableLoader(settings, spark(),
				JoinedWidePropertyTableLoader.JoinType.inner, statistics);
		jwptLoader.load();


		spark().sql("USE jwptTest_db");
		final Dataset<Row> actualJWPT = spark().sql("SELECT * FROM joined_wide_property_table_inner ORDER BY r");
		//actualJWPT.collectAsList();
	}

	public void leftOuterJWPT() throws Exception {
		final ClassLoader classLoader = getClass().getClassLoader();
		final File jwptTest = new File(
				classLoader.getResource("jwptTest.nt").getFile());
		HdfsUtilities.putFileToHDFS(jwptTest.getAbsolutePath(), System.getProperty("user.dir") +
				"\\target\\test_output\\jwptTest", jsc());

		spark().sql("DROP DATABASE IF EXISTS jwptTest_db CASCADE");

		final Settings settings = new Settings.Builder("jwptTest_db").withInputPath((System.getProperty("user.dir") + "\\target"
				+ "\\test_output\\jwptTest").replace('\\', '/')).droppingDuplicateTriples().computePropertyStatistics().build();

		final DatabaseStatistics statistics = new DatabaseStatistics("jwptTest_db");
		final TripleTableLoader ttLoader = new TripleTableLoader(settings, spark(), statistics);
		ttLoader.load();

		//TODO when statistics can be loaded from any model, vp will not be needed anymore. (It's only loaded to
		// compute statistics
		final VerticalPartitioningLoader vpLoader = new VerticalPartitioningLoader(settings, spark(), statistics);
		vpLoader.load();

		final JoinedWidePropertyTableLoader jwptLoader = new JoinedWidePropertyTableLoader(settings, spark(),
				JoinedWidePropertyTableLoader.JoinType.leftouter, statistics);
		jwptLoader.load();
		jwptLoader.load();


		spark().sql("USE jwptTest_db");
		final Dataset<Row> actualJWPT = spark().sql("SELECT * FROM joined_wide_property_table_leftouter ORDER BY r");
		//actualJWPT.collectAsList();
	}

}
