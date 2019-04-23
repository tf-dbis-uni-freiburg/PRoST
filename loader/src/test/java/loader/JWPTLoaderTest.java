package loader;

import java.io.File;

import com.holdenkarau.spark.testing.JavaDataFrameSuiteBase;
import loader.utilities.HdfsUtilities;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public class JWPTLoaderTest extends JavaDataFrameSuiteBase {
	//TODO complete tests

	public void outerJWPT() throws Exception {
		final ClassLoader classLoader = getClass().getClassLoader();
		final File jwptTest = new File(
				classLoader.getResource("jwptTest.nt").getFile());
		HdfsUtilities.putFileToHDFS(jwptTest.getAbsolutePath(), System.getProperty("user.dir") +
				"\\target\\test_output\\jwptTest", jsc());

		spark().sql("DROP DATABASE IF EXISTS jwptTest_db CASCADE");

		final TripleTableLoader ttLoader = new TripleTableLoader((System.getProperty("user.dir") + "\\target"
				+ "\\test_output\\jwptTest").replace('\\', '/'),
				"jwptTest_db", spark(), false, false, true, false);
		ttLoader.load();

		final JoinedWidePropertyTableLoader jwptLoader = new JoinedWidePropertyTableLoader("jwptTest_db", spark(),
				false, JoinedWidePropertyTableLoader.JoinType.outer);
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

		final TripleTableLoader ttLoader = new TripleTableLoader((System.getProperty("user.dir") + "\\target"
				+ "\\test_output\\jwptTest").replace('\\', '/'),
				"jwptTest_db", spark(), false, false, true, false);
		ttLoader.load();

		final JoinedWidePropertyTableLoader jwptLoader = new JoinedWidePropertyTableLoader("jwptTest_db", spark(),
				false, JoinedWidePropertyTableLoader.JoinType.inner);
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

		final TripleTableLoader ttLoader = new TripleTableLoader((System.getProperty("user.dir") + "\\target"
				+ "\\test_output\\jwptTest").replace('\\', '/'),
				"jwptTest_db", spark(), false, false, true, false);
		ttLoader.load();

		final JoinedWidePropertyTableLoader jwptLoader = new JoinedWidePropertyTableLoader("jwptTest_db", spark(),
				false, JoinedWidePropertyTableLoader.JoinType.leftouter);
		jwptLoader.load();


		spark().sql("USE jwptTest_db");
		final Dataset<Row> actualJWPT = spark().sql("SELECT * FROM joined_wide_property_table_leftouter ORDER BY r");
		//actualJWPT.collectAsList();
	}

}
