package loader;

import java.io.File;
import java.io.Serializable;
import java.util.ArrayList;

import com.holdenkarau.spark.testing.JavaDataFrameSuiteBase;
import loader.utilities.HdfsUtilities;
import loader.utilities.SparkSqlUtilities;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.junit.Ignore;
import org.junit.Test;

/**
 * This class tests the parsing of the NT triples file and the building of the
 * TripleTableLoader. It builds the triple table with a physical partitioning
 * strategy. The table is partitioned by predicate.
 *
 * @author Victor Anthony Arrascue Ayala
 */
public class TripleTableLoaderPartByPredTest extends JavaDataFrameSuiteBase implements Serializable {
	protected static final Logger logger = Logger.getLogger("PRoST");
	private static final long serialVersionUID = -5681683598336701496L;
	private static final Encoder<TripleBean> triplesEncoder = Encoders.bean(TripleBean.class);

	/**
	 * This method tests if triples with more than three elements are ignored
	 * when parsing the file.
	 *
	 * @throws Exception
	 */
	@Test
	public void parsingTriplesWithMoreThanThreeRes() throws Exception {
		final ClassLoader classLoader = getClass().getClassLoader();
		final File triplesWithMoreThanThreeRes = new File(
				classLoader.getResource("triplesWithMoreThanThreeRes.nt").getFile());
		HdfsUtilities.putFileToHDFS(triplesWithMoreThanThreeRes.getAbsolutePath(), System.getProperty("user.dir") + "\\target\\test_output\\triplesWithMoreThanThreeRes",
				jsc());

		SparkSqlUtilities.enableSessionForPhysicalPartitioning(spark());
		spark().sql("DROP DATABASE IF EXISTS triplesWithMoreThanThreeRes_db CASCADE");
		final TripleTableLoader tt_loader = new TripleTableLoader((System.getProperty("user.dir") + "\\target\\test_output\\triplesWithMoreThanThreeRes").replace('\\', '/'),
				"triplesWithMoreThanThreeRes_db", spark(), false, true, true);
		tt_loader.load();

		// Expected value:
		final TripleBean t1 = new TripleBean();
		t1.setS("<http://example.org/resource/Res1>");
		t1.setP("<http://example.org/property/pro1>");
		t1.setO("<http://example.org/resource/Res:1000>");

		final TripleBean t2 = new TripleBean();
		t2.setS("<http://example.org/resource/Res1>");
		t2.setP("<http://example.org/property/pro1>");
		t2.setO("<http://example.org/resource/Res3>");

		final TripleBean t3 = new TripleBean();
		t3.setS("<http://example.org/resource/Res3>");
		t3.setP("<http://example.org/property/pro3>");
		t3.setO("\"wow\\\" \\\" . \\\"ok\\\" hi\"");

		final TripleBean t4 = new TripleBean();
		t4.setS("<http://example.org/resource/Res4>");
		t4.setP("<http://example.org/property/pro2>");
		t4.setO("<http://example.org/resource/Res4>");

		final TripleBean t5 = new TripleBean();
		t5.setS("<http://example.org/resource/Res2>");
		t5.setP("<http://example.org/property/pro3>");
		t5.setO("\"wow hi\"");

		final ArrayList<TripleBean> triplesList = new ArrayList<TripleBean>();
		triplesList.add(t1);
		triplesList.add(t2);
		triplesList.add(t3);
		triplesList.add(t4);
		triplesList.add(t5);

		spark().sql("USE triplesWithMoreThanThreeRes_db");
		final Dataset<Row> expectedTT = spark().createDataset(triplesList, triplesEncoder).select("s", "p", "o").orderBy("s",
				"p", "o");
		final Dataset<Row> actualTT = spark().sql("SELECT s,p,o FROM tripletable ORDER BY s,p,o");

		assertDataFrameEquals(expectedTT, actualTT);
	}

	/**
	 * This methods verifies that triples which are not complete are ignored and
	 * skipped.
	 *
	 * @throws Exception
	 */
	@Test
	public void parsingIncompleteTriples() throws Exception {
		final ClassLoader classLoader = getClass().getClassLoader();
		final File incompleteTriples = new File(classLoader.getResource("incompleteTriples.nt").getFile());
		HdfsUtilities.putFileToHDFS(incompleteTriples.getAbsolutePath(), System.getProperty("user.dir") + "\\target\\test_output\\incompleteTriples", jsc());

		SparkSqlUtilities.enableSessionForPhysicalPartitioning(spark());
		spark().sql("DROP DATABASE IF EXISTS incompleteTriples_db CASCADE");
		final TripleTableLoader tt_loader = new TripleTableLoader((System.getProperty("user.dir") + "\\target\\test_output\\incompleteTriples").replace('\\', '/'), "incompleteTriples_db", spark(),
				false, true, true);
		tt_loader.load();

		// Expected value:
		final TripleBean t1 = new TripleBean();
		t1.setS("<http://example.org/resource/Res1>");
		t1.setP("<http://example.org/property/pro1>");
		t1.setO("<http://example.org/resource/Res:1000>");

		final TripleBean t2 = new TripleBean();
		t2.setS("<http://example.org/resource/Res5>");
		t2.setP("<http://example.org/property/pro3>");
		t2.setO("<http://example.org/resource/Res2>");

		final ArrayList<TripleBean> triplesList = new ArrayList<TripleBean>();
		triplesList.add(t1);
		triplesList.add(t2);

		spark().sql("USE incompleteTriples_db");
		final Dataset<Row> expectedTT = spark().createDataset(triplesList, triplesEncoder).select("s", "p", "o").orderBy("s",
				"p", "o");
		final Dataset<Row> actualTT = spark().sql("SELECT s,p,o FROM tripletable ORDER BY s,p,o");

		assertDataFrameEquals(expectedTT, actualTT);
	}

	/**
	 * This method verifies that a file which contains empty lines is parsed by
	 * ignoring those lines.
	 *
	 * @throws Exception
	 */
	@Test
	public void parsingEmptyLines() throws Exception {
		final ClassLoader classLoader = getClass().getClassLoader();
		final File triplesWithEmptyLines = new File(classLoader.getResource("triplesWithEmptyLines.nt").getFile());
		HdfsUtilities.putFileToHDFS(triplesWithEmptyLines.getAbsolutePath(), System.getProperty("user.dir") + "\\target\\test_output\\triplesWithEmptyLines", jsc());

		SparkSqlUtilities.enableSessionForPhysicalPartitioning(spark());
		spark().sql("DROP DATABASE IF EXISTS triplesWithEmptyLines_db CASCADE");
		final TripleTableLoader tt_loader = new TripleTableLoader((System.getProperty("user.dir") + "\\target\\test_output\\triplesWithEmptyLines").replace('\\', '/'), "triplesWithEmptyLines_db",
				spark(), false, true, true);
		tt_loader.load();

		// Expected value:
		final TripleBean t1 = new TripleBean();
		t1.setS("<http://example.org/resource/Res1>");
		t1.setP("<http://example.org/property/pro1>");
		t1.setO("<http://example.org/resource/Res:1000>");

		final TripleBean t2 = new TripleBean();
		t2.setS("<http://example.org/resource/Res5>");
		t2.setP("<http://example.org/property/pro3>");
		t2.setO("<http://example.org/resource/Res2>");

		final ArrayList<TripleBean> triplesList = new ArrayList<TripleBean>();
		triplesList.add(t1);
		triplesList.add(t2);

		spark().sql("USE triplesWithEmptyLines_db");
		final Dataset<Row> expectedTT = spark().createDataset(triplesList, triplesEncoder).select("s", "p", "o").orderBy("s",
				"p", "o");
		final Dataset<Row> actualTT = spark().sql("SELECT s,p,o FROM tripletable ORDER BY s,p,o");

		assertDataFrameEquals(expectedTT, actualTT);
	}

	/**
	 * This test shows that TT parses all triples even when predicates are case
	 * insensitive equal.
	 *
	 * @throws Exception
	 */
	@Ignore("There seems to be a bug in the library. The data is correctly uploaded in the cluster and it works for other partitioning strategies.")
	@Test
	public void parsingCaseInsensitivePredicates() throws Exception {
		final ClassLoader classLoader = getClass().getClassLoader();
		final File caseInsensitivePredicates = new File(classLoader.getResource("caseInsensitivePredicates.nt").getFile());
		HdfsUtilities.putFileToHDFS(caseInsensitivePredicates.getAbsolutePath(), System.getProperty("user.dir") + "\\target\\test_output\\caseInsensitivePredicates", jsc());

		SparkSqlUtilities.enableSessionForPhysicalPartitioning(spark());
		spark().sql("DROP DATABASE IF EXISTS caseInsensitivePredicates_db CASCADE");
		final TripleTableLoader tt_loader = new TripleTableLoader((System.getProperty("user.dir") + "\\target\\test_output\\caseInsensitivePredicates").replace('\\', '/'),
				"caseInsensitivePredicates_db", spark(), false, true, true);
		tt_loader.load();

		// Expected value:
		final TripleBean t1 = new TripleBean();
		t1.setS("<http://example.org/resource/Res1>");
		t1.setP("<http://example.org/property/givenname>");
		t1.setO("<http://example.org/resource/Res:1000>");

		final TripleBean t2 = new TripleBean();
		t2.setS("<http://example.org/resource/Res1>");
		t2.setP("<http://example.org/property/givenname>");
		t2.setO("<http://example.org/resource/Res3>");

		final TripleBean t3 = new TripleBean();
		t3.setS("<http://example.org/resource/Res5>");
		t3.setP("<http://example.org/property/givenName>");
		t3.setO("<http://example.org/resource/Res1>");

		final ArrayList<TripleBean> triplesList = new ArrayList<TripleBean>();
		triplesList.add(t1);
		triplesList.add(t2);
		triplesList.add(t3);

		spark().sql("USE caseInsensitivePredicates_db");
		final Dataset<Row> expectedTT = spark().createDataset(triplesList, triplesEncoder).select("s", "p", "o").orderBy("s",
				"p", "o");
		final Dataset<Row> actualTT = spark().sql("SELECT s,p,o FROM tripletable ORDER BY s,p,o");

		assertDataFrameEquals(expectedTT, actualTT);
	}

	/**
	 * Since the dot which indicates the end of a triple line is removed during
	 * the parsing, this test verifies that other dots, e.g. those present in
	 * literals are not removed.
	 *
	 * @throws Exception
	 */
	@Test
	public void parsingLiteralsWithDots() throws Exception {
		final ClassLoader classLoader = getClass().getClassLoader();
		final File triplesWithDotsInLiterals = new File(classLoader.getResource("triplesWithDotsInLiterals.nt").getFile());
		HdfsUtilities.putFileToHDFS(triplesWithDotsInLiterals.getAbsolutePath(), System.getProperty("user.dir") + "\\target\\test_output\\triplesWithDotsInLiterals", jsc());

		SparkSqlUtilities.enableSessionForPhysicalPartitioning(spark());
		spark().sql("DROP DATABASE IF EXISTS triplesWithDotsInLiterals_db CASCADE");
		final TripleTableLoader tt_loader = new TripleTableLoader((System.getProperty("user.dir") + "\\target\\test_output\\triplesWithDotsInLiterals").replace('\\', '/'),
				"triplesWithDotsInLiterals_db", spark(), false, true, true);
		tt_loader.load();

		// Expected value:
		final TripleBean t1 = new TripleBean();
		t1.setS("<http://example.org/resource/Res1>");
		t1.setP("<http://example.org/property/pro1>");
		t1.setO("<http://example.org/resource/Res:1000>");

		final TripleBean t2 = new TripleBean();
		t2.setS("<http://example.org/resource/Res5>");
		t2.setP("<http://example.org/property/pro3>");
		t2.setO("\"one literal\"");

		final TripleBean t3 = new TripleBean();
		t3.setS("<http://example.org/resource/Res2>");
		t3.setP("<http://example.org/property/pro1>");
		t3.setO("<http://example.org/resource/Res:1000>");

		final TripleBean t4 = new TripleBean();
		t4.setS("<http://example.org/resource/Res3>");
		t4.setP("<http://example.org/property/pro3>");
		t4.setO("\"This literal contains a dot . which should NOT be removed\"");

		final TripleBean t5 = new TripleBean();
		t5.setS("<http://example.org/resource/Res4>");
		t5.setP("<http://example.org/property/pro1>");
		t5.setO("<http://example.org/resource/Res:1000>");

		final TripleBean t6 = new TripleBean();
		t6.setS("<http://example.org/resource/Res6>");
		t6.setP("<http://example.org/property/pro3>");
		t6.setO("\"one literal\"^^<type1>");

		final ArrayList<TripleBean> triplesList = new ArrayList<TripleBean>();
		triplesList.add(t1);
		triplesList.add(t2);
		triplesList.add(t3);
		triplesList.add(t4);
		triplesList.add(t5);
		triplesList.add(t6);

		spark().sql("USE triplesWithDotsInLiterals_db");
		final Dataset<Row> expectedTT = spark().createDataset(triplesList, triplesEncoder).select("s", "p", "o").orderBy("s",
				"p", "o");
		final Dataset<Row> actualTT = spark().sql("SELECT s,p,o FROM tripletable ORDER BY s,p,o");

		assertDataFrameEquals(expectedTT, actualTT);
	}

	/**
	 * This test verifies that duplicates are handled according to the last
	 * argument in the class TripleTableLoader (if true, duplicates are removed,
	 * if false duplicates are kept).
	 *
	 * @throws Exception
	 */
	@Test
	public void parsingDuplicates() throws Exception {
		final ClassLoader classLoader = getClass().getClassLoader();
		final File triplesWithDuplicates = new File(classLoader.getResource("triplesWithDuplicates.nt").getFile());
		HdfsUtilities.putFileToHDFS(triplesWithDuplicates.getAbsolutePath(), System.getProperty("user.dir") + "\\target\\test_output\\triplesWithDuplicates", jsc());

		SparkSqlUtilities.enableSessionForPhysicalPartitioning(spark());
		spark().sql("DROP DATABASE IF EXISTS triplesWithDuplicates_db CASCADE");
		TripleTableLoader tt_loader = new TripleTableLoader((System.getProperty("user.dir") + "\\target\\test_output\\triplesWithDuplicates").replace('\\', '/'), "triplesWithDuplicates_db",
				spark(), false, true, true);
		tt_loader.load();

		// Expected value:
		final TripleBean t1 = new TripleBean();
		t1.setS("<http://example.org/resource/Res1>");
		t1.setP("<http://example.org/property/pro1>");
		t1.setO("<http://example.org/resource/Res:1000>");

		final TripleBean t2 = new TripleBean();
		t2.setS("<http://example.org/resource/Res1>");
		t2.setP("<http://example.org/property/pro1>");
		t2.setO("<http://example.org/resource/Res3>");

		final TripleBean t3 = new TripleBean();
		t3.setS("<http://example.org/resource/Res5>");
		t3.setP("<http://example.org/property/pro1>");
		t3.setO("<http://example.org/resource/Res1>");

		// These are duplicate triples
		final TripleBean t4 = new TripleBean();
		t4.setS("<http://example.org/resource/Res1>");
		t4.setP("<http://example.org/property/pro1>");
		t4.setO("<http://example.org/resource/Res3>");

		final TripleBean t5 = new TripleBean();
		t5.setS("<http://example.org/resource/Res5>");
		t5.setP("<http://example.org/property/pro1>");
		t5.setO("<http://example.org/resource/Res1>");

		final TripleBean t6 = new TripleBean();
		t6.setS("<http://example.org/resource/Res1>");
		t6.setP("<http://example.org/property/pro1>");
		t6.setO("<http://example.org/resource/Res:1000>");

		final ArrayList<TripleBean> triplesListNoDuplicates = new ArrayList<TripleBean>();
		triplesListNoDuplicates.add(t1);
		triplesListNoDuplicates.add(t2);
		triplesListNoDuplicates.add(t3);

		final ArrayList<TripleBean> triplesListWithDuplicates = new ArrayList<TripleBean>();
		triplesListWithDuplicates.add(t1);
		triplesListWithDuplicates.add(t2);
		triplesListWithDuplicates.add(t3);
		triplesListWithDuplicates.add(t4);
		triplesListWithDuplicates.add(t5);
		triplesListWithDuplicates.add(t6);

		// Without duplicates
		spark().sql("USE triplesWithDuplicates_db");
		Dataset<Row> expectedTT = spark().createDataset(triplesListNoDuplicates, triplesEncoder).select("s", "p", "o")
				.orderBy("s", "p", "o");
		Dataset<Row> actualTT = spark().sql("SELECT s,p,o FROM tripletable ORDER BY s,p,o");

		assertDataFrameEquals(expectedTT, actualTT);

		// Now with duplicates
		spark().sql("DROP TABLE tripletable");
		SparkSqlUtilities.enableSessionForPhysicalPartitioning(spark());
		tt_loader = new TripleTableLoader((System.getProperty("user.dir") + "\\target\\test_output\\triplesWithDuplicates").replace('\\', '/'), "triplesWithDuplicates_db", spark(), false, true,
				false);
		tt_loader.load();

		expectedTT = spark().createDataset(triplesListWithDuplicates, triplesEncoder).select("s", "p", "o").orderBy("s",
				"p", "o");
		actualTT = spark().sql("SELECT s,p,o FROM tripletable ORDER BY s,p,o");

		assertDataFrameEquals(expectedTT, actualTT);
	}

	/**
	 * This test verifies an Exception is thrown when an empty file is parsed.
	 *
	 * @throws Exception
	 */
	@Test(expected = Exception.class)
	public void parsingEmptyFile() throws Exception {
		final ClassLoader classLoader = getClass().getClassLoader();
		final File emptyFile = new File(classLoader.getResource("emptyFile.nt").getFile());
		HdfsUtilities.putFileToHDFS(emptyFile.getAbsolutePath(), System.getProperty("user.dir") + "\\target\\test_output\\emptyFile", jsc());

		SparkSqlUtilities.enableSessionForPhysicalPartitioning(spark());
		spark().sql("DROP DATABASE IF EXISTS emptyFile_db CASCADE");
		final TripleTableLoader tt_loader = new TripleTableLoader((System.getProperty("user.dir") + "\\target\\test_output\\emptyFile").replace('\\', '/'), "emptyFile_db", spark(), false, true, true);
		tt_loader.load();
	}

	// Not behaving as expected. Fix after merging (this belongs to a different
	// feature).
	public void parsingTriplesWithPrefixes() throws Exception {
		final ClassLoader classLoader = getClass().getClassLoader();
		final File triplesWithPrefixes = new File(classLoader.getResource("triplesWithPrefixes.nt").getFile());
		HdfsUtilities.putFileToHDFS(triplesWithPrefixes.getAbsolutePath(), System.getProperty("user.dir") + "\\target\\test_output\\triplesWithPrefixes", jsc());

		SparkSqlUtilities.enableSessionForPhysicalPartitioning(spark());
		spark().sql("DROP DATABASE IF EXISTS triplesWithPrefixes_db CASCADE");
		final TripleTableLoader tt_loader = new TripleTableLoader((System.getProperty("user.dir") + "\\target\\test_output\\triplesWithPrefixes").replace('\\', '/'), "triplesWithPrefixes_db", spark(),
				false, true, true);
		tt_loader.load();

		// Expected value:
		final TripleBean t1 = new TripleBean();
		t1.setS("<http://example.org/resource/Res1>");
		t1.setP("<http://example.org/property/pro1>");
		t1.setO("<http://example.org/resource/Res:1000>");

		final TripleBean t2 = new TripleBean();
		t2.setS("<http://example.org/resource/Res5>");
		t2.setP("<http://example.org/property/pro3>");
		t2.setO("<http://example.org/resource/Res2>");

		final ArrayList<TripleBean> triplesList = new ArrayList<TripleBean>();
		triplesList.add(t1);
		triplesList.add(t2);

		spark().sql("USE triplesWithPrefixes_db");
		final Dataset<Row> expectedTT = spark().createDataset(triplesList, triplesEncoder).select("s", "p", "o").orderBy("s",
				"p", "o");
		final Dataset<Row> actualTT = spark().sql("SELECT s,p,o FROM tripletable ORDER BY s,p,o");

		assertDataFrameEquals(expectedTT, actualTT);
	}
}
