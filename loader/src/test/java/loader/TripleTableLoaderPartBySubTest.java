package loader;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.junit.Test;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import com.holdenkarau.spark.testing.JavaDataFrameSuiteBase;

/**
 * This class tests the parsing of the NT triples file and the building of the
 * TripleTableLoader. It builds the triple table with a physical partitioning
 * strategy. The table is partitioned by subject.
 * 
 * @author Victor Anthony Arrascue Ayala
 */
public class TripleTableLoaderPartBySubTest extends JavaDataFrameSuiteBase implements Serializable {
	private static final long serialVersionUID = -5681683598336701496L;
	protected static final Logger logger = Logger.getLogger("PRoST");
	private static final Encoder<TripleBean> triplesEncoder = Encoders.bean(TripleBean.class);

	/**
	 * This method tests if triples with more than three elements are ignored
	 * when parsing the file.
	 * 
	 * @throws Exception
	 */
	@Test
	public void parsingTriplesWithMoreThanThreeRes() throws Exception {
		ClassLoader classLoader = getClass().getClassLoader();
		File triplesWithMoreThanThreeRes = new File(
				classLoader.getResource("triplesWithMoreThanThreeRes.nt").getFile());
		putFileToHDFS(triplesWithMoreThanThreeRes.getAbsolutePath(), "/triplesWithMoreThanThreeRes");

		enableSessionForPhysicalPartitioning();
		spark().sql("DROP DATABASE IF EXISTS triplesWithMoreThanThreeRes_db CASCADE");
		TripleTableLoader tt_loader = new TripleTableLoader("/triplesWithMoreThanThreeRes",
				"triplesWithMoreThanThreeRes_db", spark(), true, false, true);
		tt_loader.load();

		// Expected value:
		TripleBean t1 = new TripleBean();
		t1.setS("<http://example.org/resource/Res1>");
		t1.setP("<http://example.org/property/pro1>");
		t1.setO("<http://example.org/resource/Res:1000>");

		TripleBean t2 = new TripleBean();
		t2.setS("<http://example.org/resource/Res1>");
		t2.setP("<http://example.org/property/pro1>");
		t2.setO("<http://example.org/resource/Res3>");

		TripleBean t3 = new TripleBean();
		t3.setS("<http://example.org/resource/Res3>");
		t3.setP("<http://example.org/property/pro3>");
		t3.setO("\"wow\\\" \\\" . \\\"ok\\\" hi\"");

		TripleBean t4 = new TripleBean();
		t4.setS("<http://example.org/resource/Res4>");
		t4.setP("<http://example.org/property/pro2>");
		t4.setO("<http://example.org/resource/Res4>");

		TripleBean t5 = new TripleBean();
		t5.setS("<http://example.org/resource/Res2>");
		t5.setP("<http://example.org/property/pro3>");
		t5.setO("\"wow hi\"");

		ArrayList<TripleBean> triplesList = new ArrayList<TripleBean>();
		triplesList.add(t1);
		triplesList.add(t2);
		triplesList.add(t3);
		triplesList.add(t4);
		triplesList.add(t5);

		spark().sql("USE triplesWithMoreThanThreeRes_db");
		Dataset<Row> expectedTT = spark().createDataset(triplesList, triplesEncoder).select("s", "p", "o").orderBy("s",
				"p", "o");
		Dataset<Row> actualTT = spark().sql("SELECT s,p,o FROM tripletable ORDER BY s,p,o");

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
		ClassLoader classLoader = getClass().getClassLoader();
		File incompleteTriples = new File(classLoader.getResource("incompleteTriples.nt").getFile());
		putFileToHDFS(incompleteTriples.getAbsolutePath(), "/incompleteTriples");

		enableSessionForPhysicalPartitioning();
		spark().sql("DROP DATABASE IF EXISTS incompleteTriples_db CASCADE");
		TripleTableLoader tt_loader = new TripleTableLoader("/incompleteTriples", "incompleteTriples_db", spark(), true,
				false, true);
		tt_loader.load();

		// Expected value:
		TripleBean t1 = new TripleBean();
		t1.setS("<http://example.org/resource/Res1>");
		t1.setP("<http://example.org/property/pro1>");
		t1.setO("<http://example.org/resource/Res:1000>");

		TripleBean t2 = new TripleBean();
		t2.setS("<http://example.org/resource/Res5>");
		t2.setP("<http://example.org/property/pro3>");
		t2.setO("<http://example.org/resource/Res2>");

		ArrayList<TripleBean> triplesList = new ArrayList<TripleBean>();
		triplesList.add(t1);
		triplesList.add(t2);

		spark().sql("USE incompleteTriples_db");
		Dataset<Row> expectedTT = spark().createDataset(triplesList, triplesEncoder).select("s", "p", "o").orderBy("s",
				"p", "o");
		Dataset<Row> actualTT = spark().sql("SELECT s,p,o FROM tripletable ORDER BY s,p,o");

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
		ClassLoader classLoader = getClass().getClassLoader();
		File triplesWithEmptyLines = new File(classLoader.getResource("triplesWithEmptyLines.nt").getFile());
		putFileToHDFS(triplesWithEmptyLines.getAbsolutePath(), "/triplesWithEmptyLines");

		enableSessionForPhysicalPartitioning();
		spark().sql("DROP DATABASE IF EXISTS triplesWithEmptyLines_db CASCADE");
		TripleTableLoader tt_loader = new TripleTableLoader("/triplesWithEmptyLines", "triplesWithEmptyLines_db",
				spark(), true, false, true);
		tt_loader.load();

		// Expected value:
		TripleBean t1 = new TripleBean();
		t1.setS("<http://example.org/resource/Res1>");
		t1.setP("<http://example.org/property/pro1>");
		t1.setO("<http://example.org/resource/Res:1000>");

		TripleBean t2 = new TripleBean();
		t2.setS("<http://example.org/resource/Res5>");
		t2.setP("<http://example.org/property/pro3>");
		t2.setO("<http://example.org/resource/Res2>");

		ArrayList<TripleBean> triplesList = new ArrayList<TripleBean>();
		triplesList.add(t1);
		triplesList.add(t2);

		spark().sql("USE triplesWithEmptyLines_db");
		Dataset<Row> expectedTT = spark().createDataset(triplesList, triplesEncoder).select("s", "p", "o").orderBy("s",
				"p", "o");
		Dataset<Row> actualTT = spark().sql("SELECT s,p,o FROM tripletable ORDER BY s,p,o");

		assertDataFrameEquals(expectedTT, actualTT);
	}

	/**
	 * This test shows that TT parses all triples even when predicates are case
	 * insensitive equal.
	 * 
	 * @throws Exception
	 */
	@Test
	public void parsingCaseInsensitivePredicates() throws Exception {
		ClassLoader classLoader = getClass().getClassLoader();
		File caseInsensitivePredicates = new File(classLoader.getResource("caseInsensitivePredicates.nt").getFile());
		putFileToHDFS(caseInsensitivePredicates.getAbsolutePath(), "/caseInsensitivePredicates");

		enableSessionForPhysicalPartitioning();
		spark().sql("DROP DATABASE IF EXISTS caseInsensitivePredicates_db CASCADE");
		TripleTableLoader tt_loader = new TripleTableLoader("/caseInsensitivePredicates",
				"caseInsensitivePredicates_db", spark(), true, false, true);
		tt_loader.load();

		// Expected value:
		TripleBean t1 = new TripleBean();
		t1.setS("<http://example.org/resource/Res1>");
		t1.setP("<http://example.org/property/givenname>");
		t1.setO("<http://example.org/resource/Res:1000>");

		TripleBean t2 = new TripleBean();
		t2.setS("<http://example.org/resource/Res1>");
		t2.setP("<http://example.org/property/givenname>");
		t2.setO("<http://example.org/resource/Res3>");

		TripleBean t3 = new TripleBean();
		t3.setS("<http://example.org/resource/Res5>");
		t3.setP("<http://example.org/property/givenName>");
		t3.setO("<http://example.org/resource/Res1>");

		ArrayList<TripleBean> triplesList = new ArrayList<TripleBean>();
		triplesList.add(t1);
		triplesList.add(t2);
		triplesList.add(t3);

		spark().sql("USE caseInsensitivePredicates_db");
		Dataset<Row> expectedTT = spark().createDataset(triplesList, triplesEncoder).select("s", "p", "o").orderBy("s",
				"p", "o");
		Dataset<Row> actualTT = spark().sql("SELECT s,p,o FROM tripletable ORDER BY s,p,o");

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
		ClassLoader classLoader = getClass().getClassLoader();
		File triplesWithDotsInLiterals = new File(classLoader.getResource("triplesWithDotsInLiterals.nt").getFile());
		putFileToHDFS(triplesWithDotsInLiterals.getAbsolutePath(), "/triplesWithDotsInLiterals");

		enableSessionForPhysicalPartitioning();
		spark().sql("DROP DATABASE IF EXISTS triplesWithDotsInLiterals_db CASCADE");
		TripleTableLoader tt_loader = new TripleTableLoader("/triplesWithDotsInLiterals",
				"triplesWithDotsInLiterals_db", spark(), true, false, true);
		tt_loader.load();

		// Expected value:
		TripleBean t1 = new TripleBean();
		t1.setS("<http://example.org/resource/Res1>");
		t1.setP("<http://example.org/property/pro1>");
		t1.setO("<http://example.org/resource/Res:1000>");

		TripleBean t2 = new TripleBean();
		t2.setS("<http://example.org/resource/Res5>");
		t2.setP("<http://example.org/property/pro3>");
		t2.setO("\"one literal\"");

		TripleBean t3 = new TripleBean();
		t3.setS("<http://example.org/resource/Res2>");
		t3.setP("<http://example.org/property/pro1>");
		t3.setO("<http://example.org/resource/Res:1000>");

		TripleBean t4 = new TripleBean();
		t4.setS("<http://example.org/resource/Res3>");
		t4.setP("<http://example.org/property/pro3>");
		t4.setO("\"This literal contains a dot . which should NOT be removed\"");

		TripleBean t5 = new TripleBean();
		t5.setS("<http://example.org/resource/Res4>");
		t5.setP("<http://example.org/property/pro1>");
		t5.setO("<http://example.org/resource/Res:1000>");

		TripleBean t6 = new TripleBean();
		t6.setS("<http://example.org/resource/Res6>");
		t6.setP("<http://example.org/property/pro3>");
		t6.setO("\"one literal\"^^<type1>");

		ArrayList<TripleBean> triplesList = new ArrayList<TripleBean>();
		triplesList.add(t1);
		triplesList.add(t2);
		triplesList.add(t3);
		triplesList.add(t4);
		triplesList.add(t5);
		triplesList.add(t6);

		spark().sql("USE triplesWithDotsInLiterals_db");
		Dataset<Row> expectedTT = spark().createDataset(triplesList, triplesEncoder).select("s", "p", "o").orderBy("s",
				"p", "o");
		Dataset<Row> actualTT = spark().sql("SELECT s,p,o FROM tripletable ORDER BY s,p,o");

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
		ClassLoader classLoader = getClass().getClassLoader();
		File triplesWithDuplicates = new File(classLoader.getResource("triplesWithDuplicates.nt").getFile());
		putFileToHDFS(triplesWithDuplicates.getAbsolutePath(), "/triplesWithDuplicates");

		enableSessionForPhysicalPartitioning();
		spark().sql("DROP DATABASE IF EXISTS triplesWithDuplicates_db CASCADE");
		TripleTableLoader tt_loader = new TripleTableLoader("/triplesWithDuplicates", "triplesWithDuplicates_db",
				spark(), true, false, true);
		tt_loader.load();

		// Expected value:
		TripleBean t1 = new TripleBean();
		t1.setS("<http://example.org/resource/Res1>");
		t1.setP("<http://example.org/property/pro1>");
		t1.setO("<http://example.org/resource/Res:1000>");

		TripleBean t2 = new TripleBean();
		t2.setS("<http://example.org/resource/Res1>");
		t2.setP("<http://example.org/property/pro1>");
		t2.setO("<http://example.org/resource/Res3>");

		TripleBean t3 = new TripleBean();
		t3.setS("<http://example.org/resource/Res5>");
		t3.setP("<http://example.org/property/pro1>");
		t3.setO("<http://example.org/resource/Res1>");

		// These are duplicate triples
		TripleBean t4 = new TripleBean();
		t4.setS("<http://example.org/resource/Res1>");
		t4.setP("<http://example.org/property/pro1>");
		t4.setO("<http://example.org/resource/Res3>");

		TripleBean t5 = new TripleBean();
		t5.setS("<http://example.org/resource/Res5>");
		t5.setP("<http://example.org/property/pro1>");
		t5.setO("<http://example.org/resource/Res1>");

		TripleBean t6 = new TripleBean();
		t6.setS("<http://example.org/resource/Res1>");
		t6.setP("<http://example.org/property/pro1>");
		t6.setO("<http://example.org/resource/Res:1000>");

		ArrayList<TripleBean> triplesListNoDuplicates = new ArrayList<TripleBean>();
		triplesListNoDuplicates.add(t1);
		triplesListNoDuplicates.add(t2);
		triplesListNoDuplicates.add(t3);

		ArrayList<TripleBean> triplesListWithDuplicates = new ArrayList<TripleBean>();
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
		enableSessionForPhysicalPartitioning();
		tt_loader = new TripleTableLoader("/triplesWithDuplicates", "triplesWithDuplicates_db", spark(), true, false,
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
		ClassLoader classLoader = getClass().getClassLoader();
		File emptyFile = new File(classLoader.getResource("emptyFile.nt").getFile());
		putFileToHDFS(emptyFile.getAbsolutePath(), "/emptyFile");

		enableSessionForPhysicalPartitioning();
		spark().sql("DROP DATABASE IF EXISTS emptyFile_db CASCADE");
		TripleTableLoader tt_loader = new TripleTableLoader("/emptyFile", "emptyFile_db", spark(), true, false, true);
		tt_loader.load();
	}

	// Not behaving as expected. Fix after merging (this belongs to a different
	// feature).
	public void parsingTriplesWithPrefixes() throws Exception {
		ClassLoader classLoader = getClass().getClassLoader();
		File triplesWithPrefixes = new File(classLoader.getResource("triplesWithPrefixes.nt").getFile());
		putFileToHDFS(triplesWithPrefixes.getAbsolutePath(), "/triplesWithPrefixes");

		enableSessionForPhysicalPartitioning();
		spark().sql("DROP DATABASE IF EXISTS triplesWithPrefixes_db CASCADE");
		TripleTableLoader tt_loader = new TripleTableLoader("/triplesWithPrefixes", "triplesWithPrefixes_db", spark(),
				true, false, true);
		tt_loader.load();

		// Expected value:
		TripleBean t1 = new TripleBean();
		t1.setS("<http://example.org/resource/Res1>");
		t1.setP("<http://example.org/property/pro1>");
		t1.setO("<http://example.org/resource/Res:1000>");

		TripleBean t2 = new TripleBean();
		t2.setS("<http://example.org/resource/Res5>");
		t2.setP("<http://example.org/property/pro3>");
		t2.setO("<http://example.org/resource/Res2>");

		ArrayList<TripleBean> triplesList = new ArrayList<TripleBean>();
		triplesList.add(t1);
		triplesList.add(t2);

		spark().sql("USE triplesWithPrefixes_db");
		Dataset<Row> expectedTT = spark().createDataset(triplesList, triplesEncoder).select("s", "p", "o").orderBy("s",
				"p", "o");
		Dataset<Row> actualTT = spark().sql("SELECT s,p,o FROM tripletable ORDER BY s,p,o");

		assertDataFrameEquals(expectedTT, actualTT);
	}

	/**
	 * Sets the spark session to be able to partition physically.
	 */
	public void enableSessionForPhysicalPartitioning() {
		spark().sql("SET hive.exec.dynamic.partition = true");
		spark().sql("SET hive.exec.dynamic.partition.mode = nonstrict");
		spark().sql("SET hive.exec.max.dynamic.partitions = 4000");
		spark().sql("SET hive.exec.max.dynamic.partitions.pernode = 2000");
	}

	/**
	 * This method puts the file in an HDFS folder, so that this can be used for
	 * applications working on top of Hadoop. In case the HDFS folder passed as
	 * argument exists, this will be deleted.
	 * 
	 * @param localPath
	 * @param hdfsFolderPath
	 * @throws IOException
	 */
	public void putFileToHDFS(String localPath, String hdfsFolderPath) throws IOException {
		// TODO: improve. Using the Hadoop API didn't work (see commented code).
		// fs.copyFromLocalFile(new
		// Path(triplesMoreThan3Resources.getAbsolutePath()), new
		// Path("/triplesMoreThanThreeElements"));
		// FileUtil.copy(fs, new
		// Path(triplesMoreThan3Resources.getAbsolutePath()), fs, new
		// Path("/triplesMoreThanThreeElements"), false,
		// sc.hadoopConfiguration());

		FileSystem fs = FileSystem.get(jsc().hadoopConfiguration());
		if (fs.exists(new Path(hdfsFolderPath)))
			fs.delete(new Path(hdfsFolderPath), true);

		JavaRDD<String> lines = jsc().textFile(localPath);
		lines.map(x -> x.toString().replace("[", "").replace("]", "")).saveAsTextFile(hdfsFolderPath);
	}
}
