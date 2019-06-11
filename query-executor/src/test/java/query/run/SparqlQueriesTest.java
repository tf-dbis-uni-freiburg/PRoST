package query.run;

import java.io.File;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import com.holdenkarau.spark.testing.JavaDataFrameSuiteBase;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.junit.Test;
import query.utilities.HiveDatabaseUtilities;
import query.utilities.TestData;
import query.utilities.TripleBean;
import stats.DatabaseStatistics;
import utils.Settings;

/**
 * This class tests represents the highest level of testing, i.e. given a query
 * it checks that results are correctly and consistently returned according to
 * ALL supported logical partitioning strategies (at the moment WPT, IWPT, JWPT,
 * and VP?), i.e. these tests verify are about SPARQL semantics.
 *
 * @author Victor Anthony Arrascue Ayala
 */
public class SparqlQueriesTest extends JavaDataFrameSuiteBase implements Serializable {
	protected static final Logger logger = Logger.getLogger("PRoST");
	private static final long serialVersionUID = 1329L;
	private static final Encoder<TripleBean> triplesEncoder = Encoders.bean(TripleBean.class);

	/**
	 * This method tests if triples with more than three elements are ignored
	 * when parsing the file.
	 *
	 */
	public void queryOnSingleTriple() {
		final List<TripleBean> triplesList = TestData.createSingleTripleTestData();
		final Dataset<TripleBean> data = spark().createDataset(triplesList, triplesEncoder);
		HiveDatabaseUtilities.writeTriplesToDatabase("singleTripleDb", data, spark());

		final ClassLoader classLoader = getClass().getClassLoader();
		final File singleTripleQuery1 = new File(classLoader.getResource("singleTripleQuery1.q").getFile());

		// TODO need new .json statistics file
		//final File statsSingleTripleQuery1 = new File(classLoader.getResource("singletripledb.stats").getFile());


		//TODO need new json statistics
		/*Stats.getInstance().parseStats(statsSingleTripleQuery1.getAbsolutePath());
		final Translator translator = new Translator(singleTripleQuery1.getAbsolutePath(), -1);
		translator.setUseTripleTablePartitioning(true);
		translator.setMinimumGroupSize(-1);
		final JoinTree jt = translator.translateQuery();
		final Executor executor = new Executor("singleTripleDb");
		executor.setOutputFile(System.getProperty("user.dir") + "\\target\\test_output\\result");
		executor.execute(jt);*/
	}

	@Test
	public void queryOnTT() {
		spark().sql("DROP DATABASE IF EXISTS ttQueryTest_db CASCADE");
		spark().sql("CREATE DATABASE IF NOT EXISTS  ttQueryTest_db");
		spark().sql("USE ttQueryTest_db");

		// creates test tt table
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

		final ArrayList<TripleBean> triplesList = new ArrayList<>();
		triplesList.add(t1);
		triplesList.add(t2);
		triplesList.add(t3);
		triplesList.add(t4);
		triplesList.add(t5);

		final Dataset<Row> ttDataset = spark().createDataset(triplesList, triplesEncoder).select("s", "p", "o").orderBy(
				"s", "p","o");
		ttDataset.write().saveAsTable("tripletable");


		final Settings settings = new Settings.Builder("ttQueryTest_db").usingTTNodes().build();
		final DatabaseStatistics statistics = new DatabaseStatistics("ttQueryTest_db");

		/*final Triple triple = new Triple();
		final TriplePattern pattern = new TriplePattern();
		final TTNode ttNode = new TTNode();*/

	}
}
