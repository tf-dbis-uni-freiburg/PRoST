package query.run;

import java.io.File;
import java.io.Serializable;
import java.util.List;

import com.holdenkarau.spark.testing.JavaDataFrameSuiteBase;
import executor.Executor;
import joinTree.JoinTree;
import joinTree.stats.Stats;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.junit.Test;
import query.utilities.HiveDatabaseUtilities;
import query.utilities.TestData;
import query.utilities.TripleBean;
import translator.Translator;

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
	@Test
	public void queryOnSingleTriple() {
		final List<TripleBean> triplesList = TestData.createSingleTripleTestData();
		final Dataset<TripleBean> data = spark().createDataset(triplesList, triplesEncoder);
		HiveDatabaseUtilities.writeTriplesToDatabase("singleTripleDb", data, spark());

		final ClassLoader classLoader = getClass().getClassLoader();
		final File singleTripleQuery1 = new File(classLoader.getResource("singleTripleQuery1.q").getFile());
		final File statsSingleTripleQuery1 = new File(classLoader.getResource("singletripledb.stats").getFile());

		Stats.getInstance().parseStats(statsSingleTripleQuery1.getAbsolutePath());
		final Translator translator = new Translator(singleTripleQuery1.getAbsolutePath(), -1);
		translator.setUseTripleTablePartitioning(true);
		translator.setMinimumGroupSize(-1);
		final JoinTree jt = translator.translateQuery();
		final Executor executor = new Executor("singleTripleDb");
		executor.setOutputFile(System.getProperty("user.dir") + "\\target\\test_output\\works.txt");
		executor.execute(jt);
	}
}
