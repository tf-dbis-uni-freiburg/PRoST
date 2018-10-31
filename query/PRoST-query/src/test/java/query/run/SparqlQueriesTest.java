package query.run;

import java.io.File;
import java.io.Serializable;
import java.util.List;

import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.junit.Test;
import com.holdenkarau.spark.testing.JavaDataFrameSuiteBase;

import executor.Executor;
import joinTree.JoinTree;
import query.utilities.HiveDatabaseUtilities;
import query.utilities.TestData;
import query.utilities.TripleBean;
import scala.tools.nsc.Main;
import translator.Stats;
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
	private static final long serialVersionUID = 1329L;
	protected static final Logger logger = Logger.getLogger("PRoST");
	private static final Encoder<TripleBean> triplesEncoder = Encoders.bean(TripleBean.class);

	/**
	 * This method tests if triples with more than three elements are ignored
	 * when parsing the file.
	 * 
	 * @throws Exception
	 */
	@Test
	public void queryOnSingleTriple() throws Exception {
		List<TripleBean> triplesList = TestData.createSingleTripleTestData();
		Dataset<TripleBean> data = spark().createDataset(triplesList, triplesEncoder);
		HiveDatabaseUtilities.writeTriplesToDatabase("singleTripleDb", data, spark());
		
		ClassLoader classLoader = getClass().getClassLoader();
		File singleTripleQuery1 = new File(classLoader.getResource("singleTripleQuery1.q").getFile());
		File statsSingleTripleQuery1 = new File(classLoader.getResource("singletripledb.stats").getFile());

		Stats.getInstance().parseStats(statsSingleTripleQuery1.getAbsolutePath());
		Translator translator = new Translator(singleTripleQuery1.getAbsolutePath(), -1);
		translator.setUsePropertyTable(true);
		translator.setMinimumGroupSize(-1);
		JoinTree jt = translator.translateQuery();
		Executor executor = new Executor(jt, "singleTripleDb");
		executor.setOutputFile("F:\\works.txt");
		executor.execute();
	}
}
