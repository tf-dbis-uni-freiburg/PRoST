package loader;

import java.util.ArrayList;

import com.holdenkarau.spark.testing.JavaDataFrameSuiteBase;
import loader.utilities.SparkSqlUtilities;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.junit.Test;
import statistics.DatabaseStatistics;

public class VPLoaderTest extends JavaDataFrameSuiteBase {
	private static final Encoder<TripleBean> triplesEncoder = Encoders.bean(TripleBean.class);

	@Test
	public void testVP(){
		SparkSqlUtilities.enableSessionForPhysicalPartitioning(spark());
		spark().sql("DROP DATABASE IF EXISTS vpTest_db CASCADE");
		spark().sql("CREATE DATABASE IF NOT EXISTS  vpTest_db");
		spark().sql("USE vpTest_db");

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

		final ArrayList<TripleBean> vp1TripleList = new ArrayList<>();
		vp1TripleList.add(t1);
		vp1TripleList.add(t2);
		final ArrayList<TripleBean> vp2TripleList = new ArrayList<>();
		vp2TripleList.add(t4);
		final ArrayList<TripleBean> vp3TripleList = new ArrayList<>();
		vp3TripleList.add(t3);
		vp3TripleList.add(t5);

		final Dataset<Row> expectedVP1Dataset =
				spark().createDataset(vp1TripleList, triplesEncoder).select("s", "o").orderBy(
				"s","o");
		final Dataset<Row> expectedVP2Dataset =
				spark().createDataset(vp2TripleList, triplesEncoder).select("s", "o").orderBy(
						"s","o");
		final Dataset<Row> expectedVP3Dataset =
				spark().createDataset(vp3TripleList, triplesEncoder).select("s", "o").orderBy(
						"s","o");

		final Settings settings = new Settings.Builder("vpTest_db").
				withInputPath((System.getProperty("user.dir").replace('\\','/') +
						"/target/test_output/vpTest")).droppingDuplicateTriples().computePropertyStatistics().build();

		final DatabaseStatistics statistics = new DatabaseStatistics("vpTest_db");

		final VerticalPartitioningLoader vp_loader = new VerticalPartitioningLoader(settings, spark(), statistics);
		vp_loader.load();

		final Dataset<Row> actualVP1 = spark().sql("SELECT s,o FROM vp_http___example_org_property_pro1 ORDER BY s,o");
		final Dataset<Row> actualVP2 = spark().sql("SELECT s,o FROM vp_http___example_org_property_pro2 ORDER BY s,o");
		final Dataset<Row> actualVP3 = spark().sql("SELECT s,o FROM vp_http___example_org_property_pro3 ORDER BY s,o");

		assertDataFrameEquals(expectedVP1Dataset, actualVP1);
		assertDataFrameEquals(expectedVP2Dataset, actualVP2);
		assertDataFrameEquals(expectedVP3Dataset, actualVP3);
	}
}
