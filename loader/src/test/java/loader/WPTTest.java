package loader;

import java.io.Serializable;
import java.util.ArrayList;

import com.holdenkarau.spark.testing.JavaDataFrameSuiteBase;
import loader.utilities.SparkSqlUtilities;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.junit.Test;
import stats.DatabaseStatistics;

public class WPTTest extends JavaDataFrameSuiteBase {
	private static class wptBean implements Serializable {
		private static final long serialVersionUID = 39L;
		private String s;
		private String pro1;
		private String pro2;
		private String pro3;

		public String getS() {
			return s;
		}

		public void setS(final String s) {
			this.s = s;
		}

		public String getPro1() {
			return pro1;
		}

		public void setPro1(final String value) {
			this.pro1 = value;
		}

		public String getPro2() {
			return pro2;
		}

		public void setPro2(final String value) {
			this.pro2 = value;
		}

		public String getPro3() {
			return pro3;
		}

		public void setPro3(final String value) {
			this.pro3 = value;
		}
	}

	private static final Encoder<TripleBean> triplesEncoder = Encoders.bean(TripleBean.class);
	@Test
	public void testWPT() {
		SparkSqlUtilities.enableSessionForPhysicalPartitioning(spark());
		spark().sql("DROP DATABASE IF EXISTS wptTest_db CASCADE");
		spark().sql("CREATE DATABASE IF NOT EXISTS  wptTest_db");
		spark().sql("USE wptTest_db");

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
				"s", "p", "o");
		ttDataset.write().saveAsTable("tripletable");

		final Settings settings = new Settings.Builder("wptTest_db").withInputPath((System.getProperty(
				"user.dir") + "\\target\\test_output\\wptTest").replace('\\', '/')).droppingDuplicateTriples().build();

		final DatabaseStatistics statistics = new DatabaseStatistics("wptTest_db");

		final WidePropertyTableLoader wptLoader =
				new WidePropertyTableLoader(settings, spark(), statistics);

		wptLoader.load();

	}
}
