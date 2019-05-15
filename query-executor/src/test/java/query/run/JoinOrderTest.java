package query.run;

import java.util.List;

import com.holdenkarau.spark.testing.JavaDataFrameSuiteBase;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.junit.Test;
import query.utilities.HiveDatabaseUtilities;
import query.utilities.TestData;
import query.utilities.TripleBean;

public class JoinOrderTest extends JavaDataFrameSuiteBase {
	private static final Encoder<TripleBean> triplesEncoder = Encoders.bean(TripleBean.class);

	@Test
	public void queryWithJoins() {
		List<TripleBean> triplesList = TestData.createMultipleSequentialTestData(10);
		Dataset<TripleBean> data = spark().createDataset(triplesList, triplesEncoder);
		HiveDatabaseUtilities.writeTriplesToDatabase("testDB", data, spark(), "tiny");

		triplesList = TestData.createMultipleSequentialTestData(100);
		data = spark().createDataset(triplesList, triplesEncoder);
		HiveDatabaseUtilities.writeTriplesToDatabase("testDB", data, spark(), "mid");

		triplesList = TestData.createMultipleSequentialTestData(1000);
		data = spark().createDataset(triplesList, triplesEncoder);
		HiveDatabaseUtilities.writeTriplesToDatabase("testDB", data, spark(), "big");

		String queryBig = "SELECT s AS var FROM big";
		String queryMid = "SELECT s AS var FROM mid";
		String queryTiny = "SELECT s AS var FROM tiny";

		spark().sql("ANALYZE TABLE big COMPUTE STATISTICS FOR COLUMNS s");
		spark().sql("ANALYZE TABLE mid COMPUTE STATISTICS FOR COLUMNS s");
		spark().sql("ANALYZE TABLE tiny COMPUTE STATISTICS FOR COLUMNS s");

		Dataset<Row> bigData = spark().sql(queryBig);
		Dataset<Row> midData = spark().sql(queryMid);
		Dataset<Row> tinyData = spark().sql(queryTiny);

		Dataset<Row> resultBigToSmall = bigData.join(midData).join(tinyData);
		Dataset<Row> resultSmallToBig = tinyData.join(midData).join(bigData);

		//List<Row> computedResult =  result.collectAsList();

		spark().sql("describe table tiny");
	}
}
