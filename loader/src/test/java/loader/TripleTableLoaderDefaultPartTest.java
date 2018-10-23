package loader;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.List;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.Test;
import org.junit.jupiter.api.BeforeAll;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import com.holdenkarau.spark.testing.JavaDataFrameSuiteBase;

/**
 * This class tests the parsing of the NT triples file and the building of the
 * TripleTableLoader.
 * 
 * @author Victor Anthony Arrascue Ayala
 */
public class TripleTableLoaderTest extends JavaDataFrameSuiteBase implements Serializable {
	private static final long serialVersionUID = -5681683598336701496L;
	protected static final Logger logger = Logger.getLogger("PRoST");

	/**
	 * This method tests if triples with more than three elements are ignored when parsing the file.
	 * @throws Exception
	 */
	@Test
	public void parsingTriplesMoreThanThreeElements() throws Exception {
		// private JavaSparkContext sc = jsc();
		// private SQLContext sqlc = sqlContext();
		// private SparkSession spark = spark();

		// fs.copyFromLocalFile(new
		// Path(triplesMoreThan3Resources.getAbsolutePath()), new
		// Path("/triplesMoreThanThreeElements"));
		// FileUtil.copy(fs, new
		// Path(triplesMoreThan3Resources.getAbsolutePath()), fs, new
		// Path("/triplesMoreThanThreeElements"), false,
		// sc.hadoopConfiguration());
		
		ClassLoader classLoader = getClass().getClassLoader();
		File triplesWithMoreThanThreeRes = new File(
				classLoader.getResource("triplesWithMoreThanThreeRes.nt").getFile());

		FileSystem fs = FileSystem.get(jsc().hadoopConfiguration());
		String outPutPath = "/triplesWithMoreThanThreeRes";
		if (fs.exists(new Path(outPutPath)))
			fs.delete(new Path(outPutPath), true);

		JavaRDD<String> lines = jsc().textFile(triplesWithMoreThanThreeRes.getAbsolutePath());
		lines.map(x -> x.toString().replace("[", "").replace("]", "")).saveAsTextFile("/triplesWithMoreThanThreeRes");

		TripleTableLoader tt_loader = new TripleTableLoader("/triplesWithMoreThanThreeRes", "test_db", spark(), false,
				false, true);
		tt_loader.load();

		// spark.sql("USE parsingTriplesMoreThanThreeElements");
		Dataset<Row> fullTripleTable = spark().sql("SELECT * FROM tripletable");

		List<Row> fileAsListOfRows = fullTripleTable.collectAsList();
		logger.error("ACCESSING TABLES");
		logger.error(fileAsListOfRows);
	}
}
