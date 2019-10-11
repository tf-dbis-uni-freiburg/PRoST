package loader;

import java.io.File;
import java.io.Serializable;
import java.util.ArrayList;

import com.holdenkarau.spark.testing.JavaDataFrameSuiteBase;
import loader.utilities.HdfsUtilities;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;
import statistics.CharacteristicSetStatistics;
import statistics.DatabaseStatistics;

/**
 * This class contains tests validating the computed statistic files given an input NT triples file.
 */
public class statisticsTest extends JavaDataFrameSuiteBase implements Serializable {
	private static final long serialVersionUID = -5681683598336701496L;

	/**
	 * Tests if the computed annotations for characteristic sets is correct
	 *
	 * @throws Exception i/o related exceptions
	 */
	@Test
	@Ignore("Lacks expected extended characteristic set")
	//TODO implement tests for extended characteristic sets
	public void characteristicSetsTest() throws Exception {
		final ClassLoader classLoader = getClass().getClassLoader();
		final File triplesWithMoreThanThreeRes = new File(
				classLoader.getResource("charset.nt").getFile());
		HdfsUtilities.putFileToHDFS(triplesWithMoreThanThreeRes.getAbsolutePath(),
				System.getProperty("user.dir").replace('\\','/') +
				"/target/test_output/charset", jsc());

		spark().sql("DROP DATABASE IF EXISTS charset_db CASCADE");

		final DatabaseStatistics statistics = new DatabaseStatistics("charset_db");
		final Settings settings = new Settings.Builder("charset_db").
				withInputPath((System.getProperty("user.dir").replace('\\','/') +
				"/target/test_output/charset")).droppingDuplicateTriples().computeCharacteristicSets().
				computePropertyStatistics().generateVp().generateWpt().build();

		final TripleTableLoader tt_loader = new TripleTableLoader(settings, spark(), statistics);
		tt_loader.load();
		final VerticalPartitioningLoader vpLoader = new VerticalPartitioningLoader(settings, spark(), statistics);
		vpLoader.load();
		statistics.computePropertyStatistics(spark());
		final WidePropertyTableLoader wptLoader = new WidePropertyTableLoader(settings,spark(),statistics);
		wptLoader.load();

		//statistics.computeCharacteristicSetsStatistics(spark());
		//statistics.computeCharacteristicSetsStatisticsFromTT(spark());

		statistics.computeCharacteristicSetsStatistics(spark());

		/*statistics.saveToFile(System.getProperty("user.dir") + "\\target\\test_output\\charset"
				+ ".json");*/

		final ArrayList<CharacteristicSetStatistics> characteristicSets = statistics.getCharacteristicSets();
		Assert.assertEquals(3, characteristicSets.size());


		final CharacteristicSetStatistics expectedCharacteristicSet1 = new CharacteristicSetStatistics();
		expectedCharacteristicSet1.setDistinctResources(Long.valueOf("1"));
		expectedCharacteristicSet1.addProperty("ex:hasAuthor", Long.parseLong("1"));
		expectedCharacteristicSet1.addProperty("ex:hasPublisher", Long.parseLong("1"));
		expectedCharacteristicSet1.addProperty("ex:hasTitle", Long.parseLong("1"));
		expectedCharacteristicSet1.addProperty("ex:hasYear", Long.parseLong("1"));

		final CharacteristicSetStatistics expectedCharacteristicSet2 = new CharacteristicSetStatistics();
		expectedCharacteristicSet2.setDistinctResources(Long.valueOf("2"));
		expectedCharacteristicSet2.addProperty("ex:hasAuthor", Long.parseLong("3"));
		expectedCharacteristicSet2.addProperty("ex:hasTitle", Long.parseLong("3"));
		expectedCharacteristicSet2.addProperty("ex:hasYear", Long.parseLong("2"));

		final CharacteristicSetStatistics expectedCharacteristicSet3 = new CharacteristicSetStatistics();
		expectedCharacteristicSet3.setDistinctResources(Long.valueOf("4"));
		expectedCharacteristicSet3.addProperty("ex:hasAuthor", Long.parseLong("4"));

		Assert.assertTrue(characteristicSets.contains(expectedCharacteristicSet1));
		Assert.assertTrue(characteristicSets.contains(expectedCharacteristicSet2));
		Assert.assertTrue(characteristicSets.contains(expectedCharacteristicSet3));
	}
}
