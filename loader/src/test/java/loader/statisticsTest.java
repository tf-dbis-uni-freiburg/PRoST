package loader;

import java.io.File;
import java.io.Serializable;
import java.util.ArrayList;

import com.holdenkarau.spark.testing.JavaDataFrameSuiteBase;
import loader.utilities.HdfsUtilities;
import org.junit.Assert;
import org.junit.Test;
import stats.CharacteristicSetStatistics;
import stats.DatabaseStatistics;

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
	public void characteristicSetsTest() throws Exception {
		final ClassLoader classLoader = getClass().getClassLoader();
		final File triplesWithMoreThanThreeRes = new File(
				classLoader.getResource("charset.nt").getFile());
		HdfsUtilities.putFileToHDFS(triplesWithMoreThanThreeRes.getAbsolutePath(), System.getProperty("user.dir") +
				"\\target\\test_output\\charset", jsc());

		spark().sql("DROP DATABASE IF EXISTS charset_db CASCADE");

		final DatabaseStatistics statistics = new DatabaseStatistics("charset_db");
		final Settings settings = new Settings.Builder("charset_db").withInputPath((System.getProperty("user.dir") +
				"\\target\\test_output\\charset").replace('\\', '/')).droppingDuplicateTriples().computeCharacteristicSets().build();

		final TripleTableLoader tt_loader = new TripleTableLoader(settings, spark(), statistics);
		tt_loader.load();

		/*statistics.saveToFile(System.getProperty("user.dir") + "\\target\\test_output\\charset"
				+ ".json");*/

		final ArrayList<CharacteristicSetStatistics> characteristicSets = statistics.getCharacteristicSets();
		Assert.assertEquals(3, characteristicSets.size());


		final CharacteristicSetStatistics expectedCharacteristicSet1 = new CharacteristicSetStatistics();
		expectedCharacteristicSet1.setDistinctSubjects(Long.valueOf("1"));
		expectedCharacteristicSet1.getTuplesPerPredicate().put("ex:hasAuthor", Long.valueOf("1"));
		expectedCharacteristicSet1.getTuplesPerPredicate().put("ex:hasPublisher", Long.valueOf("1"));
		expectedCharacteristicSet1.getTuplesPerPredicate().put("ex:hasTitle", Long.valueOf("1"));
		expectedCharacteristicSet1.getTuplesPerPredicate().put("ex:hasYear", Long.valueOf("1"));

		final CharacteristicSetStatistics expectedCharacteristicSet2 = new CharacteristicSetStatistics();
		expectedCharacteristicSet2.setDistinctSubjects(Long.valueOf("2"));
		expectedCharacteristicSet2.getTuplesPerPredicate().put("ex:hasAuthor", Long.valueOf("3"));
		expectedCharacteristicSet2.getTuplesPerPredicate().put("ex:hasTitle", Long.valueOf("3"));
		expectedCharacteristicSet2.getTuplesPerPredicate().put("ex:hasYear", Long.valueOf("2"));

		final CharacteristicSetStatistics expectedCharacteristicSet3 = new CharacteristicSetStatistics();
		expectedCharacteristicSet3.setDistinctSubjects(Long.valueOf("4"));
		expectedCharacteristicSet3.getTuplesPerPredicate().put("ex:hasAuthor", Long.valueOf("4"));

		Assert.assertTrue(characteristicSets.contains(expectedCharacteristicSet1));
		Assert.assertTrue(characteristicSets.contains(expectedCharacteristicSet2));
		Assert.assertTrue(characteristicSets.contains(expectedCharacteristicSet3));
	}

}
