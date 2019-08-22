package query.utilities;

import java.util.ArrayList;
import java.util.List;

/**
 * This class generates test data which is shared and used by test cases.
 *
 * @author Victor Anthony Arrascue Ayala
 */
public class TestData {
	/**
	 * This method creates test data based on the w3c specification:
	 * https://www.w3.org/TR/rdf-sparql-query/#docDataDesc.
	 */
	public static List<TripleBean> createSingleTripleTestData() {
		final List<TripleBean> testDataTriples = new ArrayList<>();
		final TripleBean singleTriple = new TripleBean();
		singleTriple.setS("<http://example.org/book/book1>");
		singleTriple.setP("<http://purl.org/dc/elements/1.1/title>");
		singleTriple.setO("\"SPARQL Tutorial\"");
		testDataTriples.add(singleTriple);
		return testDataTriples;
	}

	public static List<TripleBean> createMultipleSequentialTestData(final int n) {
		final List<TripleBean> testDataTriples = new ArrayList<>();
		for (int id = 0; id < n; id++) {
			final TripleBean singleTriple = new TripleBean();
			singleTriple.setS("<" + id + ">");
			singleTriple.setP("<property>");
			singleTriple.setO("\"data\"");
			testDataTriples.add(singleTriple);
		}
		return testDataTriples;
	}
}
