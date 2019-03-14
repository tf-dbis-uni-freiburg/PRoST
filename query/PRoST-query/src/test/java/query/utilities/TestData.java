package query.utilities;

import java.util.ArrayList;
import java.util.List;

/**
 * This class generates test data which is shared and used by test cases.
 * 
 * @author Victor Anthony Arrascue Ayala
 *
 */
public class TestData {
	/**
	 * This method creates test data based on the w3c specification:
	 * https://www.w3.org/TR/rdf-sparql-query/#docDataDesc.
	 * 
	 * @return
	 */
	public static List<TripleBean> createSingleTripleTestData() {
		List<TripleBean> testDataTriples = new ArrayList<TripleBean>();
		TripleBean singleTriple = new TripleBean();
		singleTriple.setS("<http://example.org/book/book1>");
		singleTriple.setP("<http://purl.org/dc/elements/1.1/title>");
		singleTriple.setO("\"SPARQL Tutorial\"");
		 testDataTriples.add(singleTriple);
		 return testDataTriples;
	}
}
