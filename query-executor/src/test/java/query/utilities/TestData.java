package query.utilities;

import java.util.ArrayList;
import java.util.List;

import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.query.Query;
import com.hp.hpl.jena.query.QueryFactory;
import com.hp.hpl.jena.shared.PrefixMapping;
import com.hp.hpl.jena.sparql.algebra.Algebra;
import com.hp.hpl.jena.sparql.algebra.Op;
import com.hp.hpl.jena.sparql.algebra.OpWalker;
import translator.QueryTree;
import translator.QueryVisitor;

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


	public static List<Triple> loadTriplesFromQueryFile(final String path) {
		final Query query = QueryFactory.read("file:" + path);
		final PrefixMapping prefixes = query.getPrefixMapping();

		final Op opQuery = Algebra.compile(query);
		final QueryVisitor queryVisitor = new QueryVisitor(prefixes);
		OpWalker.walk(opQuery, queryVisitor);
		final QueryTree mainTree = queryVisitor.getMainQueryTree();
		return mainTree.getTriples();
	}

}
