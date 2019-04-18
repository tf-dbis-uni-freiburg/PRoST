package query.run;

import java.util.List;

import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.shared.PrefixMapping;
import joinTree.PTNode;
import org.junit.Assert;
import org.junit.Test;
import query.utilities.TestData;
import stats.DatabaseStatistics;

public class NodePriorityTest {
	/**
	 * tests the use of characteristic sets to calculate node priorities.
	 */
	@Test
	public void computeNodesPriorities() {
		final ClassLoader classLoader = getClass().getClassLoader();

		final DatabaseStatistics statistics = new DatabaseStatistics();
		statistics.loadFromFile(classLoader.getResource("nodePriority.json").getPath());


		final PrefixMapping prefixes = PrefixMapping.Factory.create();
		final List<Triple> triples_set_p0_p1 =
				TestData.loadTriplesFromQueryFile(classLoader.getResource("nodePriority0_set_p0_p1.q").getPath());
		final List<Triple> triples_set_superset_p0 =
				TestData.loadTriplesFromQueryFile(classLoader.getResource("nodePriority1_set_superset_p0.q").getPath());
		final List<Triple> triples_set_p2 =
				TestData.loadTriplesFromQueryFile(classLoader.getResource("nodePriority2_set_p2.q").getPath());
		final List<Triple> triples_noSet =
				TestData.loadTriplesFromQueryFile(classLoader.getResource("nodePriority3_noSet.q").getPath());
		final List<Triple> triples_superset_p1 =
				TestData.loadTriplesFromQueryFile(classLoader.getResource("nodePriority4_superset_p1.q").getPath());

		final PTNode wptNode_set_p0_p1 = new PTNode(triples_set_p0_p1, prefixes, statistics);
		Assert.assertEquals(2, wptNode_set_p0_p1.getPriority(), 0);

		final PTNode wptNode_set_superset_p0 = new PTNode(triples_set_superset_p0, prefixes, statistics);
		Assert.assertEquals(3, wptNode_set_superset_p0.getPriority(), 0);

		final PTNode wptNode_set_p2 = new PTNode(triples_set_p2, prefixes, statistics);
		Assert.assertEquals(1, wptNode_set_p2.getPriority(), 0);

		final PTNode wptNode_noSet = new PTNode(triples_noSet, prefixes, statistics);
		Assert.assertEquals(0, wptNode_noSet.getPriority(), 0);

		final PTNode wptNode_superset_p1 = new PTNode(triples_superset_p1, prefixes, statistics);
		Assert.assertEquals(2, wptNode_superset_p1.getPriority(), 0);

		//TODO tests with bounded objects
	}
}
