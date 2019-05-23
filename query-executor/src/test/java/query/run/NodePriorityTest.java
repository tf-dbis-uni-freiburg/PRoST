package query.run;

import java.io.FileNotFoundException;
import java.util.List;

import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.shared.PrefixMapping;
import joinTree.WPTNode;
import org.junit.Assert;
import org.junit.Test;
import query.utilities.TestData;
import stats.DatabaseStatistics;

public class NodePriorityTest {
	/**
	 * tests the use of characteristic sets to calculate node priorities.
	 */
	@Test
	public void computeNodesPriorities() throws FileNotFoundException {
		final ClassLoader classLoader = getClass().getClassLoader();

		final DatabaseStatistics statistics = DatabaseStatistics.loadFromFile(classLoader.getResource("nodePriority.json").getPath());

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

		final WPTNode wptNode_set_p0_p1 = new WPTNode(triples_set_p0_p1, prefixes, statistics);
		Assert.assertEquals(2, wptNode_set_p0_p1.getPriority(), 0);

		final WPTNode wptNode_set_superset_p0 = new WPTNode(triples_set_superset_p0, prefixes, statistics);
		Assert.assertEquals(3, wptNode_set_superset_p0.getPriority(), 0);

		final WPTNode wptNode_set_p2 = new WPTNode(triples_set_p2, prefixes, statistics);
		Assert.assertEquals(1, wptNode_set_p2.getPriority(), 0);

		final WPTNode wptNode_noSet = new WPTNode(triples_noSet, prefixes, statistics);
		Assert.assertEquals(0, wptNode_noSet.getPriority(), 0);

		final WPTNode wptNode_superset_p1 = new WPTNode(triples_superset_p1, prefixes, statistics);
		Assert.assertEquals(2, wptNode_superset_p1.getPriority(), 0);

		//TODO tests with bounded objects
	}
}
