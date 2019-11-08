package translator.algebraTree;

import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.shared.PrefixMapping;
import com.hp.hpl.jena.sparql.algebra.op.OpBGP;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.PriorityQueue;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import statistics.DatabaseStatistics;
import translator.NodeComparator;
import translator.algebraTree.bgpTree.BgpNode;
import translator.algebraTree.bgpTree.ElementType;
import translator.algebraTree.bgpTree.IWPTNode;
import translator.algebraTree.bgpTree.JWPTNode;
import translator.algebraTree.bgpTree.JoinNode;
import translator.algebraTree.bgpTree.TTNode;
import translator.algebraTree.bgpTree.TriplePattern;
import translator.algebraTree.bgpTree.VPNode;
import translator.algebraTree.bgpTree.WPTNode;
import translator.triplesGroup.JoinedTriplesGroup;
import translator.triplesGroup.TriplesGroup;
import translator.triplesGroup.TriplesGroupsMapping;
import utils.Settings;

/**
 * An algebra tree node containing a Basic Graph Pattern.
 */
public class Bgp extends Operation {
	private static final Logger logger = Logger.getLogger("PRoST");
	private final List<Triple> triples;
	private final BgpNode bgpRootNode;

	Bgp(final OpBGP jenaAlgebraTree, final DatabaseStatistics statistics, final Settings settings,
		final PrefixMapping prefixes) {
		this.triples = jenaAlgebraTree.getPattern().getList();
		this.bgpRootNode = computeRootNode(statistics, settings, prefixes);
	}

	public Dataset<Row> computeOperation(final SQLContext sqlContext) {
		bgpRootNode.computeNodeData(sqlContext);
		return bgpRootNode.getSparkNodeData();
	}

	/**
	 * Constructs the bgp tree.
	 */
	private BgpNode computeRootNode(final DatabaseStatistics statistics, final Settings settings,
									final PrefixMapping prefixes) {
		final PriorityQueue<BgpNode> nodesQueue = getNodesQueue(triples, settings, statistics, prefixes);
		BgpNode currentNode = null;

		while (!nodesQueue.isEmpty()) {
			currentNode = nodesQueue.poll();
			final BgpNode relatedNode = findRelateNode(currentNode, nodesQueue);

			if (relatedNode != null) {
				final JoinNode joinNode = new JoinNode(currentNode, relatedNode, statistics, settings);
				nodesQueue.add(joinNode);
				nodesQueue.remove(currentNode);
				nodesQueue.remove(relatedNode);
			}
		}
		return currentNode;
	}

	private ArrayList<BgpNode> createJwptNodes(final List<Triple> triples, final Settings settings,
											   final DatabaseStatistics statistics, final PrefixMapping prefixes,
											   final HashSet<String> minimumVertexCover) {
		final ArrayList<BgpNode> nodesList = new ArrayList<>();

		for (final String resource : minimumVertexCover) {
			final TriplesGroup triplesGroup = new JoinedTriplesGroup(resource);


			for (final Triple triple : triples) {
				if (triple.getSubject().toString().equals(resource)){
					triplesGroup.addTriple(triple);
				} else if (triple.getObject().toString().equals(resource)){
					triplesGroup.addTriple(triple);
				}
			}
			nodesList.add(new JWPTNode(triplesGroup, prefixes, statistics, settings));
		}
		return nodesList;
	}

	private BgpNode computeLinearQueryPlan(final DatabaseStatistics statistics, final Settings settings,
										   final PrefixMapping prefixes, final HashSet<String> minimumVertexCover) {
		final PriorityQueue<BgpNode> nodesQueue = getNodesQueue(triples, settings, statistics, prefixes);

		assert nodesQueue.size() > 0 : "No elements in generated nodesQueue";

		if (nodesQueue.size() == 1) {
			return nodesQueue.poll();
		} else {
			BgpNode leftNode = null;
			BgpNode rightNode = null;
			double score = Double.MAX_VALUE;
			while (!nodesQueue.isEmpty()) {
				//first join (bottom of the linear tree)
				if (leftNode == null) {
					double candidateScore = Double.MAX_VALUE;
					final List<BgpNode> queue = new ArrayList<>(nodesQueue);
					final ListIterator<BgpNode> leftNodeIterator = queue.listIterator();
					while (leftNodeIterator.hasNext()) {
						final BgpNode candidateLeftNode = leftNodeIterator.next();
						final Iterator<BgpNode> rightNodeIterator = queue.listIterator(leftNodeIterator.nextIndex());
						while (rightNodeIterator.hasNext()) {
							final BgpNode candidateRightNode = rightNodeIterator.next();
							if (existsVariableInCommon(candidateLeftNode.collectTriples(),
									candidateRightNode.collectTriples())) {
								final JoinNode joinNode = new JoinNode(candidateLeftNode,
										candidateRightNode, statistics, settings);
								candidateScore = joinNode.getPriority();
								if (candidateScore < score) {
									leftNode = candidateLeftNode;
									rightNode = candidateRightNode;
									score = candidateScore;
								}
							}
						}
					}
					assert leftNode != null : "BGP not a closed graph?";

					nodesQueue.remove(leftNode);
					nodesQueue.remove(rightNode);
					leftNode = new JoinNode(leftNode, rightNode, statistics, settings);
					rightNode = null;
					score = Double.MAX_VALUE;
				} else {
					for (final BgpNode candidateRightNode : nodesQueue) {
						final double candidateScore = candidateRightNode.getPriority();
						if (existsVariableInCommon(leftNode.collectTriples(),
								candidateRightNode.collectTriples())
								&& candidateScore < score) {
							rightNode = candidateRightNode;
							score = candidateScore;
						}
					}

					assert rightNode != null : "BGP not a closed graph?";

					nodesQueue.remove(rightNode);
					leftNode = new JoinNode(leftNode, rightNode, statistics, settings);
					rightNode = null;
					score = Double.MAX_VALUE;
				}
			}
			return leftNode;
		}
	}

	private ArrayList<HashSet<String>> getMinimumVertexCovers(final List<Triple> triples) {
		final ArrayList<String[]> edgeCovers = new ArrayList<>();

		for (final Triple triple : triples) {
			final String[] edgeCover = new String[2];
			edgeCover[0] = triple.getSubject().toString();
			edgeCover[1] = triple.getObject().toString();
			edgeCovers.add(edgeCover);
		}

		ArrayList<HashSet<String>> vertexCovers = new ArrayList<>();
		int vcMaximumSize = 0;

		for (final String[] edge : edgeCovers) {
			final ArrayList<HashSet<String>> updatedVertexCovers = new ArrayList<>();
			if (vertexCovers.isEmpty()) {
				final HashSet<String> vc1 = new HashSet<>();
				final HashSet<String> vc2 = new HashSet<>();
				vc1.add(edge[0]);
				vc2.add(edge[1]);
				updatedVertexCovers.add(vc1);
				updatedVertexCovers.add(vc2);
				vcMaximumSize = 1;
			} else {
				for (final HashSet<String> originalVC : vertexCovers) {
					final HashSet<String> vc1 = new HashSet<>(originalVC);
					final HashSet<String> vc2 = new HashSet<>(originalVC);
					vc1.add(edge[0]);
					vc2.add(edge[1]);
					updatedVertexCovers.add(vc1);
					updatedVertexCovers.add(vc2);

					final int vc1Size = vc1.size();
					final int vc2Size = vc2.size();
					if (vc1Size > vcMaximumSize) {
						vcMaximumSize = vc1Size;
					} else if (vc2Size > vcMaximumSize) {
						vcMaximumSize = vc2Size;
					}
				}
			}
			vertexCovers = updatedVertexCovers;
		}

		final ListIterator<HashSet<String>> vcIterator = vertexCovers.listIterator();

		while (vcIterator.hasNext()) {
			if (vcIterator.next().size() < vcMaximumSize) {
				vcIterator.remove();
			}
		}

		return vertexCovers;
	}

	private PriorityQueue<BgpNode> getNodesQueue(final List<Triple> triples, final Settings settings,
												 final DatabaseStatistics statistics, final PrefixMapping prefixes) {
		final PriorityQueue<BgpNode> nodesQueue = new PriorityQueue<>(triples.size(), new NodeComparator());
		final List<Triple> unassignedTriples = new ArrayList<>();
		final List<Triple> unassignedTriplesWithVariablePredicate = new ArrayList<>();

		for (final Triple triple : triples) {
			if (triple.getPredicate().isVariable()) {
				unassignedTriplesWithVariablePredicate.add(triple);
			} else {
				unassignedTriples.add(triple);
			}
		}

		if (settings.isUsingWPT() || settings.isUsingIWPT() || settings.isUsingJWPTInner()
				|| settings.isUsingJWPTOuter() || settings.isUsingJWPTLeftouter()) {
			logger.info("Creating grouped nodes...");
			final TriplesGroupsMapping groupsMapping = new TriplesGroupsMapping(unassignedTriples, settings);
			while (groupsMapping.size() > 0) {
				final TriplesGroup largestGroup = groupsMapping.extractBestTriplesGroup(settings);
				if (largestGroup.size() < settings.getMinGroupSize()) {
					break;
				}
				final List<BgpNode> createdNodes = largestGroup.createNodes(settings, statistics, prefixes);
				if (!createdNodes.isEmpty()) {
					nodesQueue.addAll(createdNodes);
					groupsMapping.removeTriples(largestGroup);
					unassignedTriples.removeAll(largestGroup.getTriples());
				}
			}
			logger.info("Done! Triple patterns without nodes: " + (unassignedTriples.size()
					+ unassignedTriplesWithVariablePredicate.size()));
		}

		if (settings.isUsingVP()) {
			// VP only
			logger.info("Creating VP nodes...");
			createVpNodes(unassignedTriples, nodesQueue, statistics, settings, prefixes);
			logger.info("Done! Triple patterns without nodes: " + (unassignedTriples.size()
					+ unassignedTriplesWithVariablePredicate.size()));
		}
		if (settings.isUsingTT()) {
			logger.info("Creating TT nodes...");
			createTTNodes(unassignedTriples, nodesQueue, statistics, settings, prefixes);
			logger.info("Done! Triple patterns without nodes: " + (unassignedTriples.size()
					+ unassignedTriplesWithVariablePredicate.size()));
		}

		//create nodes for patterns with variable predicates
		for (final Triple triple : new ArrayList<>(unassignedTriplesWithVariablePredicate)) {
			final ArrayList<Triple> tripleAsList = new ArrayList<>(); // list is needed as argument to node creation
			// methods
			tripleAsList.add(triple);
			//first try to find best PT node type for the given pattern
			if (settings.isUsingWPT() && triple.getSubject().isConcrete()) {
				nodesQueue.add(new WPTNode(tripleAsList, prefixes, statistics, settings));
				unassignedTriplesWithVariablePredicate.remove(triple);
			} else if (settings.isUsingIWPT() && triple.getObject().isConcrete()) {
				nodesQueue.add(new IWPTNode(tripleAsList, prefixes, statistics, settings));
				unassignedTriplesWithVariablePredicate.remove(triple);
			} else if (settings.isUsingJWPTOuter()
					&& (triple.getSubject().isConcrete() || triple.getObject().isConcrete())) {
				nodesQueue.add(new JWPTNode(triple, prefixes, statistics, true, settings));
				unassignedTriplesWithVariablePredicate.remove(triple);
			} else if (settings.isUsingJWPTLeftouter()
					&& (triple.getSubject().isConcrete() || triple.getObject().isConcrete())) {
				nodesQueue.add(new JWPTNode(triple, prefixes, statistics, true, settings));
				unassignedTriplesWithVariablePredicate.remove(triple);
			} else {
				//no best pt node type, uses general best option
				if (settings.isUsingTT()) {
					createTTNodes(tripleAsList, nodesQueue, statistics, settings, prefixes);
					unassignedTriplesWithVariablePredicate.remove(triple);
				} else if (settings.isUsingVP()) {
					createVpNodes(tripleAsList, nodesQueue, statistics, settings, prefixes);
					unassignedTriplesWithVariablePredicate.remove(triple);
				} else if (settings.isUsingWPT()) {
					nodesQueue.add(new WPTNode(tripleAsList, prefixes, statistics, settings));
					unassignedTriplesWithVariablePredicate.remove(triple);
				} else if (settings.isUsingJWPTOuter()) {
					nodesQueue.add(new JWPTNode(triple, prefixes, statistics, true, settings));
					unassignedTriplesWithVariablePredicate.remove(triple);
				} else if (settings.isUsingJWPTLeftouter()) {
					nodesQueue.add(new JWPTNode(triple, prefixes, statistics, true, settings));
					unassignedTriplesWithVariablePredicate.remove(triple);
				} else if (settings.isUsingIWPT()) {
					nodesQueue.add(new IWPTNode(tripleAsList, prefixes, statistics, settings));
					unassignedTriplesWithVariablePredicate.remove(triple);
				}
			}
		}

		if (unassignedTriples.size() > 0 || unassignedTriplesWithVariablePredicate.size() > 0) {
			throw new RuntimeException("Cannot generate nodes queue. Some triple patterns are not assigned to a node.");
		} else {
			return nodesQueue;
		}
	}

	/**
	 * Given a source node, finds another node with at least one variable in common, if there isn't return null.
	 */
	private BgpNode findRelateNode(final BgpNode sourceNode, final PriorityQueue<BgpNode> availableNodes) {
		for (final TriplePattern tripleSource : sourceNode.collectTriples()) {
			for (final BgpNode node : availableNodes) {
				for (final TriplePattern tripleDest : node.collectTriples()) {
					if (existsVariableInCommon(tripleSource, tripleDest)) {
						return node;
					}
				}
			}
		}
		return null;
	}

	/**
	 * Check if two Triple Patterns share at least one variable.
	 */
	private boolean existsVariableInCommon(final TriplePattern tripleA, final TriplePattern tripleB) {
		final List<String> variablesTripleA = new ArrayList<>();
		if (tripleA.getSubjectType() == ElementType.VARIABLE) {
			variablesTripleA.add(tripleA.getSubject());
		}
		if (tripleA.getPredicateType() == ElementType.VARIABLE) {
			variablesTripleA.add(tripleA.getPredicate());
		}
		if (tripleA.getObjectType() == ElementType.VARIABLE) {
			variablesTripleA.add(tripleA.getObject());
		}

		final List<String> variablesTripleB = new ArrayList<>();
		if (tripleB.getSubjectType() == ElementType.VARIABLE) {
			variablesTripleB.add(tripleB.getSubject());
		}
		if (tripleB.getPredicateType() == ElementType.VARIABLE) {
			variablesTripleB.add(tripleB.getPredicate());
		}
		if (tripleB.getObjectType() == ElementType.VARIABLE) {
			variablesTripleB.add(tripleB.getObject());
		}

		for (final String varA : variablesTripleA) {
			for (final String varB : variablesTripleB) {
				if (varA.equals(varB)) {
					return true;
				}
			}
		}
		return false;
	}

	private boolean existsVariableInCommon(final List<TriplePattern> triplesListA,
										   final List<TriplePattern> triplesListB) {
		for (final TriplePattern tripleA : triplesListA) {
			for (final TriplePattern tripleB : triplesListB) {
				if (existsVariableInCommon(tripleA, tripleB)) {
					return true;
				}
			}
		}
		return false;
	}

	/**
	 * Creates TT nodes from a list of triples.
	 *
	 * @param triples    Triples for which TT nodes will be created
	 * @param nodesQueue PriorityQueue where created nodes are added to
	 */
	private void createTTNodes(final List<Triple> triples, final PriorityQueue<BgpNode> nodesQueue,
							   final DatabaseStatistics statistics, final Settings settings,
							   final PrefixMapping prefixes) {
		for (final Triple t : triples) {
			nodesQueue.add(new TTNode(new TriplePattern(t, prefixes), statistics, settings));
		}
		triples.clear();
	}

	/**
	 * Creates VP nodes from a list of triples.
	 *
	 * @param unassignedTriples Triples for which VP nodes will be created
	 * @param nodesQueue        PriorityQueue where created nodes are added to
	 */
	private void createVpNodes(final List<Triple> unassignedTriples, final PriorityQueue<BgpNode> nodesQueue,
							   final DatabaseStatistics statistics, final Settings settings,
							   final PrefixMapping prefixes) {
		final List<Triple> triples = new ArrayList<>(unassignedTriples);
		for (final Triple t : triples) {
			final BgpNode newNode = new VPNode(new TriplePattern(t, prefixes), statistics, settings);
			nodesQueue.add(newNode);
			unassignedTriples.remove(t);
		}
	}

	public BgpNode getRootNode() {
		return this.bgpRootNode;
	}

	@Override
	public String toString() {
		return this.bgpRootNode.toString();
	}
}
