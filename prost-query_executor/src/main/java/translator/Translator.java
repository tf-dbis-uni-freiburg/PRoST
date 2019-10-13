package translator;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import java.util.List;
import java.util.PriorityQueue;
import java.io.BufferedReader;
import java.io.FileReader;

import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.query.Query;
import com.hp.hpl.jena.query.QueryFactory;
import com.hp.hpl.jena.shared.PrefixMapping;
import com.hp.hpl.jena.sparql.algebra.Algebra;
import com.hp.hpl.jena.sparql.algebra.Op;
import com.hp.hpl.jena.sparql.algebra.OpWalker;
import com.hp.hpl.jena.sparql.core.Var;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import com.google.gson.Gson;
import com.google.gson.JsonParser;
import com.google.gson.JsonArray;
import com.google.gson.reflect.TypeToken;
import joinTree.ElementType;
import joinTree.IWPTNode;
import joinTree.JWPTNode;
import joinTree.JoinNode;
import joinTree.JoinTree;
import joinTree.Node;
import joinTree.TTNode;
import joinTree.TriplePattern;
import joinTree.VPNode;
import joinTree.WPTNode;
import org.apache.log4j.Logger;
import statistics.DatabaseStatistics;
import statistics.PropertyStatistics;
import translator.triplesGroup.TriplesGroup;
import translator.triplesGroup.ForwardTriplesGroup;
import translator.triplesGroup.InverseTriplesGroup;
import translator.triplesGroup.TriplesGroupsMapping;
import utils.EmergentSchema;
import utils.Settings;

/**
 * This class parses the SPARQL query, build a {@link JoinTree} and save its
 * serialization in a file.
 *
 * @author Matteo Cossu
 * @author Polina Koleva
 */
public class Translator {
	private static final Logger logger = Logger.getLogger("PRoST");
	private final DatabaseStatistics statistics;
	private final Settings settings;
	private final String queryPath;
	private PrefixMapping prefixes;
	private HashMap<String, Double> tripleGroupOccurences;
	private List<TriplesGroup> triLengthGroups;
	private List<TriplesGroup> pairedGroups;

	public Translator(final Settings settings, final DatabaseStatistics statistics, final String queryPath) {
		this.settings = settings;
		this.statistics = statistics;
		this.queryPath = queryPath;
	}

	public JoinTree translateQuery() {
		// parse the query and extract prefixes
		final Query query = QueryFactory.read("file:" + queryPath);
		prefixes = query.getPrefixMapping();

		logger.info("** SPARQL QUERY **\n" + query + "\n****************");


		// extract variables, list of triples and filter
		final Op opQuery = Algebra.compile(query);
		final QueryVisitor queryVisitor = new QueryVisitor(prefixes);
		OpWalker.walk(opQuery, queryVisitor);

		fillTripleSizesMap();

		final QueryTree mainTree = queryVisitor.getMainQueryTree();
		final List<Var> projectionVariables = queryVisitor.getProjectionVariables();

		// build main tree
		Node rootNode = buildTree(mainTree.getTriples());
		for (final QueryTree optionalTree : queryVisitor.getOptionalQueryTrees()) {
			final Node optionalNode = buildTree(optionalTree.getTriples());
			rootNode = new JoinNode(rootNode, optionalNode, "leftouter", statistics, settings);
			//TODO apply filters from optionalNodes
		}
		final JoinTree tree = new JoinTree(rootNode, queryPath);

		tree.setFilter(mainTree.getFilter());

		// set projections
		if (projectionVariables != null) {
			// set the root node with the variables that need to be projected
			// only for the main tree
			final ArrayList<String> projectionList = new ArrayList<>();
			for (final Var projectionVariable : projectionVariables) {
				projectionList.add(projectionVariable.getVarName());
			}
			tree.setProjectionList(projectionList);
		}

		tree.setDistinct(query.isDistinct());

		logger.info("** Spark JoinTree **\n" + tree + "\n****************");
		return tree;
	}

	private void fillTripleSizesMap() {
		tripleGroupOccurences = new HashMap<>();
		Gson gson = new Gson();
	
		try {
			JsonParser parser = new JsonParser();
			String json = parser.parse(new FileReader("characteristics.json")).toString();
			tripleGroupOccurences = gson.fromJson(json, HashMap.class);
		}	 
		catch (Exception e) {

		}
	}

	/**
	 * Constructs the join tree.
	 */
	private Node buildTree(final List<Triple> triples) {
		final PriorityQueue<Node> nodesQueue = getDetailedNodesQueue(triples);

		Node currentNode = null;

		while (!nodesQueue.isEmpty()) {
			currentNode = nodesQueue.poll();
			final Node relatedNode = findRelateNode(currentNode, nodesQueue);

			if (relatedNode != null) {
				final JoinNode joinNode = new JoinNode(currentNode, relatedNode, statistics, settings);
				nodesQueue.add(joinNode);
				nodesQueue.remove(currentNode);
				nodesQueue.remove(relatedNode);
			}
		}
		return currentNode;
	}

	private PriorityQueue<Node> getDetailedNodesQueue(final List<Triple> triples) {
		PriorityQueue<Node> nodesQueue = new PriorityQueue<>(triples.size(), new NodeComparator());
		final List<Triple> unassignedTriples = new ArrayList<>();

		for (final Triple triple : triples) {
			unassignedTriples.add(triple);
		}
		TriplesGroupsMapping groupsMapping = new TriplesGroupsMapping(unassignedTriples, settings);
		Multimap<String, TriplesGroup> triplesGroups = groupsMapping.getTriplesGroups();

		List<TriplesGroup> uniqueTriples = new ArrayList<>();
		for (String vr: triplesGroups.keys()) {
			Collection<TriplesGroup> trigrps =  triplesGroups.get(vr);
			for (TriplesGroup trigrp : trigrps) {

				boolean isUnique = true;
				for (TriplesGroup oldGroup: uniqueTriples) {
					List<Triple> oldTriples = oldGroup.getTriples();
					List<Triple> currentTriples = trigrp.getTriples();
					if (oldTriples.equals(currentTriples)) {
						isUnique = false;
						break;
					}
				}
				if (isUnique) {
					uniqueTriples.add(trigrp);
				}
			}
		}
		/*
		List<TriplesGroup> bestGroups = getMinimalTripleGroup(triples, uniqueTriples);
		
		for (TriplesGroup triplesGroup: bestGroups) {
			List<Node> createdNodes = triplesGroup.createNodes(settings, statistics, prefixes);
			nodesQueue.addAll(createdNodes);

		}
	    */

	    orderPairsandTriples(uniqueTriples);

	    if (pairedGroups.size() % 2 == 0) {
	    	List<TriplesGroup> bestGroups = getEvenTriplesGroup();
	    	for (TriplesGroup triplesGroup: bestGroups) {
				List<Node> createdNodes = triplesGroup.createNodes(settings, statistics, prefixes);
				nodesQueue.addAll(createdNodes);
			}
	    }

	    else {
	    	List<TriplesGroup> bestGroups = getOddTriplesGroup();
	    	for (TriplesGroup triplesGroup: bestGroups) {
				List<Node> createdNodes = triplesGroup.createNodes(settings, statistics, prefixes);
				nodesQueue.addAll(createdNodes);
			}
	    }

		return nodesQueue;

	}

	private List<TriplesGroup> getOddTriplesGroup() {
		List<List<TriplesGroup> > candidateGroups = new ArrayList<>();

		List<TriplesGroup> oddGroups = new ArrayList<>();
		for (int i = 0; i < triLengthGroups.size(); i += 2) {
			oddGroups.add(triLengthGroups.get(i));
		}
		oddGroups.add(triLengthGroups.get(triLengthGroups.size() - 1));

		candidateGroups.add(oddGroups);

		List<TriplesGroup> evenGroups = new ArrayList<>();
		for (int i = 1; i < triLengthGroups.size(); i += 2) {
			evenGroups.add(triLengthGroups.get(i));
		}
		evenGroups.add(triLengthGroups.get(0));

		candidateGroups.add(evenGroups);

		for (int pivot = 0; pivot < pairedGroups.size(); pivot += 2) {
			List<TriplesGroup> mixedGroups = new ArrayList<>();
			mixedGroups.add(pairedGroups.get(pivot));
			
			for (int i = 0; i < pivot ; i += 2) {
				mixedGroups.add(triLengthGroups.get(i));	
			}

			for (int i = pivot + 1; i < triLengthGroups.size(); i += 2) {
				mixedGroups.add(triLengthGroups.get(i));
			}
			candidateGroups.add(mixedGroups);
		}

		int bestIndex = -1;
		long bestCost = Long.MAX_VALUE;

		for (int i = 0; i < candidateGroups.size(); i++) {
			long currentCost = evaluateTripleGroupCost(candidateGroups.get(i));

			if (currentCost < bestCost) {
				bestIndex = i;
				bestCost = currentCost;
			}
		}

		return candidateGroups.get(bestIndex);
	}

	private List<TriplesGroup> getEvenTriplesGroup() {
		List<TriplesGroup> bestGroups = new ArrayList<>();
		for (int i = 0; i < triLengthGroups.size(); i += 2) {
			bestGroups.add(triLengthGroups.get(i));
		}
		return bestGroups;
	}

	private void orderPairsandTriples(List<TriplesGroup> uniqueGroups) {
		pairedGroups = new ArrayList<>();
		triLengthGroups = new ArrayList<>();

		HashSet<String> groupsListString = new HashSet<>();

		for (TriplesGroup grp : uniqueGroups) {
			String listMapping = grp.getTriples().toString();
			groupsListString.add(listMapping);
		}

		List<TriplesGroup> newUniqueGroups = new ArrayList<>();
		for (TriplesGroup grp : uniqueGroups) {
			if (grp instanceof ForwardTriplesGroup) {
				if (grp.getTriples().size() > 1) {
					for (Triple triple : grp.getTriples()) {
						String triMap = "[" + triple.toString() + "]";
						if (!groupsListString.contains(triMap)) {
							TriplesGroup newGroup = new ForwardTriplesGroup(triple);
							newUniqueGroups.add(newGroup);
							groupsListString.add(triMap);
						} 
					}
				}
			}
			if (grp instanceof InverseTriplesGroup) {
				if (grp.getTriples().size() > 1) {
					for (Triple triple : grp.getTriples()) {
						String triMap = "[" + triple.toString() + "]";

						if (!groupsListString.contains(triMap)) {
							TriplesGroup newGroup = new InverseTriplesGroup(triple);
							newUniqueGroups.add(newGroup);
							groupsListString.add(triMap);
						} 
					}
				}
			}
		}

		uniqueGroups.addAll(newUniqueGroups);

		Multimap<String, TriplesGroup> uniqueGroupsMap = ArrayListMultimap.create();
		for (TriplesGroup grp : uniqueGroups) {
			List<Triple> triples = grp.getTriples();

			for (Triple tri : triples) {
				String subject = tri.getSubject().toString();
				String object = tri.getObject().toString();
				
				if (triples.size() == 1) {
					uniqueGroupsMap.put(subject, grp);
					uniqueGroupsMap.put(object, grp);
				}

				else {
					String commonVar = getCommonVariableInGroup(grp);


					if (!subject.equals(commonVar)) {
						uniqueGroupsMap.put(subject, grp);
					}

					if (!object.equals(commonVar)) {
						uniqueGroupsMap.put(object, grp);
					}
				}

				
			}

			if (triples.size() == 2) {
				uniqueGroupsMap.put(getCommonVariableInGroup(grp), grp);
			} 
		}

		/*for (String key : uniqueGroupsMap.keySet()) {
			Collection<TriplesGroup> val = uniqueGroupsMap.get(key);
			for (TriplesGroup grp : val) {
				System.out.println(grp.getTriples());	
			}
			System.out.println("---------------------------");
		}*/
		
		String source = null;
		String destination = null;



		for (String key : uniqueGroupsMap.keySet()) {
			if (source != null && destination != null) {
				break;
			}

			Collection<TriplesGroup> keyEdges = uniqueGroupsMap.get(key);
			if (keyEdges.size() > 2) {
				continue;
			}

			if (source == null) {
				source = key;
			}

			else {
				destination = key;
			}
		}



		HashSet<String> visitedNodes = new HashSet<>();

		String index = source;

		while (!index.equals(destination)) {
			Collection<TriplesGroup> indexEdges = uniqueGroupsMap.get(index);

			String next = null;



			for (TriplesGroup group : indexEdges ) {
				HashSet<String> groupVars = getTripleGroupVars(group);


				if (groupVars.size() == 2) {
					HashSet<String> intersection = new HashSet<String>(groupVars);
					intersection.retainAll(visitedNodes);

					if (intersection.size() == 0) {
						pairedGroups.add(group);
						HashSet<String> union = new HashSet<String>(groupVars);
						union.remove(index);
						for (String neighbour : union) {
							next = neighbour;
						}
					}
				}

				else {
					HashSet<String> intersection = new HashSet<String>(groupVars);
					intersection.retainAll(visitedNodes);

					if (intersection.size() == 0) {
						triLengthGroups.add(group);
					}
				}
			}
			visitedNodes.add(index);
			index = next;
		}
	}

	private String getCommonVariableInGroup(TriplesGroup trigroup) {
		List<Triple> triples = trigroup.getTriples();
		String firstSubject = triples.get(0).getSubject().toString();
		String firstObject = triples.get(0).getObject().toString();
		String secondSubject = triples.get(1).getSubject().toString();
		String secondObject = triples.get(1).getObject().toString();

		if(firstSubject.equals(secondObject)) {
			return firstSubject;
		}

		if(firstSubject.equals(secondSubject)) {
			return firstSubject;
		}

		return firstObject;

	}

	private List<TriplesGroup> getMinimalTripleGroup(List<Triple> triples, List<TriplesGroup> allTripleGroups) {
		int n = allTripleGroups.size();
		List<TriplesGroup> bestGroups = allTripleGroups;
		int minSize = n;
		long groupCost = evaluateTripleGroupCost(allTripleGroups);
		
		for (int i = 0; i < (1 << n); i++) {
			List<TriplesGroup> candidateGroups = new ArrayList<TriplesGroup>();
			for (int j = 0; j < n; j++) {
				
				if ( (i & (1 << j)) > 0) {
					candidateGroups.add(allTripleGroups.get(j));
				}
			}
			
			if (! areAllTriplesCovered(triples, candidateGroups)) {
				continue;
			}

			if (candidateGroups.size() == minSize) {
				long newCost = evaluateTripleGroupCost(candidateGroups);
				if (newCost < groupCost) {
					groupCost = newCost;
					bestGroups = candidateGroups;
				}
			}

			else if (candidateGroups.size() < minSize) {
				bestGroups = candidateGroups;
				minSize = candidateGroups.size();
			}


		}
		
		return bestGroups;
	}

	private long evaluateTripleGroupCost(List<TriplesGroup> candidateGroups) {
		long answer = 0;
		for (TriplesGroup grp: candidateGroups ) {

			List<Triple> grpTriples = grp.getTriples();
			if (grpTriples.size() == 1) {
				answer += getSingularGroupSize(grpTriples.get(0));
			} 
			else  {
				Triple first = grpTriples.get(0);
				Triple second = grpTriples.get(1);
				String firstPredicate = first.getPredicate().toString();
				String secondPredicate = second.getPredicate().toString();
				if (firstPredicate.compareTo(secondPredicate) < 0) {
					answer += getDoubleGroupSize(first, second);
				}
				else {
					answer += getDoubleGroupSize(second, first);
				}
			}
		}

		return answer;
	}



	private long getDoubleGroupSize(Triple first, Triple second) {
		String relationBetweenTriples = getCommonVariablePlacing(first, second); 
		String firstPredicate = "<" + first.getPredicate().toString() + ">";
		String secondPredicate = "<" + second.getPredicate().toString() + ">";
		String charKey = firstPredicate + "-" + secondPredicate + "-" + relationBetweenTriples;
		double answer = tripleGroupOccurences.get(charKey);
		return (long) answer;
	}



	private String getCommonVariablePlacing(Triple first, Triple second) {
		if (first.getSubject().toString().equals(second.getSubject().toString())) {
			return "SS";
		}

		if (first.getObject().toString().equals(second.getObject().toString())) {
			return "OO";
		}

		if (first.getSubject().toString().equals(second.getObject().toString())) {
			return "SO";
		}
		return "OS";
	}

	private long getSingularGroupSize(Triple triple) {
		String predicate = "<" + triple.getPredicate().toString() + ">";
		HashMap<String, PropertyStatistics> properties = statistics.getProperties();
		long answer = (long) properties.get(predicate).getTuplesNumber();
		return answer;
	}
	

	private HashSet<String> getTripleGroupVars(TriplesGroup group) {
		HashSet<String> vars = new HashSet<String>();
		for (Triple triple: group.getTriples()) {
			String subject = triple.getSubject().toString();
			String object = triple.getObject().toString();
			vars.add(subject);
			vars.add(object);
		}
		return vars;
	}

	private boolean areAllTriplesCovered(List<Triple> triples, List<TriplesGroup> candidateGroups) {
		HashSet<Triple> allCandidateTriples = new HashSet<>();
		for (TriplesGroup candidate: candidateGroups) {
			if (triples.size() == allCandidateTriples.size()) {
				return true;
			}
			List<Triple> candidateTriples = candidate.getTriples();
			HashSet<Triple> candidateSet = new HashSet<Triple>(candidateTriples);
			allCandidateTriples.addAll(candidateSet);
		}
		return triples.size() == allCandidateTriples.size();
	}

	private PriorityQueue<Node> getNodesQueue(final List<Triple> triples) {
	    PriorityQueue<Node> nodesQueue = new PriorityQueue<>(triples.size(), new NodeComparator());
		final List<Triple> unassignedTriples = new ArrayList<>();
		final List<Triple> unassignedTriplesWithVariablePredicate = new ArrayList<>();

		for (final Triple triple : triples) {
			if (triple.getPredicate().isVariable()) {
				unassignedTriplesWithVariablePredicate.add(triple);
			} else {
				unassignedTriples.add(triple);
			}
		}

		HashSet<Triple> assignedTriples = new HashSet<>();

		if (settings.isUsingWPT() || settings.isUsingIWPT() || settings.isUsingJWPTInner()
				|| settings.isUsingJWPTOuter() || settings.isUsingJWPTLeftouter()) {
			logger.info("Creating grouped nodes...");
			final TriplesGroupsMapping groupsMapping = new TriplesGroupsMapping(unassignedTriples, settings);
			while (groupsMapping.size() > 0) {
				final TriplesGroup largestGroup = groupsMapping.extractBestTriplesGroup(settings);
				
				if (largestGroup.size() < 2) {
					break;
				}
				final List<Node> createdNodes = largestGroup.createNodes(settings, statistics, prefixes);
				if (!createdNodes.isEmpty()) {
					nodesQueue.addAll(createdNodes);
					//groupsMapping.removeTriples(largestGroup);
					assignedTriples.addAll(largestGroup.getTriples());
					//unassignedTriples.removeAll(largestGroup.getTriples());
				}
			}
			logger.info("Done! Triple patterns without nodes: " + (unassignedTriples.size()
					+ unassignedTriplesWithVariablePredicate.size()));
		}

		if (settings.isUsingVP()) {
			// VP only
			logger.info("Creating VP nodes...");
			createVpNodes(unassignedTriples, nodesQueue);
			logger.info("Done! Triple patterns without nodes: " + (unassignedTriples.size()
					+ unassignedTriplesWithVariablePredicate.size()));
		}
		if (settings.isUsingTT()) {
			logger.info("Creating TT nodes...");
			createTTNodes(unassignedTriples, nodesQueue);
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
					createTTNodes(tripleAsList, nodesQueue);
					unassignedTriplesWithVariablePredicate.remove(triple);
				} else if (settings.isUsingVP()) {
					createVpNodes(tripleAsList, nodesQueue);
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

		
		unassignedTriples.removeAll(new ArrayList<Triple>(assignedTriples));

		nodesQueue = clearRedundantNodes(nodesQueue);


		if (unassignedTriples.size() > 0 || unassignedTriplesWithVariablePredicate.size() > 0) {
			throw new RuntimeException("Cannot generate nodes queue. Some triple patterns are not assigned to a node.");
		} else {
		
			return nodesQueue;
		}
	}

	private PriorityQueue<Node> clearRedundantNodes(PriorityQueue<Node> nodesQueue) {
		List<Node> nodesList = new ArrayList<>(nodesQueue);
		PriorityQueue<Node> filteredNodes = new PriorityQueue<>(nodesQueue.size(), new NodeComparator());
		for (int i = nodesQueue.size() - 1; i >= 0 ;i--) {
			Node currentNode = nodesList.get(i);
			Set<String> currentNodeVars = getNodeVariables(currentNode);
			Set<String> othersVars = new HashSet<>();
			for (int j = 0; j < nodesList.size() ;j++) {
				if ( i != j ) {
					Node neighbourNode = nodesList.get(j);
					Set<String> neighbourVars = getNodeVariables(neighbourNode);
					othersVars.addAll(neighbourVars);
				} 
			}
			othersVars.retainAll(currentNodeVars);
			if (othersVars.size() != currentNodeVars.size()) {
				filteredNodes.add(currentNode);
			}
		}
		return filteredNodes;
	}

	private Set<String> getNodeVariables(Node node) {
		List<TriplePattern> triples = node.collectTriples();
		Set<String> nodeVars = new HashSet<>();
		for (TriplePattern triple : triples) {
			nodeVars.add(triple.getSubject());
			nodeVars.add(triple.getObject());
		}
		return nodeVars;
	}	


	/**
	 * Creates VP nodes from a list of triples.
	 *
	 * @param unassignedTriples Triples for which VP nodes will be created
	 * @param nodesQueue        PriorityQueue where created nodes are added to
	 */
	private void createVpNodes(final List<Triple> unassignedTriples, final PriorityQueue<Node> nodesQueue) {
		final List<Triple> triples = new ArrayList<>(unassignedTriples);
		for (final Triple t : triples) {
			final Node newNode = new VPNode(new TriplePattern(t, prefixes), statistics, settings);
			nodesQueue.add(newNode);
			unassignedTriples.remove(t);
		}
	}

	/**
	 * Creates TT nodes from a list of triples.
	 *
	 * @param triples    Triples for which TT nodes will be created
	 * @param nodesQueue PriorityQueue where created nodes are added to
	 */
	private void createTTNodes(final List<Triple> triples, final PriorityQueue<Node> nodesQueue) {
		for (final Triple t : triples) {
			nodesQueue.add(new TTNode(new TriplePattern(t, prefixes), statistics, settings));
		}
		triples.clear();
	}

	/**
	 * Groups the input triples by subject considering the emergent schema. If two
	 * triples have the same subject, but they are not part of the same property
	 * table, they won't be grouped.
	 *
	 * @param triples triples to be grouped
	 * @return hash map of triples grouped by the subject considering the emergent schema
	 */
	private HashMap<String, HashMap<String, List<Triple>>> getEmergentSchemaSubjectGroups(final List<Triple> triples) {
		// key - table names, (subject, triples)
		final HashMap<String, HashMap<String, List<Triple>>> subjectGroups = new HashMap<>();
		for (final Triple triple : triples) {
			if (!triple.getPredicate().isVariable()) {
				final String subject = triple.getSubject().toString(prefixes);
				// find in which table this triple is stored, based on the predicate
				final String subjectTableName = EmergentSchema.getInstance()
						.getTable(statistics.getProperties().get(triple.getPredicate().toString()).getInternalName());
				// if we already have a triple for the table
				if (subjectGroups.containsKey(subjectTableName)) {
					final HashMap<String, List<Triple>> subjects = subjectGroups.get(subjectTableName);
					// if we have a triple with the same subject
					if (subjects.containsKey(subject)) {
						subjectGroups.get(subjectTableName).get(subject).add(triple);
					} else {
						final List<Triple> subjTriples = new ArrayList<>();
						subjTriples.add(triple);
						subjectGroups.get(subjectTableName).put(triple.getSubject().toString(prefixes), subjTriples);
					}
				} else {
					// add new table and a new subject to it
					final HashMap<String, List<Triple>> subjectGroup = new HashMap<>();
					final List<Triple> subjTriples = new ArrayList<>();
					subjTriples.add(triple);
					subjectGroup.put(triple.getSubject().toString(prefixes), subjTriples);
					subjectGroups.put(subjectTableName, subjectGroup);
				}
			}
		}
		return subjectGroups;
	}

	/**
	 * Given a source node, finds another node with at least one variable in common,
	 * if there isn't return null.
	 */
	private Node findRelateNode(final Node sourceNode, final PriorityQueue<Node> availableNodes) {
		for (final TriplePattern tripleSource : sourceNode.collectTriples()) {
			for (final Node node : availableNodes) {
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
}
