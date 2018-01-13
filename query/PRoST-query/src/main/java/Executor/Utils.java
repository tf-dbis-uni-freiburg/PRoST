package Executor;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import JoinTree.ElementType;
import JoinTree.TriplePattern;



public class Utils {
	
	
	/**
	 * Makes the string conform to the requirements for HiveMetastore column names.
	 * e.g. remove braces, replace non word characters, trim spaces.
	 */
	public static String toMetastoreName(String s) {
		return s.replaceAll("[<>]", "").trim().replaceAll("[[^\\w]+]", "_");
	}
	
	public static String removeQuestionMark(String s){
		if(s.startsWith("?"))
			return s.substring(1);
		return s;
	}
	
	/**
	 * findCommonVariable find a return the common variable between two triples.
	 * 
	 */
	private static String findCommonVariable(TriplePattern a, TriplePattern b){
		if(a.subjectType == ElementType.VARIABLE && 
				(removeQuestionMark(a.subject).equals(removeQuestionMark(b.subject)) 
						|| removeQuestionMark(a.subject).equals(removeQuestionMark(b.object))))
			return removeQuestionMark(a.subject);
		if(a.objectType == ElementType.VARIABLE && 
				(removeQuestionMark(a.object).equals(removeQuestionMark(b.subject)) 
						|| removeQuestionMark(a.object).equals(removeQuestionMark(b.object))))
			return removeQuestionMark(a.object);
		return null;
	}
	
	public static String findCommonVariable(TriplePattern tripleA, List<TriplePattern> tripleGroupA, 
			TriplePattern tripleB, List<TriplePattern> tripleGroupB){
		// triple with triple case
		if(tripleGroupA.isEmpty() && tripleGroupB.isEmpty())
			return findCommonVariable(tripleA, tripleB);
		if(!tripleGroupA.isEmpty() && !tripleGroupB.isEmpty())
			for(TriplePattern at : tripleGroupA)
				for(TriplePattern bt : tripleGroupB)
					if(findCommonVariable(at, bt) != null)
						return findCommonVariable(at, bt);
		if(tripleGroupA.isEmpty())
			for(TriplePattern bt : tripleGroupB)
				if(findCommonVariable(tripleA, bt) != null)
					return findCommonVariable(tripleA, bt);
		if(tripleGroupB.isEmpty())
			for(TriplePattern at : tripleGroupA)
				if(findCommonVariable(at, tripleB) != null)
					return findCommonVariable(at, tripleB);
		
		return null;
	}
	
	public static List<String> commonVariables(String[] variablesOne, String[] variablesTwo) {
	  Set<String> varsOne = new HashSet<String>(Arrays.asList(variablesOne)); 
	  Set<String> varsTwo = new HashSet<String>(Arrays.asList(variablesTwo));
	  varsOne.retainAll(varsTwo);
	  
	  List<String> results = new ArrayList<String>(varsOne);
	  if (!varsOne.isEmpty())
	    return results;
	  
	  return null;	  
	}

}
