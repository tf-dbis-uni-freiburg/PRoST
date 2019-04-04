package utils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import joinTree.ElementType;
import joinTree.TriplePattern;

public class Utils {

	/**
	 * Makes the string conform to the requirements for HiveMetastore column names. e.g. remove braces, replace non word
	 * characters, trim spaces.
	 */
	public static String toMetastoreName(final String s) {
		return s.replaceAll("[<>]", "").trim().replaceAll("[[^\\w]+]", "_");
	}

	public static String removeQuestionMark(final String s) {
		if (s.startsWith("?")) {
			return s.substring(1);
		}
		return s;
	}

	/**
	 * findCommonVariable find a return the common variable between two triples.
	 */
	private static String findCommonVariable(final TriplePattern a, final TriplePattern b) {
		if (a.subjectType == ElementType.VARIABLE
				&& (removeQuestionMark(a.subject).equals(removeQuestionMark(b.subject))
				|| removeQuestionMark(a.subject).equals(removeQuestionMark(b.object)))) {
			return removeQuestionMark(a.subject);
		}
		if (a.objectType == ElementType.VARIABLE && (removeQuestionMark(a.object).equals(removeQuestionMark(b.subject))
				|| removeQuestionMark(a.object).equals(removeQuestionMark(b.object)))) {
			return removeQuestionMark(a.object);
		}
		return null;
	}

	public static String findCommonVariable(final TriplePattern tripleA, final List<TriplePattern> tripleGroupA,
											final TriplePattern tripleB, final List<TriplePattern> tripleGroupB) {
		// triple with triple case
		if (tripleGroupA.isEmpty() && tripleGroupB.isEmpty()) {
			return findCommonVariable(tripleA, tripleB);
		}
		if (!tripleGroupA.isEmpty() && !tripleGroupB.isEmpty()) {
			for (final TriplePattern at : tripleGroupA) {
				for (final TriplePattern bt : tripleGroupB) {
					if (findCommonVariable(at, bt) != null) {
						return findCommonVariable(at, bt);
					}
				}
			}
		}
		if (tripleGroupA.isEmpty()) {
			for (final TriplePattern bt : tripleGroupB) {
				if (findCommonVariable(tripleA, bt) != null) {
					return findCommonVariable(tripleA, bt);
				}
			}
		}
		if (tripleGroupB.isEmpty()) {
			for (final TriplePattern at : tripleGroupA) {
				if (findCommonVariable(at, tripleB) != null) {
					return findCommonVariable(at, tripleB);
				}
			}
		}

		return null;
	}

	public static List<String> commonVariables(final String[] variablesOne, final String[] variablesTwo) {
		final Set<String> varsOne = new HashSet<>(Arrays.asList(variablesOne));
		final Set<String> varsTwo = new HashSet<>(Arrays.asList(variablesTwo));
		varsOne.retainAll(varsTwo);

		final List<String> results = new ArrayList<>(varsOne);
		if (!varsOne.isEmpty()) {
			return results;
		}

		return null;
	}

}
