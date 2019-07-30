package utils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class Utils {
	public static String removeQuestionMark(final String string) {
		if (string.startsWith("?")) {
			return string.substring(1);
		}
		return string;
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
