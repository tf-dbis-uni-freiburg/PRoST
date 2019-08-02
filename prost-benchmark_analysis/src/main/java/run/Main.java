package run;

import java.util.HashMap;

import benchmark_analysis.CsvHandler;
import benchmark_analysis.QueryData;

/**
 * Loads a .csv benchmark file, merges results of same queries, removes outliers, and saves to a new .csv file.
 */
public class Main {
	public static void main(final String[] args) {
		final String filename = args[0];

		System.out.println("Loading " + filename);
		final HashMap<String, QueryData> data = CsvHandler.loadData(filename);
		for (final QueryData queryData : data.values()) {
			queryData.compute();
		}
		CsvHandler.saveData(filename, data);
		System.out.println("Saved file: " + filename + "_filtered.csv");
	}
}
