import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;

class csvHandler {
	private HashMap<String, QueryData> data;

	csvHandler() {
		data = new HashMap<>();
	}

	void loadData(final String csvFilePath) {
		final Path pathToFile = Paths.get(csvFilePath);

		try {
			final BufferedReader br = Files.newBufferedReader(pathToFile, StandardCharsets.UTF_8);
			br.readLine();//skips header
			String line = br.readLine();
			while (line != null) {
				final String[] elements = line.split(",");
				final String query = elements[0];
				final int time = Integer.parseInt(elements[1]);

				if (data.containsKey(query)) {
					data.get(query).addTime(time);
				} else {
					final QueryData newQueryData = new QueryData();
					newQueryData.addTime(time);
					newQueryData.setResultsCount(Long.valueOf(elements[2]));
					newQueryData.setJoinsCount(Integer.parseInt(elements[3]));
					newQueryData.setBroadcastJoinsCount(Integer.parseInt(elements[4]));
					newQueryData.setSortMergeJoinsCount(Integer.parseInt(elements[5]));
					newQueryData.setJoinNodesCount(Integer.parseInt(elements[6]));
					newQueryData.setTtNodesCount(Integer.parseInt(elements[7]));
					newQueryData.setVpNodesCount(Integer.parseInt(elements[8]));
					newQueryData.setWptNodesCount(Integer.parseInt(elements[9]));
					newQueryData.setIwptNodesCount(Integer.parseInt(elements[10]));
					newQueryData.setJwptNodesCount(Integer.parseInt(elements[11]));

					data.put(query, newQueryData);
				}
				line = br.readLine();
			}

		} catch (final IOException e) {
			e.printStackTrace();
		}

		for (final QueryData queryData : data.values()) {
			queryData.compute();
		}
	}

	void saveData(String fileName) {
		fileName = fileName.replace(".csv", "_filtered.csv");
		try (final BufferedWriter writer = Files.newBufferedWriter(Paths.get(fileName), StandardCharsets.UTF_8,
				StandardOpenOption.APPEND, StandardOpenOption.CREATE);

			 final CSVPrinter csvPrinter = new CSVPrinter(writer,
					 CSVFormat.DEFAULT.withHeader("Query", "Average", "Median", "Q1",
							 "Q3", "UpperFence", "Outliers", "Number of Results", "Joins", "Broadcast joins",
							 "SortMergeJoins", "Join Nodes", "TT Nodes", "VP Nodes", "WPT Nodes", "IWPT Nodes",
							 "JWPT Nodes"))) {
			for (final Map.Entry<String, QueryData> entry : this.data.entrySet()) {
				final QueryData queryData = entry.getValue();
				csvPrinter.printRecord(entry.getKey(), queryData.getAverage(),
						queryData.getMedian(), queryData.getQ1(),
						queryData.getQ3(), queryData.getUpperFence(),
						queryData.getNumberOfOutliersTrimmed(), queryData.getResultsCount(),
						queryData.getJoinsCount(), queryData.getBroadcastJoinsCount(),
						queryData.getSortMergeJoinsCount(), queryData.getJoinNodesCount(),
						queryData.getTtNodesCount(), queryData.getVpNodesCount(), queryData.getWptNodesCount(),
						queryData.getIwptNodesCount(), queryData.getJwptNodesCount());
			}
			csvPrinter.flush();
		} catch (final IOException e) {
			e.printStackTrace();
		}
	}

}
