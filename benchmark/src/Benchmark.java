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

public class Benchmark {
	private HashMap<String, QueryData> data;


	public Benchmark() {
		data = new HashMap<>();
	}

	public void loadData(final String csvFilePath) {
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

	public void saveData(String fileName) {
		fileName = fileName.replace(".csv", "_filtered.csv");
		try (final BufferedWriter writer = Files.newBufferedWriter(Paths.get(fileName), StandardCharsets.UTF_8,
				StandardOpenOption.APPEND, StandardOpenOption.CREATE);

			 final CSVPrinter csvPrinter = new CSVPrinter(writer,
					 CSVFormat.DEFAULT.withHeader("Query", "Average", "Median", "Q1",
							 "Q3", "UpperFence", "Outliers"))) {
			for (final Map.Entry<String, QueryData> entry : this.data.entrySet()) {
				final QueryData queryData = entry.getValue();
				csvPrinter.printRecord(entry.getKey(), queryData.getAverage(),
						queryData.getMedian(), queryData.getQ1(),
						queryData.getQ3(), queryData.getUpperFence(),
						queryData.getNumberOfOutliersTrimmed());
			}
			csvPrinter.flush();
		} catch (final IOException e) {
			e.printStackTrace();
		}
	}

}
