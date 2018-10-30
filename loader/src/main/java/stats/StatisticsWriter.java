package stats;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.List;

import stats.ProtobufStats.Graph;
import stats.ProtobufStats.TableStats;

public class StatisticsWriter {
	
	public String stats_file_suffix = ".stats";

	private final boolean computeStatistics;

	/*
	 * save the statistics in a serialized file
	 */
	private void save_stats(final String name, final List<TableStats> table_stats) {
		final Graph.Builder graph_stats_builder = Graph.newBuilder();

		graph_stats_builder.addAllTables(table_stats);
		final Graph serialized_stats = graph_stats_builder.build();

		FileOutputStream f_stream; // s
		File file;
		try {
			file = new File(name + stats_file_suffix);
			f_stream = new FileOutputStream(file);
			serialized_stats.writeTo(f_stream);
		} catch (final FileNotFoundException e) {
			e.printStackTrace();
		} catch (final IOException e) {
			e.printStackTrace();
		}
	}

}
