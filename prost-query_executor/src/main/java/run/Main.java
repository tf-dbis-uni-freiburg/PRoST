package run;

import java.io.File;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

import executor.Executor;
import joinTree.JoinTree;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import statistics.DatabaseStatistics;
import translator.Translator;
import utils.EmergentSchema;
import utils.Settings;

/**
 * The Main class parses the CLI arguments and calls the translator and the
 * executor.
 *
 * @author Matteo Cossu
 * @author Polina Koleva
 */
public class Main {
	private static final Logger logger = Logger.getLogger("PRoST");
	private static final String loj4jFileName = "log4j.properties";

	public static void main(final String[] args) throws Exception {
		logger.info("INITIALIZING QUERY-EXECUTOR");

		final InputStream inStream = Main.class.getClassLoader().getResourceAsStream(loj4jFileName);
		final Properties props = new Properties();
		props.load(inStream);
		PropertyConfigurator.configure(props);

		final Settings settings = new Settings(args);

		// if emergent schema has to be applied
		if (settings.isUsingEmergentSchema()) {
			EmergentSchema.getInstance().readSchema(settings.getEmergentSchemaPath());
		}

		final DatabaseStatistics statistics = DatabaseStatistics.loadFromFile(settings.getStatsPath());
		settings.checkTablesAvailability(statistics);

		final File file = new File(settings.getQueriesInputPath());

		// create an executor
		final Executor executor = new Executor(settings);

		// single file
		if (file.isFile()) {

			// translation phase
			final JoinTree translatedQuery = translateSingleQuery(settings.getQueriesInputPath(),
					statistics, settings);

			executor.execute(translatedQuery);

			// if benchmark file is presented, save results
			if (settings.isSavingBenchmarkFile()) {
				executor.saveResultsCsv(settings.getBenchmarkFilePath());
			}
		} else if (file.isDirectory()) {
			final List<String> queryFiles = Arrays.asList(file.list());

			// if random order applied, shuffle the queries
			if (settings.isRandomQueryOrder()) {
				Collections.shuffle(queryFiles);
			}

			// if the path is a directory execute every files inside
			for (final String fileName : queryFiles) {
				logger.info("Starting: " + fileName);

				// translation phase
				final JoinTree translatedQuery = translateSingleQuery(settings.getQueriesInputPath() + "/" + fileName,
						statistics, settings);

				// execution phase
				executor.execute(translatedQuery);
			}

			if (settings.isSavingBenchmarkFile()) {
				logger.info("Benchmark file: " + settings.getBenchmarkFilePath());
				executor.saveResultsCsv(settings.getBenchmarkFilePath());
			}
		}
	}

	private static JoinTree translateSingleQuery(final String query, final DatabaseStatistics statistics,
												 final Settings settings) {
		final Translator translator = new Translator(settings, statistics, query);
		return translator.translateQuery();
	}
}
