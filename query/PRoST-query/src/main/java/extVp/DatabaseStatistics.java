package extVp;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.NavigableSet;
import java.util.TreeMap;

import org.apache.log4j.Logger;
import org.apache.spark.sql.SparkSession;

/**
 * <p>
 * This class is used to mantain the ExtVP database and its tables usage statistics.
 * </p>
 *
 */
public class DatabaseStatistics implements Serializable {
	private static final long serialVersionUID = -5125939737940488044L;
	private static final Logger logger = Logger.getLogger("PRoST");
	/** number of rows in used in the database. */
	private long size;
	/**
	 * HashMaps of tables in the database for which statistics were calculated.
	 */
	private final Map<String, TableStatistic> tables;
	private transient TreeMap<String, TableStatistic> sortedTables;
	private final String databaseName;

	public DatabaseStatistics(final String databaseName) {
		size = 0;
		tables = new HashMap<>();
		sortedTables = new TreeMap<>();
		this.databaseName = databaseName;
	}

	public void setSize(final long size) {
		this.size = size;
	}

	public long getSize() {
		return size;
	}

	public Map<String, TableStatistic> getTables() {
		return tables;
	}

	/**
	 * Loads a DatabaseStatistics binary file.
	 *
	 *
	 * @param extVPDatabaseName
	 *            name of the extVP database name and .stats file name
	 * @param dbStatistics
	 *            <i>DatabaseStatistics</i> object to be loaded into
	 * @return returns the loaded <i>DatabaseStatistics</i> object (<i>dbStatistics</i>
	 *         variable). <i>dbStatistics</i> remains unchanged if no .stats file is present.
	 */
	public static DatabaseStatistics loadStatisticsFile(final String extVPDatabaseName,
			DatabaseStatistics dbStatistics) {
		final File file = new File(extVPDatabaseName + ".stats");
		if (file.exists()) {
			dbStatistics = null;
			try {
				final FileInputStream fis = new FileInputStream(extVPDatabaseName + ".stats");
				final ObjectInputStream ois = new ObjectInputStream(fis);
				dbStatistics = (DatabaseStatistics) ois.readObject();
				ois.close();
				fis.close();
			} catch (final ClassNotFoundException | IOException e) {
				e.printStackTrace();
				return dbStatistics;
			}
			logger.info("ExtVP statistics file loaded!");
		} else {
			logger.info("No ExtVp statistics file found!");
		}
		return dbStatistics;
	}

	/**
	 * Saves a binary file containing database statistics.
	 *
	 * @param extVPDatabaseName
	 *            name of the ExtVP database name to be used in the name of the file
	 * @param dbStatistics
	 *            <i>DatabaseStatistics</i> object to be saved
	 */
	public static void saveStatisticsFile(final String extVPDatabaseName, final DatabaseStatistics dbStatistics) {
		try {
			final FileOutputStream fos = new FileOutputStream(extVPDatabaseName + ".stats");
			final ObjectOutputStream oos = new ObjectOutputStream(fos);
			oos.writeObject(dbStatistics);
			oos.close();
			fos.close();
		} catch (final IOException ioe) {
			ioe.printStackTrace();
		}
		logger.info("ExtVP database data saved in " + extVPDatabaseName + ".stats");
	}

	/**
	 * Updates <i>sortedTables</i> with the sorting of <i>tables</i>.
	 */
	private void sortTables() {
		final TableComparator comparator = new TableComparator(tables);
		sortedTables = new TreeMap<>(comparator);
		sortedTables.putAll(tables);
	}

	/**
	 * Drops <i>tableName</i> from the ExtVP database and updates statistics database size.
	 *
	 * @param tableName
	 *            name of semi-join table to be dropped
	 * @param spark
	 *            spark session variable
	 */
	private void removeTableFromCache(final String tableName, final SparkSession spark) {
		final String query = "drop table " + databaseName + "." + tableName;
		// logger.info("query: " + query);
		spark.sql(query);
		final TableStatistic statistic = tables.get(tableName);
		statistic.setTableExists(false);
		logger.info("Table " + tableName + " removed from cache");
		size = size - statistic.getSize();
	}

	/**
	 * Frees up space in ExtVP database.
	 *
	 * @param expectedSize
	 *            maximum size of the ExtVP database after clearing up space
	 * @param spark
	 *            Spark session
	 */
	public void clearCache(final long expectedSize, final SparkSession spark) {
		logger.info("Clearing cache...");
		logger.info("Cache size: " + size + "; expected size: " + expectedSize);

		sortTables();

		final NavigableSet<String> tablesKeys = sortedTables.descendingKeySet();
		final Iterator<String> iterator = tablesKeys.descendingIterator();
		while (expectedSize < size && iterator.hasNext()) {
			final String tableName = iterator.next();
			final TableStatistic tableStatistic = tables.get(tableName);
			if (tableStatistic.getTableExists() == true) {
				removeTableFromCache(tableName, spark);
			}
		}
		logger.info("Done clearing cache. Final size: " + size);
	}

	/**
	 * Frees up space in ExtVP database.
	 * <p>
	 * If the ExtVP database is over the <i>maxSize value</i>, remove tables with lowest
	 * selectivity until the database size is at most <i>expectedSize</i> A <i>maxSize</i>
	 * value of -1 means the cache is never cleared.
	 * </p>
	 *
	 * @param expectedSize
	 *            Maximum size of the database after freeing up space
	 * @param maxSize
	 *            The minimum size the database must have before clearing the cache
	 * @param spark
	 *            Spark session
	 */
	public void clearCache(final long expectedSize, final long maxSize, final SparkSession spark) {
		if (maxSize != -1) {
			if (maxSize < size) {
				clearCache(expectedSize, spark);
			}
		}
	}
}
