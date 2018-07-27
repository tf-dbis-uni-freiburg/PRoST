package extVp;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.NavigableSet;
import java.util.TreeMap;

import org.apache.log4j.Logger;
import org.apache.spark.sql.SparkSession;

/**
 * <p>This class is used to mantain the ExtVP database and its tables usage statistics</p>
 *
 */
public class DatabaseStatistics implements Serializable{
	private static final long serialVersionUID = -5125939737940488044L;
	private long size; //total number of rows
	private Map<String, TableStatistic> tables;
	private transient TreeMap<String, TableStatistic> sortedTables;
	private String databaseName;
	
	private static final Logger logger = Logger.getLogger("PRoST");

	
	public DatabaseStatistics (String databaseName) {
		this.size = 0;
		this.tables = new HashMap<String, TableStatistic>();
		this.sortedTables = new TreeMap<String, TableStatistic>();
		this.databaseName = databaseName;
	}
	
	/**
	 * @param size Sets the number of rows in the database
	 */
	public void setSize(long size) {
		this.size = size;
	}
	
	/**
	 * @return returns the number of rows in the database
	 */
	public long getSize() {
		return this.size;
	}
	
	/**
	 * @return HashMaps of tables in the database for which statistics were calculated
	 */
	public Map<String, TableStatistic> getTables(){
		return tables;
	}
	
	/**
	 * Loads a DatabaseStatistics binary file
	 * 
	 * 
	 * @param extVPDatabaseName name of the extVP database name and .stats file name
	 * @param dbStatistics <i>DatabaseStatistics</i> object to be loaded into
	 * @return returns the loaded <i>DatabaseStatistics</i> object (<i>dbStatistics</i> variable). 
	 * <i>dbStatistics</i> remains unchanged if no .stats file is present.
	 */
	public static DatabaseStatistics loadStatisticsFile(String extVPDatabaseName, DatabaseStatistics dbStatistics) {
		File file = new File(extVPDatabaseName + ".stats");
		if (file.exists()) {
			dbStatistics = null;
			try {
			    FileInputStream fis = new FileInputStream(extVPDatabaseName + ".stats");
			    ObjectInputStream ois = new ObjectInputStream(fis);
			    dbStatistics = (DatabaseStatistics) ois.readObject();
			    ois.close();
			    fis.close();
			}catch(Exception e) {
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
	 * Saves a binary file containing database statistics
	 * 
	 * @param extVPDatabaseName name of the ExtVP database name to be used in the name of the file
	 * @param dbStatistics <i>DatabaseStatistics</i> object to be saved
	 */
	public static void saveStatisticsFile(String extVPDatabaseName, DatabaseStatistics dbStatistics) {
		try{
			FileOutputStream fos = new FileOutputStream(extVPDatabaseName + ".stats");
            ObjectOutputStream oos = new ObjectOutputStream(fos);
            oos.writeObject(dbStatistics);
            oos.close();
            fos.close();
		} catch(IOException ioe){
            ioe.printStackTrace();
        }
		logger.info("ExtVP database data saved in " + extVPDatabaseName + ".stats");
	}
	
	/**
	 * Updates <i>sortedTables</i> with the sorting of <i>tables</i>
	 */
	private void sortTables() {
		TableComparator comparator = new TableComparator(tables);
		sortedTables = new TreeMap<String, TableStatistic>(comparator);
		sortedTables.putAll(tables);
	}
	
	/**
	 * Drops <i>tableName</i> from the ExtVP database and updates statistics database size
	 * 
	 * @param tableName
	 * @param spark
	 */
	private void removeTableFromCache(String tableName, SparkSession spark) {
		String query = "drop table " + databaseName + "." + tableName;
		//logger.info("query: " + query);
		spark.sql(query);
		TableStatistic statistic = tables.get(tableName);
		statistic.setTableExists(false);
		logger.info("Table " + tableName + " removed from cache");
		size = size - statistic.getSize();
	}
	
	/**
	 * Frees up space in ExtVP database
	 * 
	 * @param expectedSize maximum size of the ExtVP database after clearing up space
	 * @param spark Spark session
	 */
	public void clearCache(long expectedSize, SparkSession spark) {
		logger.info("Clearing cache...");
		logger.info("Cache size: " + size + "; expected size: " + expectedSize);
		
		sortTables();
		
		NavigableSet<String> tablesKeys = sortedTables.descendingKeySet();
		Iterator<String> iterator = tablesKeys.descendingIterator();
		while (expectedSize < size && iterator.hasNext()) {
			String tableName = iterator.next();
			TableStatistic tableStatistic = tables.get(tableName);
			if (tableStatistic.getTableExists()==true) {
				removeTableFromCache(tableName, spark);
			}
		}
		logger.info("Done clearing cache. Final size: " + size);
	}
	
	/**
	 * Frees up space in ExtVP database
	 * <p>
	 * If the ExtVP database is over the <i>maxSize value</i>, remove tables with lowest selectivity until the database
	 * size is at most <i>expectedSize</i>
	 * </p>
	 * 
	 * @param expectedSize Maximum size of the database after freeing up space
	 * @param maxSize The minimum size the database must have before clearing the cache
	 * @param spark Spark session
	 */
	public void clearCache(long expectedSize, long maxSize, SparkSession spark) {
		if (maxSize<size) {
			clearCache(expectedSize, spark);
		}
	}
}

/**
 * <p>Implements a comparator for the table statistics hashMap. Compares its selectivity. Lower selectivity value is better.
 * Selectivity score of 1 means the extVP table is equal to the VP table</p>
 * 
 *
 */
class TableComparator implements Comparator<String>{
	Map<String, TableStatistic> base;
	
	public TableComparator(Map<String, TableStatistic> base) {
        this.base = base;
    }
	
    /* (non-Javadoc)
     * @see java.util.Comparator#compare(java.lang.Object, java.lang.Object)
     */
    public int compare(String a, String b) {
        TableStatistic firstTableStatistic = base.get(a);
        TableStatistic secondTableStatistic = base.get(b);
    	
    	if (firstTableStatistic.getSelectivity() >= secondTableStatistic.getSelectivity()) {
            return -1;
        } else if (firstTableStatistic.getSelectivity() < secondTableStatistic.getSelectivity()){
            return 1;
        } else {
        	return a.compareTo(b);
        }
    	// returning 0 would merge keys
    }
}

