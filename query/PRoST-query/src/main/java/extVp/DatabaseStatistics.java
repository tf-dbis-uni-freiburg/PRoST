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

public class DatabaseStatistics implements Serializable{
	/**
	 * 
	 */
	private static final long serialVersionUID = -5125939737940488044L;
	private long size;
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
	
	public void setSize(long size) {
		this.size = size;
	}
	
	public long getSize() {
		return this.size;
	}
	
	public Map<String, TableStatistic> getTables(){
		return tables;
	}
	
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
		logger.info("Serialized HashMap data is saved in " + extVPDatabaseName + ".stats");
	}
	
	private void sortTables() {
		TableComparator comparator = new TableComparator(tables);
		sortedTables = new TreeMap<String, TableStatistic>(comparator);
		sortedTables.putAll(tables);
	}
	
	private void removeTableFromCache(String tableName, SparkSession spark) {
		String query = "drop table " + databaseName + "." + tableName;
		logger.info("query: " + query);
		spark.sql(query);
		TableStatistic statistic = tables.get(tableName);
		statistic.setTableExists(false);
		logger.info("Table " + tableName + " removed from cache");
		size = size - statistic.getSize();
	}
	
	public void clearCache(long excpectedSize, SparkSession spark) {
		logger.info("Clearing cache...");
		logger.info("Cache size: " + size + "; expected size: " + excpectedSize);
		
		sortTables();
		
		NavigableSet<String> tablesKeys = sortedTables.descendingKeySet();
		Iterator<String> iterator = tablesKeys.descendingIterator();
		while (excpectedSize < size && iterator.hasNext()) {
			String tableName = iterator.next();
			TableStatistic tableStatistic = tables.get(tableName);
			if (tableStatistic.getTableExists()==true) {
				removeTableFromCache(tableName, spark);
			}
		}
		logger.info("Done clearing cache. Final size: " + size);
	}
}

class TableComparator implements Comparator<String>{
	Map<String, TableStatistic> base;
	
	public TableComparator(Map<String, TableStatistic> base) {
        this.base = base;
    }
	
	// Note: this comparator imposes orderings that are inconsistent with
    // equals.
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

