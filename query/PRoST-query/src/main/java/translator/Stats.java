package translator;

import java.io.FileInputStream;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.HashMap;

import org.apache.log4j.Logger;
import JoinTree.ProtobufStats;
import Executor.Utils;

/**
 * This class is used to parse statistics from a Protobuf file and it exposes
 * methods to retrieve singular entries.
 * 
 * TODO: implement whole graph statistics
 * 
 * @author Matteo Cossu
 *
 */
public class Stats {

	private static final Logger logger = Logger.getLogger("PRoST");

	// single instance of the statistics
	private static Stats instance = null;

	private static boolean areStatsParsed = false;

	private String fileName;
	private HashMap<String, JoinTree.ProtobufStats.TableStats> tableStats;
	private HashMap<String, Integer> tableSize;
	private HashMap<String, Integer> tableDistinctSubjects;
	
	/**
	 * Are prefixes used in the data set. The data will be stored as it comes, if it
	 * comes with full URIs, it will be stored with full URIs. If it comes prefixed,
	 * prefixed version of the data will be stored. NO substitution will be done.
	 * This property indicates if the data is stored with full URIs or with its
	 * prefixed version.
	 */
	private boolean arePrefixesActive;
	private String[] tableNames;

	protected Stats() {
		// Exists only to defeat instantiation.
	}

	public static Stats getInstance() {
		if (instance == null) {
			instance = new Stats();
			instance.tableSize = new HashMap<String, Integer>();
			instance.tableDistinctSubjects = new HashMap<String, Integer>();
			instance.tableStats = new HashMap<String, JoinTree.ProtobufStats.TableStats>();
			return instance;
		}
		if (areStatsParsed) {
			return instance;
		} else {
			System.err.println("You should invoke parseStats before using the instance.");
			return null;
		}
	}

	public void parseStats(String fileName) {
		if (areStatsParsed)
			return;
		else
			areStatsParsed = true;

		ProtobufStats.Graph graph;
		try {
			graph = ProtobufStats.Graph.parseFrom(new FileInputStream(fileName));
		} catch (FileNotFoundException e) {
			logger.error("Statistics input File Not Found");
			return;
		} catch (IOException e) {
			e.printStackTrace();
			return;
		}

		this.tableNames = new String[graph.getTablesCount()];
		this.arePrefixesActive = graph.getArePrefixesActive();
		int i = 0;
		for (ProtobufStats.TableStats table : graph.getTablesList()) {
			tableNames[i] = table.getName();
			tableStats.put(tableNames[i], table);
			tableSize.put(tableNames[i], table.getSize());
			tableDistinctSubjects.put(tableNames[i], table.getDistinctSubjects());
			i++;
		}
		logger.info("Statistics correctly parsed");
	}

	public int getTableSize(String table) {
		table = this.findTableName(table);
		if (table == null)
			return -1;
		return tableSize.get(table);
	}

	public int getTableDistinctSubjects(String table) {
		table = this.findTableName(table);
		if (table == null)
			return -1;
		return tableDistinctSubjects.get(table);
	}

	public ProtobufStats.TableStats getTableStats(String table) {
		table = this.findTableName(table);
		if (table == null)
			return null;
		return tableStats.get(table);
	}

	public boolean isTableComplex(String table) {
		String cleanedTableName = this.findTableName(table);
		return this.getTableSize(cleanedTableName) != this.getTableDistinctSubjects(cleanedTableName);
	}

	/*
	 * This method returns the same name for the table (VP) or column (PT) that was
	 * used in the loading phase. Returns the name from an exact match or from a
	 * partial one, if a prefix was used in loading or in the query. Return null if
	 * there is no match
	 */
	public String findTableName(String tableName) {
		String cleanedTableName = Utils.toMetastoreName(tableName).toLowerCase();

		if (cleanedTableName.contains("_")) {
			int lstIdx = cleanedTableName.lastIndexOf("_");
			cleanedTableName = cleanedTableName.substring(lstIdx);
		}

		for (String realTableName : this.tableNames) {

			boolean exactMatch = realTableName.equalsIgnoreCase(cleanedTableName);
			// one of the two is prefixed the other not
			boolean partialMatch1 = realTableName.toLowerCase().endsWith(cleanedTableName);
			boolean partialMatch2 = cleanedTableName.endsWith(realTableName.toLowerCase());

			// if there is a match, return the correct table name
			if (exactMatch || partialMatch1 || partialMatch2)
				return realTableName;
		}
		// not found
		return null;
	}

	/*
	 * Return true if prefixes are used in the data set.
	 */
	public boolean arePrefixesActive() {
		return arePrefixesActive;
	}

}
