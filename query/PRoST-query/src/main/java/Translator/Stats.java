package Translator;


import java.io.FileInputStream;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.HashMap;

import javax.realtime.RealtimeThread;

import org.apache.log4j.Logger;

import JoinTree.ProtobufStats;
import run.Main;
import JoinTree.ProtobufStats.Table;
import Executor.Utils;

/**
 * This class is used to parse statistics from a Protobuf file and 
 * it exposes methods to retrieve singular entries.
 * TODO: implement whole graph statistics
 * @author Matteo Cossu
 *
 */
public class Stats {
	String fileName;
	private HashMap<String, JoinTree.ProtobufStats.Table> tableStats;
	private HashMap<String, Integer> tableSize;
	private HashMap<String, Integer> tableDistinctSubjects;
	public String [] tableNames;

	
	private static final Logger logger = Logger.getLogger(Main.class);
	
	public Stats(String fileName){
		this.fileName = fileName;
		tableSize  = new HashMap<String, Integer>();
		tableDistinctSubjects = new HashMap<String, Integer>();
		tableStats = new HashMap<String, Table>();
		this.parseStats();
	}
	
	public void parseStats() {
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
		
		tableNames = new String[graph.getTablesCount()];
		int i = 0;
		for(Table table : graph.getTablesList()){
			tableNames[i] = table.getName();
			tableStats.put(tableNames[i], table);
			tableSize.put(tableNames[i], table.getSize());
			tableDistinctSubjects.put(tableNames[i], table.getDistinctSubjects());
			i++;
		}
		logger.info("Statistics correctly parsed");
	}
	
	public int getTableSize(String table){
		if(!tableSize.containsKey(table)) return -1;
		return tableSize.get(table);
	}
	
	public int getTableDistinctSubjects(String table){
		if(!tableDistinctSubjects.containsKey(table)) return -1;
		return tableDistinctSubjects.get(table);
	}
	
	public Table getTableStats(String table){
		if(!tableStats.containsKey(table)) return null;
		return tableStats.get(table);
	}
	
	
	/*
	 * This method returns the same name for the table (VP) or column (PT)
	 * that was used in the loading phase.
	 * Returns the name from an exact match or from a partial one,
	 * if a prefix was used in loading or in the query.
	 * Return null if there is no match
	 */
	public String findTableName(String tableName) {
	  String cleanedTableName = Utils.toMetastoreName(tableName);
	  
	  for(String realTableName: this.tableNames) {
	    boolean exactMatch = realTableName.equals(cleanedTableName);
	    // one of the two is prefixed the other not
	    boolean partialMatch1 = realTableName.endsWith(cleanedTableName);
	    boolean partialMatch2 = cleanedTableName.endsWith(realTableName);
	    
	    // if there is a match, return the correct table name
	    if(exactMatch || partialMatch1 || partialMatch2)
	      return realTableName;
	  }
	  
	  // not found
	  return null;
	}
	

}
