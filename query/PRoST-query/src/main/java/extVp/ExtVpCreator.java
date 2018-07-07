package extVp;

import java.util.List;
import java.util.ListIterator;

import org.apache.log4j.Logger;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalog.Catalog;

import com.hp.hpl.jena.graph.Triple;


import com.hp.hpl.jena.shared.PrefixMapping;

import translator.Stats;

public class ExtVpCreator {
	
	private static final Logger logger = Logger.getLogger("PRoST");
	
	public enum extVPType {
	    SS, SO, OS, OO
	}
	
	public void createExtVPTable(String predicate1, String predicate2, extVPType extVPType, SparkSession spark) {
		//TODO Check if table exists 
		
		String vp1TableName = "vp_" + Stats.getInstance().findTableName(predicate1);
		String vp2TableName = "vp_" + Stats.getInstance().findTableName(predicate2);
		
		
		String extVpTableName = "extVP_" + extVPType.toString() + "_" + getValidHiveName(predicate1) + "__" + getValidHiveName(predicate2);
		
		if (!spark.catalog().tableExists(extVpTableName)) {
			String createTableQuery = String.format("create table if not exists %1$s(s string, o string) stored as parquet", extVpTableName);
			
			logger.info(createTableQuery);
			spark.sql(createTableQuery);
			logger.info("table created");
					
			
			String queryOnCommand = "";	
			switch (extVPType) {
				case SO:
					queryOnCommand = String.format("%1$s.s=%2$s.o", vp1TableName, vp2TableName);
					break;
				case OS:
					queryOnCommand = String.format("%1$s.o=%2$s.s", vp1TableName, vp2TableName);
					break;
				case OO: 
					queryOnCommand = String.format("%1$s.o=%2$s.o", vp1TableName, vp2TableName);
					break;
				case SS:
				default:
					queryOnCommand = String.format("%1$s.s=%2$s.s", vp1TableName, vp2TableName);
					break;
			}
				
			String insertDataQuery = String.format("insert overwrite table %1$s select %2$s.s as s, %2$s.o as o from %2$s left semi join %3$s on %4$s", extVpTableName, vp1TableName, vp2TableName, queryOnCommand);
			
			logger.info(insertDataQuery);
			spark.sql(insertDataQuery);
			logger.info("data inserted");
		} else {
			logger.info("Table " + extVpTableName +"already exists");
		}
	}
	
	public void createExtVPFromTriples(List<Triple> triples, PrefixMapping prefixes, SparkSession spark){
		logger.info("triples list size: " + triples.size());
		
		for(ListIterator<Triple> outerTriplesListIterator = triples.listIterator(); outerTriplesListIterator.hasNext() ; ) {
		    Triple outerTriple = outerTriplesListIterator.next();
		    String outerSubject = outerTriple.getSubject().toString(prefixes);
		    String outerPredicate = outerTriple.getPredicate().toString(prefixes);
		    String outerObject = outerTriple.getObject().toString(prefixes);
		    
		    for(ListIterator<Triple> innerTriplesListIterator = triples.listIterator(outerTriplesListIterator.nextIndex()); innerTriplesListIterator.hasNext(); ) {
		    	Triple innerTriple = innerTriplesListIterator.next();
		    	String innerSubject = innerTriple.getSubject().toString(prefixes);
			    String innerPredicate = innerTriple.getPredicate().toString(prefixes);
			    String innerObject = innerTriple.getObject().toString(prefixes);
			    
			    if (outerSubject.equals(innerSubject)) {
			    	//SS
			    	logger.info("creating SS extVP table");
			    	createExtVPTable(outerPredicate, innerPredicate, extVPType.SS, spark);
				    createExtVPTable(innerPredicate, outerPredicate, extVPType.SS, spark); 
			    }
			    else if (outerObject.equals(innerObject)) {
			    	//OO
			    	logger.info("creating OO extVP table");
			    	createExtVPTable(outerPredicate, innerPredicate, extVPType.OO, spark);
				    createExtVPTable(innerPredicate, outerPredicate, extVPType.OO, spark); 
			    }
			    else if (outerObject.equals(innerSubject)) {
			    	//OS
			    	logger.info("creating OS-SO extVP table");
			    	createExtVPTable(outerPredicate, innerPredicate, extVPType.OS, spark);
				    createExtVPTable(innerPredicate, outerPredicate, extVPType.SO, spark); 
			    }
			    else if (outerSubject.equals(innerObject)) {
			    	//SO
			    	logger.info("creating SO-OS extVP table");
			    	createExtVPTable(outerPredicate, innerPredicate, extVPType.SO, spark);
				    createExtVPTable(innerPredicate, outerPredicate, extVPType.OS, spark); 
			    }
		    }
		}
	}
	
	protected String getValidHiveName(String columnName) {
		return columnName.replaceAll("[<>]", "").trim().replaceAll("[[^\\w]+]", "_");
	}
}
		
		

