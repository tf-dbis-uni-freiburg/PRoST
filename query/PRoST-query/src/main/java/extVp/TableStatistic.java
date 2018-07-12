package extVp;

import java.io.Serializable;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;

import org.apache.log4j.Logger;

import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.shared.PrefixMapping;

import JoinTree.TriplePattern;

public class TableStatistic implements Serializable{
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	//private static final Logger logger = Logger.getLogger("PRoST");
	private String tableName;
	private float selectivity;
	private int size;
	
	public TableStatistic (String tableName, float selectivity, int size) {
		this.tableName = tableName;
		this.selectivity = selectivity;
		this.size = size;
	}
	
	public String getTableName() {
		return tableName;
	}
	
	public float getSelectivity() {
		return selectivity;
	}
	
	public int getSize() {
		return size;
	}
	
	public static String selectExtVPTable(Triple currentTriple, List<Triple> triples, Map<String, TableStatistic> statistics, PrefixMapping prefixes) {
		String currentSubject = currentTriple.getSubject().toString(prefixes);
		String currentObject = currentTriple.getObject().toString(prefixes);
		String currentPredicate = currentTriple.getPredicate().toString(prefixes);
		
		String selectedTableName = new String();
		float currentTableScore = 0;
		
		ListIterator<Triple> triplesIterator = triples.listIterator();
		while (triplesIterator.hasNext()) {
			Triple outerTriple = triplesIterator.next();
			if (outerTriple!=currentTriple) {
				String outerSubject = outerTriple.getSubject().toString(prefixes);
				String outerObject = outerTriple.getObject().toString(prefixes);
				String outerPredicate = outerTriple.getPredicate().toString(prefixes);
				if (currentSubject.equals(outerSubject)) {
					//SS
					String tableName = ExtVpCreator.getExtVPTableName(currentPredicate, outerPredicate, ExtVpCreator.extVPType.SS);
					TableStatistic tableStatistic = statistics.get(tableName);
					if (tableStatistic.getSelectivity()>currentTableScore) {
						selectedTableName = tableName;
					}
				}
				if (currentObject.equals(outerObject)) {
					//OO
					
					String tableName = ExtVpCreator.getExtVPTableName(currentPredicate, outerPredicate, ExtVpCreator.extVPType.OO);
					TableStatistic tableStatistic = statistics.get(tableName);
					if (tableStatistic.getSelectivity()>currentTableScore) {
						selectedTableName = tableName;
					}
				}
				if (currentSubject.equals(outerObject)) {
					//SO
					
					String tableName = ExtVpCreator.getExtVPTableName(currentPredicate, outerPredicate, ExtVpCreator.extVPType.SO);
					TableStatistic tableStatistic = statistics.get(tableName);
					if (tableStatistic.getSelectivity()>currentTableScore) {
						selectedTableName = tableName;
					}
				}
				if (currentObject.equals(outerSubject)) {
					//OS
					
					String tableName = ExtVpCreator.getExtVPTableName(currentPredicate, outerPredicate, ExtVpCreator.extVPType.OS);
					TableStatistic tableStatistic = statistics.get(tableName);
					if (tableStatistic.getSelectivity()>currentTableScore) {
						selectedTableName = tableName;
					}
				}
			}
		}	
		return selectedTableName;
	}
}
