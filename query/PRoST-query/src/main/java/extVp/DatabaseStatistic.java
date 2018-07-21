package extVp;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

public class DatabaseStatistic implements Serializable{
	/**
	 * 
	 */
	private static final long serialVersionUID = -5125939737940488044L;
	private long size;
	private Map<String, TableStatistic> tables;

	
	public DatabaseStatistic () {
		this.size = 0;
		this.tables = new HashMap<String, TableStatistic>();
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
}
