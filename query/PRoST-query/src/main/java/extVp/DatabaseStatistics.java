package extVp;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Logger;

public class DatabaseStatistics implements Serializable{
	/**
	 * 
	 */
	private static final long serialVersionUID = -5125939737940488044L;
	private long size;
	private Map<String, TableStatistic> tables;
	
	private static final Logger logger = Logger.getLogger("PRoST");

	
	public DatabaseStatistics () {
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
}
