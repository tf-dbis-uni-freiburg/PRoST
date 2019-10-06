package utils;

import java.util.concurrent.ConcurrentMap;

import org.apache.log4j.Logger;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.Serializer;

//TODO add comments
public class EmergentSchema {
	private static final Logger logger = Logger.getLogger("PRoST");

	// single instance of the statistics
	private static EmergentSchema instance = null;
	private static boolean isSchemaParsed = false;
	private ConcurrentMap<String, String> map = null;

	protected EmergentSchema() {
		// Exists only to defeat instantiation.
	}

	public static EmergentSchema getInstance() {
		if (instance == null) {
			instance = new EmergentSchema();
			return instance;
		}
		if (isSchemaParsed) {
			return instance;
		} else {
			System.err.println("You should invoke readSchema before using the instance.");
			return null;
		}
	}

	public static boolean isUsed() {
		return isSchemaParsed;
	}

	public ConcurrentMap<String, String> readSchema(final String filePath) {
		isSchemaParsed = true;
		// We load the map from the persisted file and use it
		final DB db = DBMaker.fileDB(filePath).make();
		this.map = db.hashMap("map", Serializer.STRING, Serializer.STRING).make();
		//TODO close before quitting the program
		//db.close();
		return this.map;
	}

	public String getTable(final String predicate) {
		return this.map.get(predicate);
	}
}
