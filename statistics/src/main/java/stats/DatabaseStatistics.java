package stats;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.lang.reflect.Type;
import java.util.HashMap;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;

/**
 * Handles statistical information about a whole database.
 */
public class DatabaseStatistics {
	private String databaseName;
	private HashMap<String, PropertyStatistics> properties;

	public DatabaseStatistics() {

	}

	public DatabaseStatistics(final String databaseName) {
		this.databaseName = databaseName;
		this.properties = new HashMap<>();
	}

	public void saveToFile(final String path) {

		final Gson gson = new GsonBuilder().setPrettyPrinting().create();
		final String json = gson.toJson(this);
		try {
			final FileWriter writer = new FileWriter(path);
			writer.write(json);
			writer.close();
		} catch (final IOException e) {
			e.printStackTrace();
		}
	}

	public void loadFromFile(final String path) {
		final Gson gson = new Gson();
		try {
			final BufferedReader br = new BufferedReader(new FileReader(path));
			final Type type = new TypeToken<HashMap<String, PropertyStatistics>>() {
			}.getType();


			final DatabaseStatistics ds2 = gson.fromJson(br, type);
			this.databaseName = ds2.databaseName;
			this.properties = ds2.properties;

		} catch (final IOException e) {
			e.printStackTrace();
		}
	}

	public HashMap<String, PropertyStatistics> getProperties() {
		return properties;
	}

}
