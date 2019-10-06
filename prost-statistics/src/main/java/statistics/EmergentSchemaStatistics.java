package statistics;

import java.util.HashSet;

public class EmergentSchemaStatistics {
	private final HashSet<String> properties;
	private final String tableName;

	EmergentSchemaStatistics(final HashSet<String> properties, final String tableName){
		this.properties = properties;
		this.tableName = tableName;
	}

	public boolean containsProperty(final String property) {
		return properties.contains(property);
	}

	public String getTableName() {
		return tableName;
	}
}
