package extVp;

import java.util.Comparator;
import java.util.Map;

/**
 * <p>
 * Implements a comparator for the table statistics hashMap. Compares its selectivity.
 * Lower selectivity value is better. Selectivity score of 1 means the extVP table is
 * equal to the VP table
 * </p>
 *
 *
 */
public class TableComparator implements Comparator<String> {
	private final Map<String, TableStatistic> base;

	public TableComparator(final Map<String, TableStatistic> base) {
		this.base = base;
	}

	/*
	 * (non-Javadoc)
	 *
	 * @see java.util.Comparator#compare(java.lang.Object, java.lang.Object)
	 */
	@Override
	public int compare(final String table1, final String table2) {
		final TableStatistic firstTableStatistic = base.get(table1);
		final TableStatistic secondTableStatistic = base.get(table2);

		if (firstTableStatistic.getSelectivity() >= secondTableStatistic.getSelectivity()) {
			return -1;
		} else if (firstTableStatistic.getSelectivity() < secondTableStatistic.getSelectivity()) {
			return 1;
		} else {
			return table1.compareTo(table2);
		}
		// returning 0 would merge keys
	}
}