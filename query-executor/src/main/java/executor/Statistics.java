package executor;

class Statistics {
	private String queryName;

	private long executionTime;
	private long resultsCount;
	private int joinsCount;
	private int broadcastJoinsCount;
	private int sortMergeJoinsCount;

	private int ttNodesCount;
	private int vpNodesCount;
	private int wptNodesCount;
	private int iwptNodesCount;
	private int jwptOuterNodesCount;
	private int jwptInnerNodesCount;
	private int jwptLeftOuterNodesCount;

	private Statistics() {

	}

	public String getQueryName() {
		return queryName;
	}

	public long getExecutionTime() {
		return executionTime;
	}

	public long getResultsCount() {
		return resultsCount;
	}

	public int getJoinsCount() {
		return joinsCount;
	}

	public int getBroadcastJoinsCount() {
		return broadcastJoinsCount;
	}

	public int getSortMergeJoinsCount() {
		return sortMergeJoinsCount;
	}

	public int getTtNodesCount() {
		return ttNodesCount;
	}

	public int getVpNodesCount() {
		return vpNodesCount;
	}

	public int getWptNodesCount() {
		return wptNodesCount;
	}

	public int getIwptNodesCount() {
		return iwptNodesCount;
	}

	public int getJwptOuterNodesCount() {
		return jwptOuterNodesCount;
	}

	public int getJwptInnerNodesCount() {
		return jwptInnerNodesCount;
	}

	public int getJwptLeftOuterNodesCount() {
		return jwptLeftOuterNodesCount;
	}

	/**
	 * Builder for an executor statistics object.
	 */
	public static class Builder {
		private String queryName;

		private long executionTime = 0;
		private long resultsCount = 0;
		private int joinsCount = 0;
		private int broadcastJoinsCount = 0;
		private int sortMergeJoinsCount = 0;

		private int ttNodesCount = 0;
		private int vpNodesCount = 0;
		private int wptNodesCount = 0;
		private int iwptNodesCount = 0;
		private int jwptOuterNodesCount = 0;
		private int jwptInnerNodesCount = 0;
		private int jwptLeftOuterNodesCount = 0;

		Builder(final String queryName) {
			this.queryName = queryName;
		}

		Builder executionTime(final long executionTime) {
			this.executionTime = executionTime;
			return this;
		}

		Builder resultsCount(final long resultsCount) {
			this.resultsCount = resultsCount;
			return this;
		}

		Builder joinsCount(final int joinsCount) {
			this.joinsCount = joinsCount;
			return this;
		}

		Builder broadcastJoinsCount(final int broadcastJoinsCount) {
			this.broadcastJoinsCount = broadcastJoinsCount;
			return this;
		}

		Builder sortMergeJoinsCount(final int sortMergeJoinsCount) {
			this.sortMergeJoinsCount = sortMergeJoinsCount;
			return this;
		}

		Builder ttNodesCount(final int ttNodesCount) {
			this.ttNodesCount = ttNodesCount;
			return this;
		}

		Builder vpNodesCount(final int vpNodesCount) {
			this.vpNodesCount = vpNodesCount;
			return this;
		}

		Builder wptNodesCount(final int wptNodesCount) {
			this.wptNodesCount = wptNodesCount;
			return this;
		}

		Builder iwptNodesCount(final int iwptNodesCount) {
			this.iwptNodesCount = iwptNodesCount;
			return this;
		}

		Builder jwptOuterNodesCount(final int jwptOuterNodesCount) {
			this.jwptOuterNodesCount = jwptOuterNodesCount;
			return this;
		}

		Builder jwptInnerNodesCount(final int jwptInnerNodesCount) {
			this.jwptInnerNodesCount = jwptInnerNodesCount;
			return this;
		}

		Builder jwptLeftOuterNodesCount(final int jwptLeftOuterNodesCount) {
			this.jwptLeftOuterNodesCount = jwptLeftOuterNodesCount;
			return this;
		}

		Statistics build() {
			final Statistics statistics = new Statistics();
			statistics.queryName = this.queryName;
			statistics.executionTime = this.executionTime;
			statistics.resultsCount = this.resultsCount;
			statistics.joinsCount = this.joinsCount;
			statistics.broadcastJoinsCount = this.broadcastJoinsCount;
			statistics.sortMergeJoinsCount = this.sortMergeJoinsCount;
			statistics.ttNodesCount = this.ttNodesCount;
			statistics.vpNodesCount = this.vpNodesCount;
			statistics.wptNodesCount = this.wptNodesCount;
			statistics.iwptNodesCount = this.iwptNodesCount;
			statistics.jwptOuterNodesCount = this.jwptOuterNodesCount;
			statistics.jwptInnerNodesCount = this.jwptInnerNodesCount;
			statistics.jwptLeftOuterNodesCount = this.jwptLeftOuterNodesCount;
			return statistics;
		}
	}
}
