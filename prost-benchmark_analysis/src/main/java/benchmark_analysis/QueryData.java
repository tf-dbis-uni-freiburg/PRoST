package benchmark_analysis;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Execution data of a single input query. May contain information about multiple executions of the query.
 */
public class QueryData {
	private List<Integer> times = new ArrayList<>();
	private double average;
	private double iQR;
	private double q1;
	private double q3;
	private double upperFence;
	private double median;
	private double standardDeviation;
	private int numberOfOutliersTrimmed;

	private long resultsCount;
	private int joinsCount;
	private int broadcastJoinsCount;
	private int sortMergeJoinsCount;
	private int joinNodesCount;
	private int ttNodesCount;
	private int vpNodesCount;
	private int wptNodesCount;
	private int iwptNodesCount;
	private int jwptNodesCount;

	/**
	 * Merge multiple results of the same query by computing the average time. Outliers are remove using the
	 * interquartile range.
	 */
	public void compute() {
		Collections.sort(times);
		computeMedian();
		computeInterquartile();
		filterOutliers();
		computeAverage();
	}

	private void computeAverage() {
		float sum = 0;
		for (final Integer value : this.times) {
			sum += value;
		}
		this.average = sum / this.times.size();
	}

	private void computeMedian() {
		this.median = computeSubListMedian(this.times);
	}

	private double computeSubListMedian(final List<Integer> subList) {
		final int size = subList.size();
		if (size % 2 == 0) {
			return ((double) subList.get(size / 2) + (double) subList.get(size / 2 - 1)) / 2;
		} else {
			return subList.get(size / 2);
		}
	}

	private void computeInterquartile() {
		final int size = this.times.size();
		final int middlePoint = size / 2;
		this.q1 = computeSubListMedian(times.subList(0, middlePoint));
		this.q3 = computeSubListMedian(times.subList(middlePoint, size));
		this.iQR = this.q3 - this.q1;
		this.upperFence = this.q3 + this.iQR * 1.5;
	}

	private void filterOutliers() {
		final int originalSize = times.size();
		this.times.removeIf(n -> n >= this.upperFence);
		this.numberOfOutliersTrimmed = originalSize - times.size();
	}

	void addTime(final int time) {
		this.times.add(time);
	}

	double getAverage() {
		return average;
	}

	double getQ1() {
		return q1;
	}

	double getQ3() {
		return q3;
	}

	double getUpperFence() {
		return upperFence;
	}

	double getMedian() {
		return median;
	}

	int getNumberOfOutliersTrimmed() {
		return numberOfOutliersTrimmed;
	}

	long getResultsCount() {
		return resultsCount;
	}

	void setResultsCount(final long resultsCount) {
		this.resultsCount = resultsCount;
	}

	int getJoinsCount() {
		return joinsCount;
	}

	void setJoinsCount(final int joinsCount) {
		this.joinsCount = joinsCount;
	}

	int getBroadcastJoinsCount() {
		return broadcastJoinsCount;
	}

	void setBroadcastJoinsCount(final int broadcastJoinsCount) {
		this.broadcastJoinsCount = broadcastJoinsCount;
	}

	int getSortMergeJoinsCount() {
		return sortMergeJoinsCount;
	}

	void setSortMergeJoinsCount(final int sortMergeJoinsCount) {
		this.sortMergeJoinsCount = sortMergeJoinsCount;
	}

	int getJoinNodesCount() {
		return joinNodesCount;
	}

	void setJoinNodesCount(final int joinNodesCount) {
		this.joinNodesCount = joinNodesCount;
	}

	int getTtNodesCount() {
		return ttNodesCount;
	}

	void setTtNodesCount(final int ttNodesCount) {
		this.ttNodesCount = ttNodesCount;
	}

	int getVpNodesCount() {
		return vpNodesCount;
	}

	void setVpNodesCount(final int vpNodesCount) {
		this.vpNodesCount = vpNodesCount;
	}

	int getWptNodesCount() {
		return wptNodesCount;
	}

	void setWptNodesCount(final int wptNodesCount) {
		this.wptNodesCount = wptNodesCount;
	}

	int getIwptNodesCount() {
		return iwptNodesCount;
	}

	void setIwptNodesCount(final int iwptNodesCount) {
		this.iwptNodesCount = iwptNodesCount;
	}

	int getJwptNodesCount() {
		return jwptNodesCount;
	}

	void setJwptNodesCount(final int jwptNodesCount) {
		this.jwptNodesCount = jwptNodesCount;
	}
}

