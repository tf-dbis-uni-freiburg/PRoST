import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

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

	public void addTime(int time) {
		this.times.add(time);
	}

	public double getAverage() {
		return average;
	}

	public double getQ1() {
		return q1;
	}

	public double getQ3() {
		return q3;
	}

	public double getUpperFence() {
		return upperFence;
	}

	public double getMedian() {
		return median;
	}

	public int getNumberOfOutliersTrimmed() {
		return numberOfOutliersTrimmed;
	}
}
