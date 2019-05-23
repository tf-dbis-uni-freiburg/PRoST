package executor;

import joinTree.IWPTNode;
import joinTree.JWPTNode;
import joinTree.JoinNode;
import joinTree.JoinTree;
import joinTree.Node;
import joinTree.TTNode;
import joinTree.VPNode;
import joinTree.WPTNode;

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
	private int jwptNodesCount;
	private int joinNodesCount;

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

	public int getJwptNodesCount() {
		return jwptNodesCount;
	}

	public int getJoinNodesCount() {
		return joinNodesCount;
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
		private int jwptNodesCount = 0;
		private int joinNodesCount = 0;


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

		Builder withCountedNodes(final JoinTree tree) {
			countNodes(tree.getRoot());
			return this;
		}

		private void countNodes(final Node node) {
			if (node instanceof JoinNode) {
				this.joinNodesCount++;
				countNodes(((JoinNode) node).getLeftChild());
				countNodes(((JoinNode) node).getRightChild());
			} else if (node instanceof TTNode) {
				ttNodesCount++;
			} else if (node instanceof VPNode) {
				vpNodesCount++;
			} else if (node instanceof WPTNode) {
				wptNodesCount++;
			} else if (node instanceof IWPTNode) {
				iwptNodesCount++;
			} else if (node instanceof JWPTNode) {
				jwptNodesCount++;
			}
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
			statistics.jwptNodesCount = this.jwptNodesCount;
			statistics.joinNodesCount = this.joinNodesCount;
			return statistics;
		}
	}
}
