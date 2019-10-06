package executor;

import translator.algebraTree.bgpTree.IWPTNode;
import translator.algebraTree.bgpTree.JWPTNode;
import translator.algebraTree.bgpTree.JoinNode;
import translator.algebraTree.bgpTree.BgpNode;
import translator.algebraTree.bgpTree.TTNode;
import translator.algebraTree.bgpTree.VPNode;
import translator.algebraTree.bgpTree.WPTNode;
import org.apache.spark.sql.execution.QueryExecution;
import translator.Query;
import translator.algebraTree.Bgp;
import translator.algebraTree.CompoundOperation;
import translator.algebraTree.Operation;
import translator.algebraTree.SimpleOperation;

/**
 * Execution statistics. Used to create the benchmark file.
 */
final class Statistics {
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

	private String logicalPlan;
	private String analyzedPlan;
	private String optimizedPlan;
	private String executedPlan;

	private Statistics() {

	}

	String getQueryName() {
		return queryName;
	}

	long getExecutionTime() {
		return executionTime;
	}

	long getResultsCount() {
		return resultsCount;
	}

	int getJoinsCount() {
		return joinsCount;
	}

	int getBroadcastJoinsCount() {
		return broadcastJoinsCount;
	}

	int getSortMergeJoinsCount() {
		return sortMergeJoinsCount;
	}

	int getTtNodesCount() {
		return ttNodesCount;
	}

	int getVpNodesCount() {
		return vpNodesCount;
	}

	int getWptNodesCount() {
		return wptNodesCount;
	}

	int getIwptNodesCount() {
		return iwptNodesCount;
	}

	int getJwptNodesCount() {
		return jwptNodesCount;
	}

	int getJoinNodesCount() {
		return joinNodesCount;
	}

	public String getLogicalPlan() {
		return logicalPlan;
	}

	public String getAnalyzedPlan() {
		return analyzedPlan;
	}

	public String getOptimizedPlan() {
		return optimizedPlan;
	}

	public String getExecutedPlan() {
		return executedPlan;
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

		private String logicalPlan;
		private String analyzedPlan;
		private String optimizedPlan;
		private String executedPlan;

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

		Builder withCountedNodes(final Query tree) {
			countNodes(tree.getAlgebraTree());
			return this;
		}

		Builder withPlansAsStrings(final QueryExecution sparkPlan) {
			this.logicalPlan = sparkPlan.logical().toString();
			this.analyzedPlan = sparkPlan.analyzed().toString();
			this.optimizedPlan = sparkPlan.optimizedPlan().toString();
			this.executedPlan = sparkPlan.executedPlan().toString();
			return this;
		}

		private void countNodes(final BgpNode node) {
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

		private void countNodes(final Operation node) {
			if (node instanceof SimpleOperation) {
				countNodes(((SimpleOperation) node).getSubOperation());
			} else if (node instanceof CompoundOperation) {
				countNodes(((CompoundOperation) node).getLeftSubOperation());
				countNodes(((CompoundOperation) node).getRightSubOperation());
			} else if (node instanceof Bgp) {
				countNodes(((Bgp) node).getRootNode());
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
			statistics.logicalPlan = this.logicalPlan;
			statistics.analyzedPlan = this.analyzedPlan;
			statistics.optimizedPlan = this.optimizedPlan;
			statistics.executedPlan = this.executedPlan;
			return statistics;
		}
	}
}
