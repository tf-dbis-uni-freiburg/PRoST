package translator.algebraTree;

import com.hp.hpl.jena.shared.PrefixMapping;
import com.hp.hpl.jena.sparql.algebra.Op;
import com.hp.hpl.jena.sparql.algebra.op.OpBGP;
import com.hp.hpl.jena.sparql.algebra.op.OpDistinct;
import com.hp.hpl.jena.sparql.algebra.op.OpFilter;
import com.hp.hpl.jena.sparql.algebra.op.OpLeftJoin;
import com.hp.hpl.jena.sparql.algebra.op.OpProject;
import com.hp.hpl.jena.sparql.algebra.op.OpUnion;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import statistics.DatabaseStatistics;
import utils.Settings;

/**
 * A base operation of an Algebra Tree.
 */
public abstract class Operation {
	public abstract Dataset<Row> computeOperation(final SQLContext sqlContext);

	/**
	 * Builds an algebra with the correct root operation given a jena algebra tree.
	 */
	public static class Builder {
		private final Op jenaAlgebraTree;
		private final DatabaseStatistics statistics;
		private final Settings settings;
		private final PrefixMapping prefixes;

		public Builder(final Op jenaAlgebraTree, final DatabaseStatistics statistics, final Settings settings,
					   final PrefixMapping prefixes) {
			this.jenaAlgebraTree = jenaAlgebraTree;
			this.statistics = statistics;
			this.settings = settings;
			this.prefixes = prefixes;
		}

		public Operation build() throws Exception {
			if (jenaAlgebraTree instanceof OpDistinct) {
				return new Distinct((OpDistinct) jenaAlgebraTree, statistics, settings, prefixes);
			} else if (jenaAlgebraTree instanceof OpProject) {
				return new Projection((OpProject) jenaAlgebraTree, statistics, settings, prefixes);
			} else if (jenaAlgebraTree instanceof OpLeftJoin) {
				final Operation leftOperation =
						new Operation.Builder(((OpLeftJoin) jenaAlgebraTree).getLeft(), this.statistics,
								this.settings, this.prefixes).build();
				final Operation rightOperation =
						new Operation.Builder(((OpLeftJoin) jenaAlgebraTree).getRight(), this.statistics,
								this.settings, this.prefixes).build();
				if (((OpLeftJoin) jenaAlgebraTree).getExprs() != null) {
					final Operation filterOperation = new Filter(rightOperation,
							((OpLeftJoin) jenaAlgebraTree).getExprs().getList());
					return new LeftJoin(leftOperation, filterOperation);
				} else {
					return new LeftJoin(leftOperation, rightOperation);
				}
			} else if (jenaAlgebraTree instanceof OpBGP) {
				return new Bgp((OpBGP) jenaAlgebraTree, statistics, settings, prefixes);
			} else if (jenaAlgebraTree instanceof OpFilter) {
				return new Filter((OpFilter) jenaAlgebraTree, statistics, settings, prefixes);
			} else if (jenaAlgebraTree instanceof OpUnion) {
				return new Union(((OpUnion) jenaAlgebraTree).getLeft(), ((OpUnion) jenaAlgebraTree).getRight(),
						statistics, settings, prefixes);
			} else {
				throw new Exception("Operation not yet implemented");
			}
		}
	}
}
