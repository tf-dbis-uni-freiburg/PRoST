package translator;

import java.util.ArrayList;
import java.util.List;

import com.hp.hpl.jena.shared.PrefixMapping;
import com.hp.hpl.jena.sparql.algebra.OpVisitorBase;
import com.hp.hpl.jena.sparql.algebra.op.OpBGP;
import com.hp.hpl.jena.sparql.algebra.op.OpFilter;
import com.hp.hpl.jena.sparql.algebra.op.OpLeftJoin;
import com.hp.hpl.jena.sparql.algebra.op.OpProject;
import com.hp.hpl.jena.sparql.core.Var;
import com.hp.hpl.jena.sparql.expr.Expr;

public class QueryVisitor extends OpVisitorBase {

	private QueryTree mainQueryTree;
	private List<QueryTree> optionalQueryTrees;
	private List<Var> projectionVariables;
	private PrefixMapping prefixes;

	public QueryVisitor(final PrefixMapping prefixes) {
		super();
		this.prefixes = prefixes;
		this.optionalQueryTrees = new ArrayList<>();
	}

	public List<Var> getProjectionVariables() {
		return this.projectionVariables;
	}

	public List<QueryTree> getOptionalQueryTrees() {
		return this.optionalQueryTrees;
	}

	public QueryTree getMainQueryTree() {
		return this.mainQueryTree;
	}

	public void visit(final OpBGP opBGP) {
		this.mainQueryTree = new QueryTree(opBGP.getPattern().getList());
	}

	public void visit(final OpBGP opBGP, final boolean isOptional, final String filter) {
		if (!isOptional) {
			// main join tree triples
			this.mainQueryTree = new QueryTree(opBGP.getPattern().getList());
		} else {
			// optional triples
			this.optionalQueryTrees.add(new QueryTree(opBGP.getPattern().getList(), filter));
		}
	}

	public void visit(final OpLeftJoin opLeftJoin) {
		if (opLeftJoin.getLeft() instanceof OpBGP) {
			this.visit((OpBGP) opLeftJoin.getLeft(), false, null);
		}
		// set optional triples
		if (opLeftJoin.getRight() instanceof OpBGP) {
			// filter expression for the optional
			if (opLeftJoin.getExprs() != null) {
				final FilterVisitor filterVisitor = new FilterVisitor(this.prefixes);
				for (final Expr e : opLeftJoin.getExprs()) {
					e.visit(filterVisitor);
				}
				final String optionalFilter = filterVisitor.getSQLFilter();
				this.visit((OpBGP) opLeftJoin.getRight(), true, optionalFilter);
			} else {
				this.visit((OpBGP) opLeftJoin.getRight(), true, null);
			}
		}
	}

	public void visit(final OpFilter opFilter) {
		final FilterVisitor filterVisitor = new FilterVisitor(this.prefixes);
		for (final Expr e : opFilter.getExprs()) {
			e.visit(filterVisitor);
		}
		this.mainQueryTree.setFilter(filterVisitor.getSQLFilter());
	}

	public void visit(final OpProject opProject) {
		this.projectionVariables = opProject.getVars();
	}
}
