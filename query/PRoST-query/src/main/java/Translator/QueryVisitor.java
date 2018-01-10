package Translator;

import java.util.List;

import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.sparql.algebra.OpVisitorBase;
import com.hp.hpl.jena.sparql.algebra.op.OpBGP;
import com.hp.hpl.jena.sparql.algebra.op.OpFilter;
import com.hp.hpl.jena.sparql.algebra.op.OpProject;
import com.hp.hpl.jena.sparql.core.Var;
import com.hp.hpl.jena.sparql.expr.Expr;

import Filter.FilterVisitor;

public class QueryVisitor extends OpVisitorBase {

	private List<Triple> triple_patterns;
	private List<Var> variables;
	private String filter;

	public List<Var> getVariables() {
		return variables;
	}

	public List<Triple> getTriple_patterns() {
		return triple_patterns;
	}

	public void visit(OpBGP opBGP) {
		triple_patterns = opBGP.getPattern().getList();
		System.out.println(triple_patterns);
	}

	public void visit(OpFilter opFilter) {
		filter = opFilter.toString();
		FilterVisitor filterV = new FilterVisitor();
    	for (Expr e : opFilter.getExprs()) {
			e.visit(filterV);
			filter = filterV.getSQLFilter();
			System.out.println("AAAB");
			System.out.println("Filter: " + filter);
			System.out.println(e);
			System.out.println(e.isFunction());
		}
	}

	public void visit(OpProject opProject) {
		variables = opProject.getVars();
	}

	public String getFilter() {
		return filter;
	}

}
