package translator;

import java.util.List;

import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.shared.PrefixMapping;
import com.hp.hpl.jena.sparql.algebra.OpVisitorBase;
import com.hp.hpl.jena.sparql.algebra.op.OpBGP;
import com.hp.hpl.jena.sparql.algebra.op.OpFilter;
import com.hp.hpl.jena.sparql.algebra.op.OpProject;
import com.hp.hpl.jena.sparql.core.Var;
import com.hp.hpl.jena.sparql.expr.Expr;

public class QueryVisitor extends OpVisitorBase {

  private List<Triple> triple;
  private List<Var> variables;
  private String filter;
  private PrefixMapping prefixes;

  public QueryVisitor(PrefixMapping prefixes) {
    super();
    this.prefixes = prefixes;
  }

  public List<Var> getVariables() {
    return this.variables;
  }

  public List<Triple> getTriples() {
    return this.triple;
  }

  public void visit(OpBGP opBGP) {
	  this.triple = opBGP.getPattern().getList();
  }

  public void visit(OpFilter opFilter) {
	// TODO filter visitor
    FilterVisitor filterVisitor = new FilterVisitor(this.prefixes);
    for (Expr e : opFilter.getExprs()) {
      e.visit(filterVisitor);
      //filter = filterVisitor.getSQLFilter();
    }
  }

  public void visit(OpProject opProject) {
    this.variables = opProject.getVars();
  }

  public String getFilter() {
    return filter;
  }

}
