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

  private List<Triple> triple_patterns;
  private List<Var> variables;
  private String filter;
  private PrefixMapping prefixes;
  private boolean prefixesActive;

  public QueryVisitor(PrefixMapping prefixes) {
    super();
    this.prefixes = prefixes;
    this.prefixesActive = Stats.getInstance().arePrefixesActive();
  }

  public List<Var> getVariables() {
    return variables;
  }

  public List<Triple> getTriple_patterns() {
    return triple_patterns;
  }

  public void visit(OpBGP opBGP) {
    triple_patterns = opBGP.getPattern().getList();
  }

  public void visit(OpFilter opFilter) {

    FilterVisitor filterVisitor = new FilterVisitor(this.prefixes);
    for (Expr e : opFilter.getExprs()) {
      e.visit(filterVisitor);
      filter = filterVisitor.getSQLFilter();
    }
  }

  public void visit(OpProject opProject) {
    variables = opProject.getVars();
  }

  public String getFilter() {
    return filter;
  }

}
