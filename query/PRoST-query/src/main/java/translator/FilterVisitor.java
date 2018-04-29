package translator;

import com.hp.hpl.jena.shared.PrefixMapping;
import com.hp.hpl.jena.sparql.expr.*;

public class FilterVisitor extends ExprVisitorBase {
	
    StringBuilder builder = new StringBuilder();

    private PrefixMapping prefixes;

    public FilterVisitor(PrefixMapping prefixes) {
        super();
        this.prefixes = prefixes;
    }

    @Override
    public void visit(ExprFunction0 func) {
        super.visit(func);
    }

    @Override
    public void visit(ExprFunction1 func) {
        builder.append(" ");
        // variable in the beginning
        if (func instanceof E_Bound || func instanceof E_IsIRI) {
            func.getArg().visit(this);
            builder.append(" ");
            builder.append(ToSQLExp.getSqlExpr(func));
            // variable in middle
        } else if (func instanceof E_DateTimeDay) {
            builder.append(ToSQLExp.getSqlExpr(func));
            func.getArg().visit(this);
            builder.append(")");
            // variable at the end
        } else if (func instanceof E_LogicalNot) {
            builder.append(ToSQLExp.getSqlExpr(func));
            func.getArg().visit(this);
        }
        builder.append(" ");
    }

    @Override
    public void visit(ExprFunction2 func) {
        func.getArg1().visit(this);
        builder.append(" ");
        builder.append(ToSQLExp.getSqlExpr(func));
        builder.append(" ");
        func.getArg2().visit(this);
    }

    @Override
    public void visit(ExprFunction3 func) {
        super.visit(func);
    }

    @Override
    public void visit(ExprFunctionN func) {
        super.visit(func);
    }

    @Override
    public void visit(ExprFunctionOp op) {
        super.visit(op);
    }

    @Override
    public void visit(NodeValue nv) {
        // for literals and URIs
        if (nv.isIRI()) {
            if (Stats.getInstance().arePrefixesActive()) {
                // use the short form
                builder.append(this.prefixes.shortForm(nv.asString()));
            } else {
                builder.append("<" + nv.asString() + ">");
            }
        } else {
            builder.append(nv.asString());
        }
    }

    @Override
    public void visit(ExprVar var) {
        builder.append(var.getVarName());
    }

    @Override
    public void visit(ExprAggregator eAgg) {
        super.visit(eAgg);
    }
    
    /**
     * Return SQL Filter expression.
     * @return
     */
    public String getSQLFilter() {
    	return this.builder.toString();
    }
}
