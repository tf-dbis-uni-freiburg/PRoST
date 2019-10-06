package translator.algebraTree;

import com.hp.hpl.jena.sparql.expr.E_Bound;
import com.hp.hpl.jena.sparql.expr.E_DateTimeDay;
import com.hp.hpl.jena.sparql.expr.E_IsIRI;
import com.hp.hpl.jena.sparql.expr.E_LogicalNot;
import com.hp.hpl.jena.sparql.expr.Expr;
import com.hp.hpl.jena.sparql.expr.ExprFunction1;
import com.hp.hpl.jena.sparql.expr.ExprFunction2;
import com.hp.hpl.jena.sparql.expr.ExprVar;
import com.hp.hpl.jena.sparql.expr.NodeValue;
import translator.ToSQLExp;

public class JenaExpressionConverter {
	public static String jenaExpressionToSqlExpression(final Expr func) throws Exception {
		if (func instanceof ExprFunction1) {
			return jenaExpressionToSqlExpression((ExprFunction1) func);
		} else if (func instanceof ExprFunction2) {
			return jenaExpressionToSqlExpression((ExprFunction2) func);
		} else if (func instanceof NodeValue) {
			return jenaExpressionToSqlExpression((NodeValue) func);
		} else if (func instanceof ExprVar) {
			return jenaExpressionToSqlExpression((ExprVar) func);
		} else {
			throw new Exception("Filter operation not yet implemented");
		}
	}

	private static String jenaExpressionToSqlExpression(final ExprFunction1 func) throws Exception {
		final StringBuilder sqlExpressionBuilder = new StringBuilder();
		sqlExpressionBuilder.append(" ");
		// variable in the beginning
		if (func instanceof E_Bound || func instanceof E_IsIRI) {
			sqlExpressionBuilder.append(jenaExpressionToSqlExpression(func.getArg()));
			sqlExpressionBuilder.append(" ");
			sqlExpressionBuilder.append(ToSQLExp.getSqlExpr(func));
			// variable in middle
		} else if (func instanceof E_DateTimeDay) {
			sqlExpressionBuilder.append(ToSQLExp.getSqlExpr(func));
			sqlExpressionBuilder.append(jenaExpressionToSqlExpression(func.getArg()));
			sqlExpressionBuilder.append(")");
			// variable at the end
		} else if (func instanceof E_LogicalNot) {
			sqlExpressionBuilder.append(ToSQLExp.getSqlExpr(func));
			sqlExpressionBuilder.append(jenaExpressionToSqlExpression(func.getArg()));
		}
		sqlExpressionBuilder.append(" ");
		return sqlExpressionBuilder.toString();
	}

	private static String jenaExpressionToSqlExpression(final ExprFunction2 func) throws Exception {
		return jenaExpressionToSqlExpression(func.getArg1())
				+ " "
				+ ToSQLExp.getSqlExpr(func)
				+ " "
				+ jenaExpressionToSqlExpression(func.getArg2());
	}

	//for literals and URIs
	private static String jenaExpressionToSqlExpression(final NodeValue nodeValue) {
		final StringBuilder sqlExpressionBuilder = new StringBuilder();
		if (nodeValue.isIRI()) {
			sqlExpressionBuilder.append("<").append(nodeValue.asString()).append(">");
		} else {
			sqlExpressionBuilder.append(nodeValue.asString());
		}
		return sqlExpressionBuilder.toString();
	}

	private static String jenaExpressionToSqlExpression(final ExprVar var) {
		return var.getVarName();
	}

}
