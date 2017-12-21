package Filter;

import java.util.Set;

import com.hp.hpl.jena.sparql.core.Var;
import com.hp.hpl.jena.sparql.expr.*;

public class FilterVisitor extends ExprVisitorBase {

	String sqlFilter = "";

	public String visit(E_NotEquals a) {
		System.out.println("E not equals");
		System.out.println(a);
		return "h";
	}

	public void visit(ExprFunction1 a) {
		System.out.println("ExprFunction1 getfunction(): " + a.getFunction());
		System.out.println("ExprFunction1 getVar(): " + a.getArg());
		System.out.println("ExprFunction1 getOpName(): " + a.getOpName());
		System.out.println("ExprFunction1 getFunctionSymbol: " + a.getFunctionSymbol().getSymbol());

		if (a.getFunctionSymbol().getSymbol() == "bound") {

		} else if (a.getFunctionSymbol().getSymbol() == "isURI") {

		} else if (a.getFunctionSymbol().getSymbol() == "isBlank") {

		}
	}

	public void visit(ExprFunction2 a) {
		System.out.println("ExprFunction2 - arg1: " + a.getArg1());
		System.out.println("ExprFunction2 - arg2: " + a.getArg2());
		System.out.println("ExprFunction2 - opname: " + a.getOpName());
		if (a.getOpName().equals("=")) {
			sqlFilter += prepareExpr(a.getArg1()) + " = " + prepareExpr(a.getArg2()) + " ";
		} else if (a.getOpName().equals(">")) {
			sqlFilter += prepareExpr(a.getArg1()) + " > " + prepareExpr(a.getArg2()) + " ";
			System.out.println("ExprFunction2 - The expression contains >");
		} else if (a.getOpName().equals("<")) {
			sqlFilter += prepareExpr(a.getArg1()) + " < " + prepareExpr(a.getArg2()) + " ";
			System.out.println("ExprFunction2 -The expression contains <");
		} else if (a.getOpName().equals("!=")) {
			sqlFilter += prepareExpr(a.getArg1()) + " != " + prepareExpr(a.getArg2()) + " ";
			System.out.println("ExprFunction2 - The expression contains !=");
		} else if (a.getOpName().equals("&&")) {
			sqlFilter += prepareExpr(a.getArg1()) + " AND " + prepareExpr(a.getArg2()) + " ";
		} else if (a.getOpName().equals("||")) {
			sqlFilter += prepareExpr(a.getArg1()) + " OR " + prepareExpr(a.getArg2()) + " ";
			System.out.println("ExprFunction2 - The expression contains ||");
		}

		System.out.println("Final SQL query: " + sqlFilter);
	}

	public String prepareExpr(Expr a) {
		String sql = "";
		System.out.println("TEST exprVarname: " + a.getVarName());
		System.out.println("TEST expr Constant: " + a.getConstant());
		System.out.println("TEST expr asVar: " + a.asVar());
		System.out.println("TEST exprvar in expr: " + a.getExprVar());
		System.out.println("TEST exprFUNKTION: " + a.getFunction());
		System.out.println("TEST exprvars in expr: " + a.getVarsMentioned().toString());

	
		if (a.getFunction() != null) {
			if (a.getFunction().getOpName().equals("=")) {

				sql = prepareExpr(a.getFunction().getArgs().get(0)) + " = "
						+ prepareExpr(a.getFunction().getArgs().get(1));

			} else if (a.getFunction().getOpName().equals("&&")) {
				sql = prepareExpr(a.getFunction().getArgs().get(0)) + " AND "
						+ prepareExpr(a.getFunction().getArgs().get(1));

				// System.out.println("setArray && : " + sql);

			} else if (a.getFunction().getOpName().equals("||")) {
				sql = prepareExpr(a.getFunction().getArgs().get(0)) + " OR "
						+ prepareExpr(a.getFunction().getArgs().get(1));

				// System.out.println("setArray && : " + sql);

			} else if (a.getFunction().getOpName().equals("<")) {
				sql = prepareExpr(a.getFunction().getArgs().get(0)) + " < "
						+ prepareExpr(a.getFunction().getArgs().get(1));
			} else if (a.getFunction().getOpName().equals(">")) {
				sql = prepareExpr(a.getFunction().getArgs().get(0)) + " > "
						+ prepareExpr(a.getFunction().getArgs().get(1));
			} else if (a.getFunction().getOpName().equals("<=")) {
				sql = prepareExpr(a.getFunction().getArgs().get(0)) + " <= "
						+ prepareExpr(a.getFunction().getArgs().get(1));
			} else if (a.getFunction().getOpName().equals(">=")) {
				sql = prepareExpr(a.getFunction().getArgs().get(0)) + " >= "
						+ prepareExpr(a.getFunction().getArgs().get(1));
			} else if (a.getFunction().getOpName().equals("||")) {
				sql = prepareExpr(a.getFunction().getArgs().get(0)) + " OR "
						+ prepareExpr(a.getFunction().getArgs().get(1));
			} else if (a.getFunction().getOpName().equals("!")) {
				sql = prepareExpr(a.getFunction().getArgs().get(0)) + " NOT "
						+ prepareExpr(a.getFunction().getArgs().get(1));
			}

		} else {
			sql = a.toString();
		}
		return sql;
	}

	public void removeQuestionMark(String query) {

	}

	public void visit(ExprFunction3 a) {
		System.out.println(a);
	}

	public void visit(E_Equals b) {
		System.out.println(b.getOpName());
		sqlFilter += b.getArg1() + " = " + b.getArg2() + " ";
		System.out.println("visit-equals: " + sqlFilter);
	}

	public void visit(E_Bound a) {
		System.out.println("E_Bound arg1: ");

		System.out.println("E_Bound opname: ");

	}

	public String getSQLFilter() {
		// TODO Auto-generated method stub
		return sqlFilter;
	}
}
