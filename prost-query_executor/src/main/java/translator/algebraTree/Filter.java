package translator.algebraTree;

import java.util.ArrayList;
import java.util.List;

import com.hp.hpl.jena.shared.PrefixMapping;
import com.hp.hpl.jena.sparql.algebra.op.OpFilter;
import com.hp.hpl.jena.sparql.expr.E_Bound;
import com.hp.hpl.jena.sparql.expr.E_DateTimeDay;
import com.hp.hpl.jena.sparql.expr.E_IsIRI;
import com.hp.hpl.jena.sparql.expr.E_LogicalNot;
import com.hp.hpl.jena.sparql.expr.Expr;
import com.hp.hpl.jena.sparql.expr.ExprFunction1;
import com.hp.hpl.jena.sparql.expr.ExprFunction2;
import com.hp.hpl.jena.sparql.expr.ExprVar;
import com.hp.hpl.jena.sparql.expr.NodeValue;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import statistics.DatabaseStatistics;
import translator.ToSQLExp;
import utils.Settings;

/**
 * A filter algebra operation on a sub-operation.
 */
public class Filter extends SimpleOperation {
	private final ArrayList<String> filters = new ArrayList<>();

	Filter(final OpFilter jenaAlgebraTree, final DatabaseStatistics statistics, final Settings settings,
		   final PrefixMapping prefixes) throws Exception {
		super(jenaAlgebraTree.getSubOp(), statistics, settings, prefixes);
		for (final Expr jenaExpression : jenaAlgebraTree.getExprs().getList()) {
			filters.add(jenaExpressionToSqlExpression(jenaExpression));
		}
	}

	Filter(final Operation subOperation, final List<Expr> jenaFilterOperations) throws Exception {
		super(subOperation);
		for (final Expr jenaExpression : jenaFilterOperations) {
			filters.add(jenaExpressionToSqlExpression(jenaExpression));
		}
	}

	private String jenaExpressionToSqlExpression(final Expr func) throws Exception {
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

	private String jenaExpressionToSqlExpression(final ExprFunction1 func) throws Exception {
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

	private String jenaExpressionToSqlExpression(final ExprFunction2 func) throws Exception {
		return jenaExpressionToSqlExpression(func.getArg1())
				+ " "
				+ ToSQLExp.getSqlExpr(func)
				+ " "
				+ jenaExpressionToSqlExpression(func.getArg2());
	}

	//for literals and URIs
	private String jenaExpressionToSqlExpression(final NodeValue nodeValue) {
		final StringBuilder sqlExpressionBuilder = new StringBuilder();
		if (nodeValue.isIRI()) {
			sqlExpressionBuilder.append("<").append(nodeValue.asString()).append(">");
		} else {
			sqlExpressionBuilder.append(nodeValue.asString());
		}
		return sqlExpressionBuilder.toString();
	}

	private String jenaExpressionToSqlExpression(final ExprVar var) {
		return var.getVarName();
	}

	public Dataset<Row> computeOperation(final SQLContext sqlContext) {
		Dataset<Row> result = this.computeSubOperation(sqlContext);
		for (final String filter : filters) {
			result = result.filter(filter);
		}
		return result;
	}

	@Override
	public String toString() {
		final StringBuilder stringBuilder = new StringBuilder();
		stringBuilder.append("FILTER{");
		for (final String filter : this.filters) {
			stringBuilder.append(System.lineSeparator());
			stringBuilder.append(filter);
		}
		stringBuilder.append(System.lineSeparator()).append("}");
		return stringBuilder.toString();
	}
}
