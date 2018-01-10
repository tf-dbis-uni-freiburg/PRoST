package Filter;

import java.util.Set;
import com.hp.hpl.jena.sparql.core.Var;
import com.hp.hpl.jena.sparql.expr.*;
import Executor.Utils;
public class FilterVisitor extends ExprVisitorBase {

	String sqlFilter = "";
	String exprVar = "";
	String position = "";
	
	public void visit(ExprFunction2 a) {
        if (a instanceof E_Equals) {
        	System.out.println("ExprFunction2 - equals - entered");
        	this.visit((E_Equals) a);
        	
        }
        else if (a instanceof E_NotEquals) {
        	System.out.println("ExprFunction2 -  NotEquals");
        	this.visit((E_NotEquals) a);
        }
        else if (a instanceof E_LogicalAnd) {
        	System.out.println("ExprFunction2 - LogicalAnd");
        	sqlFilter = this.visit((E_LogicalAnd) a);
        }
        else if (a instanceof E_LogicalOr) {
        	sqlFilter = this.visit((E_LogicalOr) a);
        }
        
        else {
        	System.out.println("exprfct2");
        }
        
	}
	
	public void visit(ExprFunction1 a) {
		 if (a instanceof E_Bound) {
        	sqlFilter = this.visit((E_Bound) a);
        }
	}

	public String visit(E_NotEquals a) {
		if (a.getArg1() instanceof ExprVar && a.getArg2() instanceof ExprVar) {
			return Utils.removeQuestionMark(a.getArg1().toString()) + " != " + Utils.removeQuestionMark(a.getArg2().toString());
			}
		else return "E_NotEquals- Arguments are no ExprVars";
	}



	public String visit(E_Equals a) {
		if (a.getArg1() instanceof ExprVar && a.getArg2() instanceof ExprVar) {
			return Utils.removeQuestionMark(a.getArg1().toString()) + " = " + Utils.removeQuestionMark(a.getArg2().toString());
		}
		else return a.getArg1().toString().replace("?", "") + " = " + a.getArg2().toString().replace("?", "");
	}
	
	public String visit(E_LessThan a) {
		if (a.getArg1() instanceof ExprVar && a.getArg2() instanceof ExprVar) {
		return Utils.removeQuestionMark(a.getArg1().toString()) + " < " + Utils.removeQuestionMark(a.getArg2().toString());
		}
		else return a.getArg1().toString().replace("?", "") + " < " + a.getArg2().toString().replace("?", "");
	}
	
	public String visit(E_LessThanOrEqual a) {
		if (a.getArg1() instanceof ExprVar && a.getArg2() instanceof ExprVar) {
		return Utils.removeQuestionMark(a.getArg1().toString()) + " <= " + Utils.removeQuestionMark(a.getArg2().toString());
		}
		else return a.getArg1().toString().replace("?", "") + " <= " + a.getArg2().toString().replace("?", "");
	}
	
	public String visit(E_GreaterThan a) {
		if (a.getArg1() instanceof ExprVar && a.getArg2() instanceof ExprVar) {
		return Utils.removeQuestionMark(a.getArg1().toString()) + " > " + Utils.removeQuestionMark(a.getArg2().toString());
		}
		else return a.getArg1().toString().replace("?", "") + " > " + a.getArg2().toString().replace("?", "");

	}
	
	public String visit(E_GreaterThanOrEqual a) {
		if (a.getArg1() instanceof ExprVar && a.getArg2() instanceof ExprVar) {
		return Utils.removeQuestionMark(a.getArg1().toString()) + " >= " + Utils.removeQuestionMark(a.getArg2().toString());
		}
		else return a.getArg1().toString().replace("?", "") + " >= " + a.getArg2().toString().replace("?", "");
	}
	
	public String visit(E_LogicalAnd a) {
		System.out.println("LogicalAND entered");
		return createSqlString(a, " AND ");
		
	}
	
	public String visit(E_LogicalOr a) {
		return createSqlString(a, " OR ");
		
	}
//-----------------------------------------------------------------------------------------------------------	
	public String visit(E_Bound a) {
		return a.getFunction().toString();
	}
	
	public String visit(E_IsLiteral a) {
		return a.getFunction().toString();
	}
	
	public String visit(E_IsIRI a) {
		return a.getFunction().toString();
	}
	
	public String visit(E_IsBlank a) {
		return "";
	}
	
	public String visit(E_LogicalNot a) {
		if (a.getArg() instanceof E_Bound) {
		return "NOT " + this.visit((E_Bound)a.getArg());
		}
		else if (a.getArg() instanceof E_IsIRI) {
			return "NOT " + this.visit((E_IsIRI)a.getArg());
			}
		else if (a.getArg() instanceof E_IsLiteral) {
			return "NOT " + this.visit((E_IsLiteral)a.getArg());
			}
		else return "NOT " + a.getArg().toString();
	}
	
	
	
	
	
	
	
	
//-----------------------------------------------------------------------------------------------------------	
	public String createSqlString(ExprFunction2 a, String op) {
		
		if (a.getArg1() instanceof ExprVar && a.getArg2() instanceof ExprVar) {
			System.out.println("LogicalAnd - Both are variables ");
			return Utils.removeQuestionMark(a.getArg1().toString()) + op + Utils.removeQuestionMark(a.getArg2().toString());
			}
		
		else if (a.getArg1() instanceof ExprFunction  && a.getArg2() instanceof ExprFunction) {
			//E_Equals
			if (a.getArg1() instanceof E_Equals && a.getArg2() instanceof E_Equals) {
	        	return this.visit((E_Equals) a.getArg1()) + op + this.visit((E_Equals) a.getArg2());
	        	}
			else if (a.getArg1() instanceof E_Equals && a.getArg2() instanceof E_NotEquals) {
	        	return this.visit((E_Equals) a.getArg1()) + op + this.visit((E_NotEquals) a.getArg2());
	        	}
			else if (a.getArg1() instanceof E_Equals && a.getArg2() instanceof E_LogicalAnd) {
				return this.visit((E_Equals) a.getArg1()) + op + this.visit((E_LogicalAnd) a.getArg2());
	        	}
			else if (a.getArg1() instanceof E_Equals && a.getArg2() instanceof E_GreaterThanOrEqual) {
				return this.visit((E_Equals) a.getArg1()) + op + this.visit((E_GreaterThanOrEqual) a.getArg2());
	        	}
			else if (a.getArg1() instanceof E_Equals && a.getArg2() instanceof E_GreaterThan) {
				return this.visit((E_Equals) a.getArg1()) + op + this.visit((E_GreaterThan) a.getArg2());
	        	}
			else if (a.getArg1() instanceof E_Equals && a.getArg2() instanceof E_LessThanOrEqual) {
				return this.visit((E_Equals) a.getArg1()) + op + this.visit((E_LessThanOrEqual) a.getArg2());
	        	}
			else if (a.getArg1() instanceof E_Equals && a.getArg2() instanceof E_LessThan) {
				return this.visit((E_Equals) a.getArg1()) + op + this.visit((E_LessThan) a.getArg2());
	        	}
			else if (a.getArg1() instanceof E_Equals && a.getArg2() instanceof E_Bound) {
				return this.visit((E_Equals) a.getArg1()) + op + this.visit((E_Bound) a.getArg2());
	        	}
			else if (a.getArg1() instanceof E_Equals && a.getArg2() instanceof E_LogicalNot) {
				return this.visit((E_Equals) a.getArg1()) + op + this.visit((E_LogicalNot) a.getArg2());
	        	}
			
			
			//E_NotEquals
	        else if (a.getArg1() instanceof E_NotEquals && a.getArg2() instanceof E_NotEquals) {
	        	return this.visit((E_NotEquals) a.getArg1()) + op + this.visit((E_NotEquals) a.getArg2());
	        	}
	        else if (a.getArg1() instanceof E_NotEquals && a.getArg2() instanceof E_Equals) {
	        	return this.visit((E_NotEquals) a.getArg1()) + op + this.visit((E_Equals) a.getArg2());
	        	}
	        else if (a.getArg1() instanceof E_NotEquals && a.getArg2() instanceof E_LogicalAnd) {
	        	return this.visit((E_NotEquals) a.getArg1()) + op + this.visit((E_LogicalAnd) a.getArg2());
	        	}
			else if (a.getArg1() instanceof E_NotEquals && a.getArg2() instanceof E_GreaterThanOrEqual) {
				return this.visit((E_NotEquals) a.getArg1()) + op + this.visit((E_GreaterThanOrEqual) a.getArg2());
	        	}
			else if (a.getArg1() instanceof E_NotEquals && a.getArg2() instanceof E_GreaterThan) {
				return this.visit((E_NotEquals) a.getArg1()) + op + this.visit((E_GreaterThan) a.getArg2());
	        	}
			else if (a.getArg1() instanceof E_NotEquals && a.getArg2() instanceof E_LessThanOrEqual) {
				return this.visit((E_NotEquals) a.getArg1()) + op + this.visit((E_LessThanOrEqual) a.getArg2());
	        	}
			else if (a.getArg1() instanceof E_NotEquals && a.getArg2() instanceof E_LessThan) {
				return this.visit((E_NotEquals) a.getArg1()) + op + this.visit((E_LessThan) a.getArg2());
	        	}
			
			
			//LogicalAnd
	        else if (a.getArg1() instanceof E_LogicalAnd && a.getArg2() instanceof E_Equals) {
	        	return this.visit((E_LogicalAnd) a.getArg1()) + op + this.visit((E_Equals) a.getArg2());
	        }
	        else if (a.getArg1() instanceof E_LogicalAnd && a.getArg2() instanceof E_NotEquals) {
	        	return this.visit((E_LogicalAnd) a.getArg1()) + op + this.visit((E_NotEquals) a.getArg2());
	        }
	        else if (a.getArg1() instanceof E_LogicalAnd && a.getArg2() instanceof E_LogicalAnd) {
	        	return this.visit((E_LogicalAnd) a.getArg1()) + op + this.visit((E_LogicalAnd) a.getArg2());
	        }
			else if (a.getArg1() instanceof E_LogicalAnd && a.getArg2() instanceof E_GreaterThanOrEqual) {
				return this.visit((E_LogicalAnd) a.getArg1()) + op + this.visit((E_GreaterThanOrEqual) a.getArg2());
	        	}
			else if (a.getArg1() instanceof E_LogicalAnd && a.getArg2() instanceof E_GreaterThan) {
				return this.visit((E_LogicalAnd) a.getArg1()) + op + this.visit((E_GreaterThan) a.getArg2());
	        	}
			else if (a.getArg1() instanceof E_LogicalAnd && a.getArg2() instanceof E_LessThanOrEqual) {
				return this.visit((E_LogicalAnd) a.getArg1()) + op + this.visit((E_LessThanOrEqual) a.getArg2());
	        	}
			else if (a.getArg1() instanceof E_LogicalAnd && a.getArg2() instanceof E_LessThan) {
				return this.visit((E_LogicalAnd) a.getArg1()) + op + this.visit((E_LessThan) a.getArg2());
	        	}
			
			//E_LessThan
			else if (a.getArg1() instanceof E_LessThan && a.getArg2() instanceof E_Equals) {
	        	return this.visit((E_LessThan) a.getArg1()) + op + this.visit((E_Equals) a.getArg2());
	        }
	        else if (a.getArg1() instanceof E_LessThan && a.getArg2() instanceof E_NotEquals) {
	        	return this.visit((E_LessThan) a.getArg1()) + op + this.visit((E_NotEquals) a.getArg2());
	        }
	        else if (a.getArg1() instanceof E_LessThan && a.getArg2() instanceof E_LogicalAnd) {
	        	return this.visit((E_LessThan) a.getArg1()) + op + this.visit((E_LogicalAnd) a.getArg2());
	        }
	        else if (a.getArg1() instanceof E_LessThan && a.getArg2() instanceof E_GreaterThanOrEqual) {
				return this.visit((E_LessThan) a.getArg1()) + op + this.visit((E_GreaterThanOrEqual) a.getArg2());
	        	}
			else if (a.getArg1() instanceof E_LessThan && a.getArg2() instanceof E_GreaterThan) {
				return this.visit((E_LessThan) a.getArg1()) + op + this.visit((E_GreaterThan) a.getArg2());
	        	}
			else if (a.getArg1() instanceof E_LessThan && a.getArg2() instanceof E_LessThanOrEqual) {
				return this.visit((E_LessThan) a.getArg1()) + op + this.visit((E_LessThanOrEqual) a.getArg2());
	        	}
			else if (a.getArg1() instanceof E_LessThan && a.getArg2() instanceof E_LessThan) {
				return this.visit((E_LessThan) a.getArg1()) + op + this.visit((E_LessThan) a.getArg2());
			}
			
			//E_LessThanOrEqual
			else if (a.getArg1() instanceof E_LessThanOrEqual && a.getArg2() instanceof E_Equals) {
	        	return this.visit((E_LessThanOrEqual) a.getArg1()) + op + this.visit((E_Equals) a.getArg2());
	        }
	        else if (a.getArg1() instanceof E_LessThanOrEqual && a.getArg2() instanceof E_NotEquals) {
	        	return this.visit((E_LessThanOrEqual) a.getArg1()) + op + this.visit((E_NotEquals) a.getArg2());
	        }
	        else if (a.getArg1() instanceof E_LessThanOrEqual && a.getArg2() instanceof E_LogicalAnd) {
	        	return this.visit((E_LessThanOrEqual) a.getArg1()) + op + this.visit((E_LogicalAnd) a.getArg2());
	        }
	        else if (a.getArg1() instanceof E_LessThanOrEqual && a.getArg2() instanceof E_GreaterThanOrEqual) {
				return this.visit((E_LessThanOrEqual) a.getArg1()) + op + this.visit((E_GreaterThanOrEqual) a.getArg2());
	        	}
			else if (a.getArg1() instanceof E_LessThanOrEqual && a.getArg2() instanceof E_GreaterThan) {
				return this.visit((E_LessThanOrEqual) a.getArg1()) + op + this.visit((E_GreaterThan) a.getArg2());
	        	}
			else if (a.getArg1() instanceof E_LessThanOrEqual && a.getArg2() instanceof E_LessThanOrEqual) {
				return this.visit((E_LessThanOrEqual) a.getArg1()) + op + this.visit((E_LessThanOrEqual) a.getArg2());
	        	}
			else if (a.getArg1() instanceof E_LessThanOrEqual && a.getArg2() instanceof E_LessThan) {
				return this.visit((E_LessThanOrEqual) a.getArg1()) + op + this.visit((E_LessThan) a.getArg2());
			}
			
			//E_GreaterThan
			else if (a.getArg1() instanceof E_GreaterThan && a.getArg2() instanceof E_Equals) {
	        	return this.visit((E_GreaterThan) a.getArg1()) + op + this.visit((E_Equals) a.getArg2());
	        }
	        else if (a.getArg1() instanceof E_GreaterThan && a.getArg2() instanceof E_NotEquals) {
	        	return this.visit((E_GreaterThan) a.getArg1()) + op + this.visit((E_NotEquals) a.getArg2());
	        }
	        else if (a.getArg1() instanceof E_GreaterThan && a.getArg2() instanceof E_LogicalAnd) {
	        	return this.visit((E_GreaterThan) a.getArg1()) + op + this.visit((E_LogicalAnd) a.getArg2());
	        }
	        else if (a.getArg1() instanceof E_GreaterThan && a.getArg2() instanceof E_GreaterThanOrEqual) {
				return this.visit((E_GreaterThan) a.getArg1()) + op + this.visit((E_GreaterThanOrEqual) a.getArg2());
	        	}
			else if (a.getArg1() instanceof E_GreaterThan && a.getArg2() instanceof E_GreaterThan) {
				return this.visit((E_GreaterThan) a.getArg1()) + op + this.visit((E_GreaterThan) a.getArg2());
	        	}
			else if (a.getArg1() instanceof E_GreaterThan && a.getArg2() instanceof E_LessThanOrEqual) {
				return this.visit((E_GreaterThan) a.getArg1()) + op + this.visit((E_LessThanOrEqual) a.getArg2());
	        	}
			else if (a.getArg1() instanceof E_GreaterThan && a.getArg2() instanceof E_LessThan) {
				return this.visit((E_GreaterThan) a.getArg1()) + op + this.visit((E_LessThan) a.getArg2());
			}
			
			
			//E_GreaterThanOrEqual
			else if (a.getArg1() instanceof E_GreaterThanOrEqual && a.getArg2() instanceof E_Equals) {
	        	return this.visit((E_GreaterThanOrEqual) a.getArg1()) + op + this.visit((E_Equals) a.getArg2());
	        }
	        else if (a.getArg1() instanceof E_GreaterThanOrEqual && a.getArg2() instanceof E_NotEquals) {
	        	return this.visit((E_GreaterThanOrEqual) a.getArg1()) + op + this.visit((E_NotEquals) a.getArg2());
	        }
	        else if (a.getArg1() instanceof E_GreaterThanOrEqual && a.getArg2() instanceof E_LogicalAnd) {
	        	return this.visit((E_GreaterThanOrEqual) a.getArg1()) + op + this.visit((E_LogicalAnd) a.getArg2());
	        }
	        else if (a.getArg1() instanceof E_GreaterThanOrEqual && a.getArg2() instanceof E_GreaterThanOrEqual) {
				return this.visit((E_GreaterThanOrEqual) a.getArg1()) + op + this.visit((E_GreaterThanOrEqual) a.getArg2());
	        	}
			else if (a.getArg1() instanceof E_GreaterThanOrEqual && a.getArg2() instanceof E_GreaterThan) {
				return this.visit((E_GreaterThanOrEqual) a.getArg1()) + op + this.visit((E_GreaterThan) a.getArg2());
	        	}
			else if (a.getArg1() instanceof E_GreaterThanOrEqual && a.getArg2() instanceof E_LessThanOrEqual) {
				return this.visit((E_GreaterThanOrEqual) a.getArg1()) + op + this.visit((E_LessThanOrEqual) a.getArg2());
	        	}
			else if (a.getArg1() instanceof E_GreaterThanOrEqual && a.getArg2() instanceof E_LessThan) {
				return this.visit((E_GreaterThanOrEqual) a.getArg1()) + op + this.visit((E_LessThan) a.getArg2());
			}
			else return "";
			
			
		}
		else {
        	return "LogicalAnd - Expression not recognized";
        }
		
		
	}
	
	
	
	
	
	
	
	
	
	
	

	public String getSQLFilter() {
		// TODO Auto-generated method stub
		return sqlFilter;
	}
}
