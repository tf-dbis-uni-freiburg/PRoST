package Filter;

import java.util.*;

import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.query.Query;
import com.hp.hpl.jena.query.QueryFactory;
import com.hp.hpl.jena.shared.PrefixMapping;
import com.hp.hpl.jena.sparql.algebra.Algebra;
import com.hp.hpl.jena.sparql.algebra.Op;
import com.hp.hpl.jena.sparql.algebra.OpVisitorBase;
import com.hp.hpl.jena.sparql.algebra.OpWalker;
import com.hp.hpl.jena.sparql.algebra.OpWalker.WalkerVisitor;
import com.hp.hpl.jena.sparql.algebra.op.OpBGP;
import com.hp.hpl.jena.sparql.algebra.op.OpFilter;
import com.hp.hpl.jena.sparql.algebra.op.OpProject;
import com.hp.hpl.jena.sparql.expr.E_NotEquals;
import com.hp.hpl.jena.sparql.expr.Expr;
import com.hp.hpl.jena.sparql.expr.ExprVisitorBase;
import com.hp.hpl.jena.sparql.syntax.Element;
import com.hp.hpl.jena.sparql.syntax.ElementGroup;


public class TestJean extends OpVisitorBase {

	public String id;
	public List<Triple> triples;
	private ElementGroup currentGroup;
	private String filter;
	
	public TestJean() {
	}

    public void visit(OpBGP opBGP)
    {
         
    	 triples = opBGP.getPattern().getList();
    	 System.out.println("Printing triple");
    	 for (Triple t : triples) {
    		 System.out.println(t);
    	 }
    	 id = "sssss";
    }
    	

    public void visit(OpFilter opFilter)
    {
    	System.out.println("filter");
    	System.out.println(opFilter.toString());
    	/*//JoinPlanner.setFilterVars();
    	Iterator<Expr> it = opFilter.getExprs().iterator();
    	while(it.hasNext()){
    		Expr e =it.next();
    		Iterator<Expr> a = e.getFunction().getArgs().iterator();
			System.out.println(e.getFunction().getOpName());
    		while(a.hasNext()){
    			Expr temp = a.next();
    			if(temp.isVariable())
    				JoinPlaner.filter(temp.toString(),e.getFunction());
    		}
    	}*/
    	FilterVisitor a = new FilterVisitor();
    	for (Expr e : opFilter.getExprs()) {
    		
			e.visit(a);
			filter = a.getSQLFilter();
			System.out.println("AAA");
			System.out.println(e);
			System.out.println(e.isFunction());
		}
    	

    }
    
    public void visit(OpProject opP)
    {
    	System.out.println("prok");
    	System.out.println(opP.getVars());
    }
    
     
    
}
