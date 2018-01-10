package Filter;

import java.util.regex.Matcher;
import java.util.regex.Pattern;
import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.query.Query;
import com.hp.hpl.jena.query.QueryFactory;
import com.hp.hpl.jena.shared.PrefixMapping;
import com.hp.hpl.jena.sparql.algebra.Algebra;
import com.hp.hpl.jena.sparql.algebra.Op;
import com.hp.hpl.jena.sparql.algebra.OpWalker;
import com.hp.hpl.jena.sparql.algebra.op.OpBGP;
import com.hp.hpl.jena.sparql.algebra.op.OpProject;
import com.hp.hpl.jena.sparql.core.Var;
public class Structures {

	public static void main(String[] args) {
		
		
		//String s = build_triple_regex();

		//Pattern p =  Pattern.compile(s.toString() );
		//Matcher m = p.matcher("<http://www.w3.org/2001/sw/RDFCore/ntriples/> <http://purl.org/dc/elements/1.1/creator> \"Dave Beckett\" .");
		//System.out.println(m.matches());
		System.out.println(double_escape(build_triple_regex()));
		String test_query = "PREFIX foaf:  <http://xmlns.com/foaf/0.1/>\n" + 
				"SELECT ?name\n" + 
				"WHERE {\n" + 
				"    ?person foaf:name ?name .\n"
				+ "  ?ciccia foaf:tot ?nome .\n"
				+ "  FILTER(?name = \"Peter\"" 
				+ " || " 
				
				//+ "!bound(?name)) "
				+ " (?a + ?n) >= 5)" 
//				+ " FILTER(bound(?name) ) \n" +
//				"  FILTER(?name = \"Peter\")" +
				+ "}";
		
		TestJean j = new TestJean();

        Query query = QueryFactory.create(test_query);
        Op opQuery = Algebra.compile(query);
        System.out.println(opQuery) ;
        OpWalker.walk(opQuery, j);
        System.out.println(j.id);
        
        
	}


	
	private static String build_triple_regex() {
		String uri_s = "<(?:[^:]+:[^\\s\\\"<>]+)>";
		String literal_s = "\\\"(?:[^\\\"\\]*(?:\\.[^\\\"\\]*)*)\\\"(?:@([a-z]+(?:-[a-zA-Z0-9]+)*)|\\^\\^" + uri_s + ")?";
		String subject_s = "(" + uri_s + "|" + literal_s + ")";
		String predicate_s = "(" + uri_s + ")";
		String object_s = "(" + uri_s + "|" + literal_s + ")";
		String space_s = "[ \t]+";
		return "[ \\t]*" + subject_s + space_s + predicate_s + space_s + object_s + "[ \t]*\\.*[ \t]*(#.*)?";
	}
	
	private static String double_escape(String a) {
		return a.replace("\\","\\\\" );
	}
	
}

