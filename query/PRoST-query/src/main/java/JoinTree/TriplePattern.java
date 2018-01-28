package JoinTree;

import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.shared.PrefixMapping;


public class TriplePattern {
	public String subject;
	public String predicate;
	public String object;
	public ElementType subjectType;
	public ElementType objectType;
	public ElementType predicateType;
	public boolean isComplex = false;
	public boolean arePrefixUsed = false;
	
	
	// construct from Jena triple
	public TriplePattern(Triple triple, PrefixMapping prefixes, boolean datasetUsesPrefixes){

		// extract and set the subject
		if(triple.getSubject().isVariable()) {
		  subjectType = ElementType.VARIABLE;
		  subject = triple.getSubject().toString();
		}
		else {
		  subjectType = ElementType.CONSTANT;
		  subject = datasetUsesPrefixes? triple.getSubject().toString(prefixes) : 
		    "<" + triple.getSubject().getURI() + ">";
		  
		}
		
		// extract and set the predicate
		predicateType = ElementType.CONSTANT;
		predicate = triple.getPredicate().toString();
		
		// extract and set the object
		if(triple.getObject().isVariable()) {
		  objectType = ElementType.VARIABLE;
		  object = triple.getObject().toString(prefixes);
		}
		else {
		  objectType = ElementType.CONSTANT;
		  object = datasetUsesPrefixes ? triple.getObject().toString(prefixes) : 
		    "<" + triple.getObject().getURI() + ">";
		}
		
	}
	
	@Override
	public String toString(){
		return String.format("(%s) (%s) (%s)", subject, predicate, object);
		
	}
}