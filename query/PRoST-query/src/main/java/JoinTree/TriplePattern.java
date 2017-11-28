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
	
	// construct from single properties
	public TriplePattern(String subject, String predicate, String object, 
			ElementType subjectType, ElementType objectType, ElementType predicateType){
		this.subject = subject;
		this.subjectType = subjectType;
		this.predicate = predicate;
		this.predicateType = predicateType;
		this.object = object;
		this.objectType = objectType;
	}
	
	// construct from Jena triple
	public TriplePattern(Triple triple, PrefixMapping prefixes){

		// extract and set the subject
		if(triple.getSubject().isVariable())
			subjectType = ElementType.VARIABLE;
		else
			subjectType = ElementType.CONSTANT;
		subject = triple.getSubject().toString(prefixes);
			
		
		// extract and set the predicate
		predicateType = ElementType.CONSTANT;
		predicate = triple.getPredicate().toString(prefixes);
		
		// extract and set the object
		if(triple.getObject().isVariable())
			objectType = ElementType.VARIABLE;
		else
			objectType = ElementType.CONSTANT;
		object = triple.getObject().toString(prefixes);
		
	}
	
	@Override
	public String toString(){
		return String.format("(%s) (%s) (%s)", subject, predicate, object);
		
	}
}