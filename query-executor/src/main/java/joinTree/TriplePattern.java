package joinTree;

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

	// construct from Jena triple
	public TriplePattern(final Triple triple, final PrefixMapping prefixes) {
		// extract and set the subject
		if (triple.getSubject().isVariable()) {
			subjectType = ElementType.VARIABLE;
			subject = triple.getSubject().toString();
		} else {
			subjectType = ElementType.CONSTANT;
			//logger.info("Subject of the triple: " + triple.getSubject());
			if (triple.getSubject().isLiteral()) {
				subject = triple.getSubject().toString();
			} else {
				subject = "<" + triple.getSubject().getURI() + ">";
			}
		}

		// extract and set the predicate
		predicateType = ElementType.CONSTANT;
		predicate = "<" + triple.getPredicate().toString() + ">";

		// extract and set the object
		if (triple.getObject().isVariable()) {
			objectType = ElementType.VARIABLE;
			object = triple.getObject().toString(prefixes);
		} else {
			objectType = ElementType.CONSTANT;
			//logger.info("Object of the triple: " + triple.getObject());
			if (triple.getObject().isLiteral()) {
				object = triple.getObject().toString();
			} else {
				object = "<" + triple.getObject().getURI() + ">";
			}
		}
	}

	@Override
	public String toString() {
		return String.format("(%s) (%s) (%s)", subject, predicate, object);
	}
}