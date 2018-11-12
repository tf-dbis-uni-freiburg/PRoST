package joinTree;

import java.util.List;

public abstract class MVNode extends Node {

	public List<TriplePattern> tripleGroup;
	
	@Override
	public List<TriplePattern> collectTriples() {
		return tripleGroup;
	}
	
}
