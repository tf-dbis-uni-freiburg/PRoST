package extVp;

public class LiteralsTuple {
	String outerLiteral;
	String innerLiteral;
	
	public LiteralsTuple(String outerLiteral, String innerLiteral) {
		super();
		this.outerLiteral = outerLiteral;
		this.innerLiteral = innerLiteral;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((innerLiteral == null) ? 0 : innerLiteral.hashCode());
		result = prime * result + ((outerLiteral == null) ? 0 : outerLiteral.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		LiteralsTuple other = (LiteralsTuple) obj;
		if (innerLiteral == null) {
			if (other.innerLiteral != null)
				return false;
		} else if (!innerLiteral.equals(other.innerLiteral))
			return false;
		if (outerLiteral == null) {
			if (other.outerLiteral != null)
				return false;
		} else if (!outerLiteral.equals(other.outerLiteral))
			return false;
		return true;
	}
}
