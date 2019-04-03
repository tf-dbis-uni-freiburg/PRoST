package query.utilities;

import java.io.Serializable;

/**
 * The purpose of this bean class is to facilitate the creation of datasets
 * (dataframes) of rows.
 *
 * @author Victor Anthony Arrascue Ayala
 */
public class TripleBean implements Serializable {
	private static final long serialVersionUID = 39L;
	private String s;
	private String p;
	private String o;

	public String getS() {
		return s;
	}

	public void setS(final String s) {
		this.s = s;
	}

	public String getP() {
		return p;
	}

	public void setP(final String p) {
		this.p = p;
	}

	public String getO() {
		return o;
	}

	public void setO(final String o) {
		this.o = o;
	}
}
