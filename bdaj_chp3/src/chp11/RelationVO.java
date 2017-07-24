package chp11;

public class RelationVO {
	private String src;
	private String dst;
	private String relationShip;
	
	public RelationVO() {
		
	}
	
	public RelationVO(String src, String dst, String relationShip) {
		this.src = src;
		this.dst = dst;
		this.relationShip = relationShip;
	}

	public String getSrc() {
		return src;
	}

	public void setSrc(String src) {
		this.src = src;
	}



	public String getDst() {
		return dst;
	}

	public void setDst(String dst) {
		this.dst = dst;
	}

	public String getRelationShip() {
		return relationShip;
	}

	public void setRelationShip(String relationShip) {
		this.relationShip = relationShip;
	}
	
	
}
