package big_data_analytics_java.chp7;

public class DataRowVO {
	private double numPregCnt;
	private double plasmaGlucConc;
	private double diaBloodPressure;
	private double skinFoldThickness;
	private double serumInsulin;
	private double bodyMassIndex;
	private double diabPedigreeFunc;
	private double age;
	private double isDiseasePresent; //1 for yes, 0 for no.
	
	public DataRowVO() {
		
	}
	
	public double getNumPregCnt() {
		return numPregCnt;
	}
	public void setNumPregCnt(double numPregCnt) {
		this.numPregCnt = numPregCnt;
	}
	public double getPlasmaGlucConc() {
		return plasmaGlucConc;
	}
	public void setPlasmaGlucConc(double plasmaGlucConc) {
		this.plasmaGlucConc = plasmaGlucConc;
	}
	public double getDiaBloodPressure() {
		return diaBloodPressure;
	}
	public void setDiaBloodPressure(double diaBloodPressure) {
		this.diaBloodPressure = diaBloodPressure;
	}
	public double getSkinFoldThickness() {
		return skinFoldThickness;
	}
	public void setSkinFoldThickness(double skinFoldThickness) {
		this.skinFoldThickness = skinFoldThickness;
	}
	public double getSerumInsulin() {
		return serumInsulin;
	}
	public void setSerumInsulin(double serumInsulin) {
		this.serumInsulin = serumInsulin;
	}
	public double getBodyMassIndex() {
		return bodyMassIndex;
	}
	public void setBodyMassIndex(double bodyMassIndex) {
		this.bodyMassIndex = bodyMassIndex;
	}
	public double getDiabPedigreeFunc() {
		return diabPedigreeFunc;
	}
	public void setDiabPedigreeFunc(double diabPedigreeFunc) {
		this.diabPedigreeFunc = diabPedigreeFunc;
	}
	public double getAge() {
		return age;
	}
	public void setAge(double age) {
		this.age = age;
	}
	public double getIsDiseasePresent() {
		return isDiseasePresent;
	}
	public void setIsDiseasePresent(double isDiseasePresent) {
		this.isDiseasePresent = isDiseasePresent;
	}

	
}
