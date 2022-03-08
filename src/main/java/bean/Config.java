package bean;

public class Config {
    private String proid;
    private String mmac;

    public Config(Object field, Object field1) {
        this.proid = (String) field;
        this.mmac = (String) field1;
    }

    public Config() {

    }

    @Override
    public String toString() {
        return "Config{" +
                "proid=" + proid +
                ", mmac='" + mmac + '\'' +
                '}';
    }

    public String getProid() {
        return proid;
    }

    public void setProid(String proid) {
        this.proid = proid;
    }

    public String getMmac() {
        return mmac;
    }

    public void setMmac(String mmac) {
        this.mmac = mmac;
    }
}
