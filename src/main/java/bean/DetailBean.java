package bean;

public class DetailBean {

    @Override
    public String toString() {
        return "DetailBean{" +
                "dataid='" + dataid + '\'' +
                ", ranges='" + ranges + '\'' +
                ", rssi0='" + rssi0 + '\'' +
                ", dtime=" + dtime +
                ", in_time=" + in_time +
                ", mmac='" + mmac + '\'' +
                ", rid='" + rid + '\'' +
                ", mac='" + mac + '\'' +
                '}';
    }

    public String getDataid() {
        return dataid;
    }

    public void setDataid(String dataid) {
        this.dataid = dataid;
    }

    public String getRanges() {
        return ranges;
    }

    public void setRanges(String ranges) {
        this.ranges = ranges;
    }

    public String getRssi0() {
        return rssi0;
    }

    public void setRssi0(String rssi0) {
        this.rssi0 = rssi0;
    }

    public Long getDtime() {
        return dtime;
    }

    public void setDtime(Long dtime) {
        this.dtime = dtime;
    }

    public Long getIn_time() {
        return in_time;
    }

    public void setIn_time(Long in_time) {
        this.in_time = in_time;
    }

    public String getMmac() {
        return mmac;
    }

    public void setMmac(String mmac) {
        this.mmac = mmac;
    }

    public String getRid() {
        return rid;
    }

    public void setRid(String rid) {
        this.rid = rid;
    }

    public String getMac() {
        return mac;
    }

    public void setMac(String mac) {
        this.mac = mac;
    }

    private String dataid;
    private String ranges;
    private String rssi0;
    private Long dtime;
    private Long in_time;
    private String mmac;
    private String rid;
    private String mac;
}
