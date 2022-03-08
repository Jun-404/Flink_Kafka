package bean;

public class UvCount {
    private String mmac;
    private Long total;
    private Long startTime;

    public Long getStartTime() {
        return startTime;
    }

    public void setStartTime(Long startTime) {
        this.startTime = startTime;
    }

    public Long getEndTime() {
        return endTime;
    }

    public void setEndTime(Long endTime) {
        this.endTime = endTime;
    }

    private Long endTime;

    public UvCount(String end, Long size,Long startTime,Long endTime) {
        this.mmac =  end;
        this.total = size;
        this.startTime = startTime;
        this.endTime = endTime;
    }

    public String getMmac() {
        return mmac;
    }

    public void setMmac(String mmac) {
        this.mmac = mmac;
    }

    public Long getTotal() {
        return total;
    }

    public void setTotal(Long total) {
        this.total = total;
    }

    @Override
    public String toString() {
        return "UvCount{" +
                "mmac='" + mmac + '\'' +
                ", total=" + total +
                ", startTime=" + startTime +
                ", endTime=" + endTime +
                '}';
    }
}
