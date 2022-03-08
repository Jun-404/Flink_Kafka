package util;

import bean.DetailBean;
import bean.Dws_n_st_basic;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DimUtil extends RichMapFunction<DetailBean, Dws_n_st_basic> {

//    private static final Logger LOGGER = LoggerFactory.getLogger(DimUtil.class);
//    ScheduledExecutorService executor = null;
        Map<String, String> dim;

//    @Override
//    public void open(Configuration parameters) throws Exception {
//        super.open(parameters);
//        executor.scheduleAtFixedRate(new Runnable() {
//            @Override
//            public void run() {
//                try {
//                    load();
//                } catch (Exception e) {
//                    e.printStackTrace();
//                }
//            }
//        }, 2, 2, TimeUnit.SECONDS);
//    }


    @Override
    public void open(Configuration parameters) throws Exception {
        dim = new HashMap<>();
        JdbcUtil jdbcUtil = new JdbcUtil();
        Class.forName("com.mysql.cj.jdbc.Driver");
        Connection connection = DriverManager.getConnection("jdbc:mysql://172.30.12.108:3306/bgd?serverTimezone=UTC&useUnicode=true&characterEncoding=utf-8&useSSL=false&user=root&password=i@SH021#bg");
        List<JSONObject> queryList = jdbcUtil.queryList(connection, "select * from flinkConfig", JSONObject.class, false);
        for (JSONObject jsonObject : queryList) {
            System.out.println(jsonObject);
            String proid = jsonObject.getString("proid");
            String mmac = jsonObject.getString("mmac");
            dim.put(mmac,proid);
        }

        connection.close();

}

//    public void load() throws Exception {
//        Class.forName("com.mysql.jdbc.Driver");
//        Connection con = DriverManager.getConnection("jdbc:mysql://172.30.12.108:3306/bgd?serverTimezone=UTC&useUnicode=true&characterEncoding=utf-8&useSSL=false&user=root&password=i@SH021#bg");
//        PreparedStatement statement = con.prepareStatement("select proid,mmac from flinkConfig");
//        ResultSet rs = statement.executeQuery();
//        while (rs.next()) {
//            String proid = rs.getString("proid");
//            String mmac = rs.getString("mmac");
//            config.setMmac(mmac);
//            config.setProid(proid);
//        }
//        con.close();
//    }


    @Override
    public Dws_n_st_basic map(DetailBean detailBean) throws Exception {
        String dataid = "";
        String ranges = "";
        String rssi0 = "";
        Long dtime = 0L;
        Long in_time = 0L;
        String mmac = "";
        String rid = "";
        String mac = "";
        String proid = "";
        if (dim.containsKey(detailBean.getMmac())/*.getMmac().equals(detailBean.getMmac())*/) {
            dataid = detailBean.getDataid();
            ranges = detailBean.getRanges();
            rssi0 = detailBean.getRssi0();
            dtime = detailBean.getDtime();
            in_time = detailBean.getIn_time();
            mmac = detailBean.getMmac();
            rid = detailBean.getRid();
            mac = detailBean.getMac();
            proid = dim.get(detailBean.getMmac());
        }else{
            dataid = detailBean.getDataid();
            ranges = detailBean.getRanges();
            rssi0 = detailBean.getRssi0();
            dtime = detailBean.getDtime();
            in_time = detailBean.getIn_time();
            mmac = detailBean.getMmac();
            rid = detailBean.getRid();
            mac = detailBean.getMac();
            proid = "000";
        }
        return new Dws_n_st_basic(dataid,ranges,rssi0,dtime,in_time,mmac,rid,mac,proid);
    }
}
