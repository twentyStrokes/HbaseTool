package getHbase;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Hbase操作demo
 *
 * @author jerry
 * @create 2018/11/27 9:40
 */
public class HbaseCase {

    public static void main(String[] args) {

        //getTableNames();

        //List<Result> tableValues = getTableValuesByRowkey("UAT:TX_LOG","00021_015874054417_518976857286310",5);
        //List<Result> tableValues = getTableValuesByRowkey("UAT:URMTPINF","0000_000000005329",5);
        //System.out.print(tableValues);

        //String str = " ";
        //System.out.println(str.length());

        //List<Result> tableValues = getTableValues("UAT:TX_LOG");
        //System.out.print(tableValues);
        // UAT:URMTPINF  md5对用户号取前四位+ "_"+用户号左补0的12位
        //String urm_rowkey = DigestUtils.md5Hex("1593811").substring(0, 4) + "_"+ StringUtils.leftPad("1573514", 12, "0");

        // UAT:PWMTQPAG	 md5对用户号取前四位+ "_"+用户号+ "_"+协议号
        //String pwm_rowkey = DigestUtils.md5Hex("1593811").substring(0, 4) + "_" + "1593811" + "_" + "10008100000000114183";

        Map<String,String> testMap1 = getElement("UAT:URMTPINF", "1593811", null);
        //Map<String,String> testMap = getElement("UAT:PWMTQPAG", "1593811", "10008100000000114183");



    }

    /**
     * IDEA本地程序查询Hbase的表名
     * @throws IOException
     */
    public static void getTableNames() {

        try {
            Configuration configuration = HBaseConfiguration.create();
            Connection connection = ConnectionFactory.createConnection(configuration);
            Admin admin = connection.getAdmin();
            TableName[] tableNames = admin.listTableNames();
            for ( TableName strings : tableNames) {
                System.out.println("IDEA本地程序查询Hbase的表名： "+strings.getNameAsString());
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * scan获取数据
     * 从startRow这个rowkey开始的count条数据
     * @param tableName 表名
     * @param startRow rowkey
     * @param count 条数
     * @return result数组
     */
    private static List<Result> getTableValuesByRowkey(String tableName, String startRow,Integer count) {
        try {
            Configuration configuration = HBaseConfiguration.create();
            Connection connection = ConnectionFactory.createConnection(configuration);
            Table table = connection.getTable(TableName.valueOf(tableName));


            Scan scan = new Scan();
            List<Result> results = new ArrayList<>();

            scan.setCacheBlocks(true);
            scan.setCaching(10000);
            scan.setStartRow(Bytes.toBytes(startRow));

            PageFilter filter = new PageFilter(count);
            scan.setFilter(filter);

            ResultScanner scanner = table.getScanner(scan);


            for (Result result: scanner ) {
                results.add(result);
                System.out.println("查出的数据： "+result);
                for (Cell cell : result.listCells()) {
                    // 在更早的版本中是使用KeyValue类来实现，但是KeyValue在0.98中已经废弃了，改用Cell
                    // cell.getRowArray()    得到数据的byte数组
                    // cell.getRowOffset()    得到rowkey在数组中的索引下标
                    // cell.getRowLength()    得到rowkey的长度
                    // 将rowkey从数组中截取出来并转化为String类型
                    String value = Bytes.toString(cell.getValueArray(),cell.getValueOffset(),cell.getValueLength());
                    String columnFamily = Bytes.toString(cell.getFamilyArray(),cell.getFamilyOffset(),cell.getFamilyLength());
                    String columnqualifier = Bytes.toString(cell.getQualifierArray(),cell.getQualifierOffset(),cell.getQualifierLength());
                    System.out.println("Cell: " + columnqualifier
                            + ", Value: " + value
                            + ", timestamp:"+cell.getTimestamp()+", value.length:" + value.length());
                }
            }
            scanner.close();
            return results;
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    /**
     * 不用rowkey，全局带条件扫描查数据
     * @param tableName 表名
     * @return result数组
     */
    private static List<Result> getTableValues(String tableName) {

        try {
            Configuration configuration = HBaseConfiguration.create();
            Connection connection = ConnectionFactory.createConnection(configuration);
            Table table = connection.getTable(TableName.valueOf(tableName));

            Scan scan = new Scan();
            List<Result> results = new ArrayList<>();



            List<Filter> filters = new ArrayList<>();
            Filter t1 = new SingleColumnValueFilter(Bytes.toBytes("TL"),
                    Bytes.toBytes("TX_TYP"),
                    CompareFilter.CompareOp.EQUAL,
                    Bytes.toBytes("05"));
            ((SingleColumnValueFilter) t1).setFilterIfMissing(true);
            filters.add(t1);

            Filter t2 = new SingleColumnValueFilter(Bytes.toBytes("TL"),
                    Bytes.toBytes("PAY_MOD"),
                    CompareFilter.CompareOp.EQUAL,
                    Bytes.toBytes("8"));
            ((SingleColumnValueFilter) t2).setFilterIfMissing(true);
            filters.add(t2);

            //默认所有过滤条件需全部满足
            FilterList filterList = new FilterList(filters);
            scan.setFilter(filterList);


            /*List<Long> longs = new ArrayList<>();
            longs.add(1539328114154L);
            longs.add(1539328113376L);
            Filter t3 = new TimestampsFilter(longs);
            filters.add(t3);*/


            // 相当于 LIMIT => 1
            // scan.setLimit(2);
            scan.setTimeRange(1539328113000L,1539328114200L);

            ResultScanner scanner = table.getScanner(scan);



            for (Result r: scanner ) {
                results.add(r);
                System.out.println("查出的数据： "+r);
                for (Cell ce : r.listCells()) {
                    // 在更早的版本中是使用KeyValue类来实现，但是KeyValue在0.98中已经废弃了，改用Cell
                    // cell.getRowArray()    得到数据的byte数组
                    // cell.getRowOffset()    得到rowkey在数组中的索引下标
                    // cell.getRowLength()    得到rowkey的长度
                    // 将rowkey从数组中截取出来并转化为String类型
                    String value = Bytes.toString(ce.getValueArray(),ce.getValueOffset(),ce.getValueLength());
                    String columnFamily = Bytes.toString(ce.getFamilyArray(),ce.getFamilyOffset(),ce.getFamilyLength());
                    String columnqualifier = Bytes.toString(ce.getQualifierArray(),ce.getQualifierOffset(),ce.getQualifierLength());
                    System.out.println(columnqualifier+":"+value+"/timestamp:"+ce.getTimestamp());
                }
            }
            scanner.close();
            return results;
        } catch (IOException e) {
            e.printStackTrace();
        }

        return null;

    }


    public static Map<String,String> getElement(String tableName, String usrNo, String agrNo) {

        Map<String,String> hbasedate = new HashMap<String,String>();
        String rowkey;
        if (tableName.equals("UAT:URMTPINF") && StringUtils.isNotEmpty(usrNo)) {
            rowkey = DigestUtils.md5Hex(usrNo).substring(0, 4) + "_"+ org.apache.commons.lang.StringUtils.leftPad(usrNo, 12, "0");
        } else if (tableName.equals("UAT:PWMTQPAG") && StringUtils.isNotEmpty(usrNo) && StringUtils.isNotEmpty(agrNo)) {
            rowkey = DigestUtils.md5Hex(usrNo).substring(0, 4) + "_" + usrNo + "_" + agrNo;
        } else {
            return null;
        }

        try {
            Configuration configuration = HBaseConfiguration.create();
            Connection connection = ConnectionFactory.createConnection(configuration);
            Table table = connection.getTable(TableName.valueOf(tableName));
            Get get = new Get(Bytes.toBytes(rowkey));
            Result result = table.get(get);
            for (Cell cell : result.listCells()) {
                String value = Bytes.toString(cell.getValueArray(),cell.getValueOffset(),cell.getValueLength());
                String columnqualifier = Bytes.toString(cell.getQualifierArray(),cell.getQualifierOffset(),cell.getQualifierLength());
                hbasedate.put(columnqualifier,value);
                System.out.println("Cell: " + columnqualifier
                        + ", Value: " +value
                        + ", timestamp:"+cell.getTimestamp());
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return hbasedate;
    }
    /**
     *
     * @param usrNo
     * @param agrNo
     *//*
    public static Map<String,String> getElement(String tableName, String usrNo, String agrNo) {

        Map<String,String> hbasedate = new HashMap<String,String>();


        String urm_TableName = "UAT:URMTPINF";
        String pwm_TableName = "UAT:PWMTQPAG";

        // UAT:URMTPINF  md5对用户号取前四位+ "_"+用户号左补0的12位
        String urm_rowkey = DigestUtils.md5Hex(usrNo).substring(0, 4) + "_"+ StringUtils.leftPad(usrNo, 12, "0");

        // UAT:PWMTQPAG	 md5对用户号取前四位+ "_"+用户号+ "_"+协议号
        String pwm_rowkey = DigestUtils.md5Hex(usrNo).substring(0, 4) + "_" + usrNo + "_" + agrNo;

        try {
            Configuration configuration = HBaseConfiguration.create();
            Connection connection = ConnectionFactory.createConnection(configuration);
            Table table = connection.getTable(TableName.valueOf(tableName));
            Get get = new Get(Bytes.toBytes(pwm_rowkey));
            Result result = table.get(get);

            for (Cell cell : result.listCells()) {
                String value = Bytes.toString(cell.getValueArray(),cell.getValueOffset(),cell.getValueLength());
                String columnqualifier = Bytes.toString(cell.getQualifierArray(),cell.getQualifierOffset(),cell.getQualifierLength());
                hbasedate.put(columnqualifier,value);
                System.out.println("Cell: " + columnqualifier
                        + ", Value: " +value
                        + ", timestamp:"+cell.getTimestamp());
            }


        } catch (IOException e) {
            e.printStackTrace();
        }
        return hbasedate;
    }*/
}
