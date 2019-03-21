package ExportHbaseToParquet;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import parquet.avro.AvroParquetOutputFormat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;


/**
 * 通过mapreduce任务，将Hbase数据抽取到hdfs上，保存成parquet。
 * PRD:TX_LOG
 *
 * @author jerry
 * @create 2018/12/20 21:43
 */
public class ExportTxLog {

    private static final Schema SCHEMA = new Schema.Parser().parse(
            "{\n" +
                    "\"type\":\"record\",\n" +
                    "\"name\":\"filerHbase\",\n" +
                    "\"fields\":[\n" +
                    "{\"name\":\"AC_PAY_FLG\",\"type\":\"string\"},\n" +
                    "{\"name\":\"AC_TYP\",\"type\":\"string\"},\n" +
                    "{\"name\":\"AGE\",\"type\":\"string\"},\n" +
                    "{\"name\":\"BNK_CUS_NM\",\"type\":\"string\"},\n" +
                    "{\"name\":\"BNK_MBL_CITY_CD\",\"type\":\"string\"},\n" +
                    "{\"name\":\"BNK_MBL_NO\",\"type\":\"string\"},\n" +
                    "{\"name\":\"BNK_MBL_PRV_CD\",\"type\":\"string\"},\n" +
                    "{\"name\":\"BUS_CNL\",\"type\":\"string\"},\n" +
                    "{\"name\":\"BUS_TYP\",\"type\":\"string\"},\n" +
                    "{\"name\":\"CRD_CITY_CD\",\"type\":\"string\"},\n" +
                    "{\"name\":\"CRD_NO\",\"type\":\"string\"},\n" +
                    "{\"name\":\"CRD_PRV_CD\",\"type\":\"string\"},\n" +
                    "{\"name\":\"FINANCE_FLG\",\"type\":\"string\"},\n" +
                    "{\"name\":\"HIGH_RISK_LVL\",\"type\":\"string\"},\n" +
                    "{\"name\":\"IDNO_CITY_CD\",\"type\":\"string\"},\n" +
                    "{\"name\":\"IDNO_PRV_CD\",\"type\":\"string\"},\n" +
                    "{\"name\":\"ID_NO\",\"type\":\"string\"},\n" +
                    "{\"name\":\"IMEI\",\"type\":\"string\"},\n" +
                    "{\"name\":\"IP\",\"type\":\"string\"},\n" +
                    "{\"name\":\"IP_CITY_CD\",\"type\":\"string\"},\n" +
                    "{\"name\":\"IP_PRV_CD\",\"type\":\"string\"},\n" +
                    "{\"name\":\"IS_BOUND_BANK_FLG\",\"type\":\"string\"},\n" +
                    "{\"name\":\"IS_IP_RISK\",\"type\":\"string\"},\n" +
                    "{\"name\":\"IS_S_TERMINAL\",\"type\":\"string\"},\n" +
                    "{\"name\":\"JRN_NO\",\"type\":\"string\"},\n" +
                    "{\"name\":\"MBL_MAC\",\"type\":\"string\"},\n" +
                    "{\"name\":\"MBL_NO\",\"type\":\"string\"},\n" +
                    "{\"name\":\"MBL_PROVIDER\",\"type\":\"string\"},\n" +
                    "{\"name\":\"OPT_AC_TYP\",\"type\":\"string\"},\n" +
                    "{\"name\":\"OPT_BNK_CUS_NM\",\"type\":\"string\"},\n" +
                    "{\"name\":\"OPT_CRD_NO\",\"type\":\"string\"},\n" +
                    "{\"name\":\"OPT_DC_FLG\",\"type\":\"string\"},\n" +
                    "{\"name\":\"OPT_ID_NO\",\"type\":\"string\"},\n" +
                    "{\"name\":\"OPT_MBL_NO\",\"type\":\"string\"},\n" +
                    "{\"name\":\"OPT_USR_NO\",\"type\":\"string\"},\n" +
                    "{\"name\":\"ORD_NO\",\"type\":\"string\"},\n" +
                    "{\"name\":\"ORD_TYP\",\"type\":\"string\"},\n" +
                    "{\"name\":\"PAY_MOD\",\"type\":\"string\"},\n" +
                    "{\"name\":\"PAY_SAFE_AUTH_FLG\",\"type\":\"string\"},\n" +
                    "{\"name\":\"RST_OPR_TYP\",\"type\":\"string\"},\n" +
                    "{\"name\":\"ST_DC_FLG\",\"type\":\"string\"},\n" +
                    "{\"name\":\"TERML_ATTR\",\"type\":\"string\"},\n" +
                    "{\"name\":\"TERML_CITY_CD\",\"type\":\"string\"},\n" +
                    "{\"name\":\"TERML_FLAG\",\"type\":\"string\"},\n" +
                    "{\"name\":\"TERML_ID\",\"type\":\"string\"},\n" +
                    "{\"name\":\"TERML_PRV_CD\",\"type\":\"string\"},\n" +
                    "{\"name\":\"TM_SMP\",\"type\":\"string\"},\n" +
                    "{\"name\":\"TX_AMT\",\"type\":\"string\"},\n" +
                    "{\"name\":\"TX_AMT1\",\"type\":\"string\"},\n" +
                    "{\"name\":\"TX_AMT9\",\"type\":\"string\"},\n" +
                    "{\"name\":\"TX_CD\",\"type\":\"string\"},\n" +
                    "{\"name\":\"TX_DT\",\"type\":\"string\"},\n" +
                    "{\"name\":\"TX_RST\",\"type\":\"string\"},\n" +
                    "{\"name\":\"TX_TM\",\"type\":\"string\"},\n" +
                    "{\"name\":\"TX_TYP\",\"type\":\"string\"},\n" +
                    "{\"name\":\"USR_CITY\",\"type\":\"string\"},\n" +
                    "{\"name\":\"USR_NO\",\"type\":\"string\"},\n" +
                    "{\"name\":\"USR_PRV_CD\",\"type\":\"string\"},\n" +
                    "{\"name\":\"USR_TYP\",\"type\":\"string\"}\n" +
                    "]\n" +
                    "}");

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        if (args.length != 4 ) {
            System.err.println("Usage: ExportTxLog <tableName> <outputPath> <starttime> <endtime>");
            System.exit(-1);
        }

        String tablename = args[0];
        String outputPath = args[1];
        String starttime = args[2];
        String endtime = args[3];


        Job job = new Job();


        job.setJarByClass(ExportTxLog.class);
        job.setJobName("Export TX_LOG To Parquet");

        Scan scan = new Scan();

        if (tablename.equals("PRD:TX_LOG")) {
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



            if (StringUtils.isNotEmpty(starttime) && StringUtils.isNotEmpty(endtime)) {
                Long start = Long.parseLong(starttime);
                Long end = Long.parseLong(endtime);
                if (end > start) {
                    scan.setTimeRange(start,end);
                }
            }

        }
        scan.setCaching(500);
        scan.setCacheBlocks(false);



        TableMapReduceUtil.initTableMapperJob(tablename, scan,
                FilerHbaseByMapReduceMapper.class, null, null, job);

        job.setOutputFormatClass(AvroParquetOutputFormat.class);
        AvroParquetOutputFormat.setOutputPath(job, new Path(outputPath));

        job.getConfiguration().set("mapreduce.output.basename", "table_TX_LOG");

        AvroParquetOutputFormat.setSchema(job, SCHEMA);


        job.setNumReduceTasks(0);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    private static class FilerHbaseByMapReduceMapper extends TableMapper<Text, GenericRecord> {

        ArrayList<String> columns = new ArrayList<>();
        HashMap<String, String> columnValueMap = new HashMap<>();

        String colvalue;
        String columnqualifier;
        String val;


        @Override
        public void setup(Context context) {

            for (Schema.Field f : SCHEMA.getFields()) {
                columns.add(f.name());
                System.out.println("Field:" + f.name());
            }

        }

        @Override
        public void map(ImmutableBytesWritable row, Result value, Context context) throws InterruptedException, IOException {


            for (Cell ce : value.listCells()) {

                columnqualifier = Bytes.toString(ce.getQualifierArray(),ce.getQualifierOffset(),ce.getQualifierLength());

                val = Bytes.toString(ce.getValueArray(),ce.getValueOffset(),ce.getValueLength());

                columnValueMap.put(columnqualifier, val);
            }

            if (columnValueMap.size() > 0) {

                GenericData.Record record = new GenericData.Record(SCHEMA);

                for (String col : columns) {
                    colvalue = columnValueMap.get(col);

                    if (colvalue != null) {
                        record.put(col, colvalue);
                    } else {
                        record.put(col, " ");
                    }
                }

                context.write(null, record);
            }


        }

    }


}
