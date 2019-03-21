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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import parquet.avro.AvroParquetOutputFormat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;


/**
 * 通过mapreduce任务，将Hbase数据抽取到hdfs上，保存成parquet。
 * PRD:URMTPINF
 *
 * @author jerry
 * @create 2018/12/20 21:43
 */
public class ExportUrmtpinf {

    private static Logger logger = LoggerFactory.getLogger(ExportUrmtpinf.class);

    private static final Schema SCHEMA = new Schema.Parser().parse(
            "{\n" +
                    "\"type\":\"record\",\n" +
                    "\"name\":\"filerHbase\",\n" +
                    "\"fields\":[\n" +
                    "{\"name\":\"AUTH_DT\",\"type\":\"string\"},\n" +
                    "{\"name\":\"AUTH_TM\",\"type\":\"string\"},\n" +
                    "{\"name\":\"CUS_NM\",\"type\":\"string\"},\n" +
                    "{\"name\":\"ID_NO\",\"type\":\"string\"},\n" +
                    "{\"name\":\"ID_TYP\",\"type\":\"string\"},\n" +
                    "{\"name\":\"REAL_NM_ATH_MOD\",\"type\":\"string\"},\n" +
                    "{\"name\":\"REG_BUS_CNL\",\"type\":\"string\"},\n" +
                    "{\"name\":\"REG_DT\",\"type\":\"string\"},\n" +
                    "{\"name\":\"REG_SYS_CNL\",\"type\":\"string\"},\n" +
                    "{\"name\":\"REG_TM\",\"type\":\"string\"},\n" +
                    "{\"name\":\"USR_CITY\",\"type\":\"string\"},\n" +
                    "{\"name\":\"USR_CITY_NM\",\"type\":\"string\"},\n" +
                    "{\"name\":\"USR_NO\",\"type\":\"string\"},\n" +
                    "{\"name\":\"USR_PROV\",\"type\":\"string\"},\n" +
                    "{\"name\":\"USR_STS\",\"type\":\"string\"},\n" +
                    "{\"name\":\"USR_AC_LEV\",\"type\":\"string\"},\n" +
                    "{\"name\":\"BIRTH_DT\",\"type\":\"string\"},\n" +
                    "{\"name\":\"IDNO_PROV\",\"type\":\"string\"},\n" +
                    "{\"name\":\"IDNO_CITY\",\"type\":\"string\"},\n" +
                    "{\"name\":\"SEX\",\"type\":\"string\"}\n" +
                    "]\n" +
                    "}");

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        if (args.length != 4) {
            System.err.println("Usage: ExportUrmtpinf <tableName> <outputPath> <starttime> <endtime>");
            System.exit(-1);
        }

        String tablename = args[0];
        String outputPath = args[1];
        String starttime = args[2];
        String endtime = args[3];

        Job job = new Job();


        job.setJarByClass(ExportUrmtpinf.class);
        job.setJobName("Export URMTPINF To Parquet");

        Scan scan = new Scan();

        scan.setCaching(500);
        scan.setCacheBlocks(false);
        if (tablename.equals("PRD:URMTPINF")) {
            List<Filter> filters = new ArrayList<>();
            Filter t1 = new SingleColumnValueFilter(Bytes.toBytes("USR_INF"),
                    Bytes.toBytes("AUTH_DT"),
                    CompareFilter.CompareOp.NOT_EQUAL,
                    Bytes.toBytes(""));
            ((SingleColumnValueFilter) t1).setFilterIfMissing(true);

            Filter t2 = new SingleColumnValueFilter(Bytes.toBytes("USR_INF"),
                    Bytes.toBytes("AUTH_DT"),
                    CompareFilter.CompareOp.NOT_EQUAL,
                    Bytes.toBytes(" "));
            ((SingleColumnValueFilter) t2).setFilterIfMissing(true);


            Filter t3 = new SingleColumnValueFilter(Bytes.toBytes("USR_INF"),
                    Bytes.toBytes("AUTH_TM"),
                    CompareFilter.CompareOp.NOT_EQUAL,
                    Bytes.toBytes(""));
            ((SingleColumnValueFilter) t3).setFilterIfMissing(true);

            Filter t4 = new SingleColumnValueFilter(Bytes.toBytes("USR_INF"),
                    Bytes.toBytes("AUTH_TM"),
                    CompareFilter.CompareOp.NOT_EQUAL,
                    Bytes.toBytes(" "));
            ((SingleColumnValueFilter) t4).setFilterIfMissing(true);

            filters.add(t1);
            filters.add(t2);
            filters.add(t3);
            filters.add(t4);

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

        TableMapReduceUtil.initTableMapperJob(tablename, scan,
                FilerHbaseByMapReduceMapper.class, null, null, job);

        job.setOutputFormatClass(AvroParquetOutputFormat.class);
        AvroParquetOutputFormat.setOutputPath(job, new Path(outputPath));

        job.getConfiguration().set("mapreduce.output.basename", "table_URMTPINF");


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
