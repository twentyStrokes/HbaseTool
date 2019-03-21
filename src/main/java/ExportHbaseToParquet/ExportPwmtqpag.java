package ExportHbaseToParquet;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
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


/**
 * 通过mapreduce任务，将Hbase数据抽取到hdfs上，保存成parquet。
 *
 * PRD:PWMTQPAG
 *
 * @author jerry
 * @create 2018/12/20 21:43
 */
public class ExportPwmtqpag {

    private static final Schema SCHEMA = new Schema.Parser().parse(
            "{\n" +
                    "\"type\":\"record\",\n" +
                    "\"name\":\"filerHbase\",\n" +
                    "\"fields\":[\n" +
                    "{\"name\":\"AC_PAY_FLG\",\"type\":\"string\"},\n" +
                    "{\"name\":\"AGR_EFF_DT\",\"type\":\"string\"},\n" +
                    "{\"name\":\"AGR_EXP_DT\",\"type\":\"string\"},\n" +
                    "{\"name\":\"AGR_TYP\",\"type\":\"string\"},\n" +
                    "{\"name\":\"BNK_MBL_NO\",\"type\":\"string\"},\n" +
                    "{\"name\":\"BUS_CNL\",\"type\":\"string\"},\n" +
                    "{\"name\":\"CAP_CORG_NM\",\"type\":\"string\"},\n" +
                    "{\"name\":\"CAP_CRD_NO\",\"type\":\"string\"},\n" +
                    "{\"name\":\"CRD_AC_TYP\",\"type\":\"string\"},\n" +
                    "{\"name\":\"CRD_EFF_FLG\",\"type\":\"string\"},\n" +
                    "{\"name\":\"ID_NO\",\"type\":\"string\"},\n" +
                    "{\"name\":\"ID_NO2\",\"type\":\"string\"},\n" +
                    "{\"name\":\"ID_TYP\",\"type\":\"string\"},\n" +
                    "{\"name\":\"MBL_NO\",\"type\":\"string\"},\n" +
                    "{\"name\":\"QP_SIGN_TYP\",\"type\":\"string\"},\n" +
                    "{\"name\":\"SIGN_DT\",\"type\":\"string\"},\n" +
                    "{\"name\":\"SIGN_TM\",\"type\":\"string\"},\n" +
                    "{\"name\":\"USR_NM\",\"type\":\"string\"},\n" +
                    "{\"name\":\"USR_NO\",\"type\":\"string\"}\n" +
                    "]\n" +
                    "}");

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        if (args.length != 4) {
            System.err.println("Usage: ExportPwmtqpag <tableName> <outputPath> <starttime> <endtime>");
            System.exit(-1);
        }

        String tablename = args[0];
        String outputPath = args[1];
        String starttime = args[2];
        String endtime = args[3];

        Job job = new Job();


        job.setJarByClass(ExportPwmtqpag.class);
        job.setJobName("Export PWMTQPAG To Parquet");

        Scan scan = new Scan();

        scan.setCaching(500);
        scan.setCacheBlocks(false);
        if (StringUtils.isNotEmpty(starttime) && StringUtils.isNotEmpty(endtime)) {
            Long start = Long.parseLong(starttime);
            Long end = Long.parseLong(endtime);
            if (end > start) {
                scan.setTimeRange(start,end);
            }
        }

        TableMapReduceUtil.initTableMapperJob(tablename, scan,
                FilerHbaseByMapReduceMapper.class, null, null, job);

        job.setOutputFormatClass(AvroParquetOutputFormat.class);
        AvroParquetOutputFormat.setOutputPath(job, new Path(outputPath));

        job.getConfiguration().set("mapreduce.output.basename", "table_PWMTQPAG");

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
