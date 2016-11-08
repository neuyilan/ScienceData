package  ict.science.test;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat;
import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles;
import org.apache.hadoop.hbase.mapreduce.PutSortReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class BulkloadMR {
    public static class BulkM extends Mapper<Object, Text, ImmutableBytesWritable, Put> {
        public final static String SP = "\t";

        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            // value切分
            String[] values = value.toString().split(SP);
            if (values.length == 2) {
                byte[] rowkey = Bytes.toBytes(values[0]);
                byte[] c_v = Bytes.toBytes(values[1]);
                byte[] family = Bytes.toBytes("d");
                byte[] cloumn = Bytes.toBytes("c");
                ImmutableBytesWritable rowkeyWriable = new ImmutableBytesWritable(rowkey);
                Put put = new Put(rowkey);
                put.add(family, cloumn, c_v);
                context.write(rowkeyWriable, put);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        // args0 dst
        // args1 out
        // args2 split MB
        // args3 hbase table name
        if (args.length != 4) {
            System.exit(0);
        }
        String dst = args[0];
        String out = args[1];
        int SplitMB = Integer.valueOf(args[2]);
        String table_name = args[3];
        // 设置属性对应参数
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://data18:9000");
		conf.set("hbase.zookeeper.quorum", "data17,data19,data20");
		conf.set("hbase.zookeeper.property.clientPort", "2181");
        conf.set("mapreduce.input.fileinputformat.split.maxsize", String.valueOf(SplitMB * 1024 * 1024));
        conf.set("mapred.min.split.size", String.valueOf(SplitMB * 1024 * 1024));
        conf.set("mapreduce.input.fileinputformat.split.minsize.per.node", String.valueOf(SplitMB * 1024 * 1024));
        conf.set("mapreduce.input.fileinputformat.split.minsize.per.rack", String.valueOf(SplitMB * 1024 * 1024));
        HBaseConfiguration.addHbaseResources(conf);
        Job job = new Job(conf, "BulkLoad");
        job.setJarByClass(BulkloadMR.class);
        job.setMapperClass(BulkM.class);
        job.setReducerClass(PutSortReducer.class);
        job.setOutputFormatClass(HFileOutputFormat.class);
        job.setMapOutputKeyClass(ImmutableBytesWritable.class);
        job.setMapOutputValueClass(Put.class);
        FileInputFormat.addInputPath(job, new Path(dst));
        FileOutputFormat.setOutputPath(job, new Path(out));
        // MapReudce 初始化一张表
//        Configuration hbaseConf = HBaseConfiguration.create();
        HTable table = new HTable(conf, table_name);
        HFileOutputFormat.configureIncrementalLoad(job, table);
        //执行job任务
        job.waitForCompletion(true);
        //将HFile文件导入HBase
        LoadIncrementalHFiles loader = new LoadIncrementalHFiles(conf);
        loader.doBulkLoad(new Path(out), table);
    }
}