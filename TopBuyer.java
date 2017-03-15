import java.io.IOException;
import java.util.TreeMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;




public class TopBuyer {
	public static class TopBuyerMap extends
	Mapper<LongWritable, Text, Text, LongWritable> {
		private TreeMap<Long, Text> repToRecordMap = new TreeMap<Long, Text>();

		public void map(LongWritable key, Text value, Context context
        ) throws IOException, InterruptedException {
			String record = value.toString();
			String[] parts = record.split(";");
			String custid=parts[1];
			long amount = Long.parseLong(parts[8]);
			context.write(new Text(custid), new LongWritable(amount));
		}
	
	}
	public static class TopBuyerReduce extends
	Reducer<Text, LongWritable, Text, LongWritable> {
		private TreeMap<Long, Text> repToRecordMap = new TreeMap<Long, Text>();
		public void reduce(Text key, Iterable <LongWritable> values, Context context) throws IOException, InterruptedException
		{
	long sum=0;
	for(LongWritable val :values)
		sum=sum+val.get();
	context.write(key, new LongWritable(sum));
	}
	
		}
		public static void main(String[] args) throws Exception {
			
			Configuration conf = new Configuration();
			Job job = Job.getInstance(conf, "Top Buyer");
		    job.setJarByClass(TopBuyer.class);
		    job.setMapperClass(TopBuyerMap.class);
		    job.setReducerClass(TopBuyerReduce.class);
		    
		    job.setOutputKeyClass(Text.class);
		    job.setOutputValueClass(LongWritable.class);
		    FileInputFormat.addInputPath(job, new Path(args[0]));
		    FileOutputFormat.setOutputPath(job, new Path(args[1]));
		    System.exit(job.waitForCompletion(true) ? 0 : 1);
		  }

}
	

