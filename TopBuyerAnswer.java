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
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class TopBuyerAnswer {
	public static class TopBuyerAnswerMapper extends
	Mapper<LongWritable, Text, LongWritable, Text> {
		private TreeMap<Long, Text> repToRecordMap = new TreeMap<Long, Text>();
		int count=-1;
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException 
		{
			String record = value.toString();
			String[] parts = record.split("\t");
			long amount = Long.parseLong(parts[1]);
			String cust_id=(parts[0]);
//			repToRecordMap.put(amount, new Text(cust_id));
//			if (repToRecordMap.size() > 1) 
//				repToRecordMap.remove(repToRecordMap.firstKey());
//			context.write(new  LongWritable((int)repToRecordMap.keySet().toArray()[0]), new Text(repToRecordMap.get(0)));
			context.write(new LongWritable(amount),new Text(cust_id));
		}
	}

	public static class TopBuyerAnswerReducer extends Reducer <LongWritable, Text, LongWritable, Text> {
		int count=0;
		public void reduce(LongWritable key, Text value, Context context) throws IOException, InterruptedException
		{
		if(--count==0)
			context.write(key, value);
		
		}
	}
	
	
	public static void main(String[] args) throws Exception {
		
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "Top 5 Records");
	    job.setJarByClass(TopBuyerAnswer.class);
	    job.setMapperClass(TopBuyerAnswerMapper.class);
	    job.setReducerClass(TopBuyerAnswerReducer.class);
//	    job.setNumReduceTasks(0);
	    job.setOutputKeyClass(LongWritable.class);
	    job.setOutputValueClass(Text.class);
	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	    System.exit(job.waitForCompletion(true) ? 0 : 1);
	  }
}