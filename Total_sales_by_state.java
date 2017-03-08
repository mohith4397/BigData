import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.Reducer;

public class Total_sales_by_state {
		
	public static class  StateSalesMapper extends Mapper<LongWritable,Text, Text, IntWritable> 
	{
		protected void map(LongWritable key, Text value, Context context)
	            throws java.io.IOException, InterruptedException {
			String row = value.toString();
        	String[] tokens = row.split(",");
        	int quantity=Integer.parseInt(tokens[2]) * Integer.parseInt(tokens[3]);
        	context.write(new Text(tokens[4]), new IntWritable(quantity));
        	
			
		}
	
	}
	public static class  StateSalesReducer extends Reducer  <Text,IntWritable, Text, IntWritable> 
	{
	
		protected void reduce (Text key, Iterable <IntWritable> values, Context context)
	            throws java.io.IOException, InterruptedException {
		
			int sum=0;
			for(IntWritable val:values)
			sum=sum + val.get();
			context.write(key, new IntWritable(sum));
		}
	}
public static void main(String args[]) throws  IOException, ClassNotFoundException, InterruptedException {
	Configuration conf = new Configuration();
    //conf.set("name", "value")
    Job job = Job.getInstance(conf, "Total Sales by State");
    job.setJarByClass(Total_sales_by_state.class);
    job.setMapperClass(StateSalesMapper.class);
    //job.setCombinerClass(ReduceClass.class);
    job.setReducerClass(StateSalesReducer.class);
    job.setCombinerClass(StateSalesReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
}
}