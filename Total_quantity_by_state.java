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
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;


public class Total_quantity_by_state {
		
	public static class  StateSalesMapper extends Mapper<LongWritable,Text, Text, LongWritable> 
	{
		protected void map(LongWritable key, Text value, Context context)
	            throws java.io.IOException, InterruptedException {
			String row = value.toString();
        	String[] tokens = row.split(",");
        	int itemid=Integer.parseInt(tokens[1]) ;
        	int quantity=Integer.parseInt(tokens[3]);
        	String itemidPLUSstate=itemid+","+tokens[4]; //state
        	context.write(new Text(itemidPLUSstate), new LongWritable(quantity)); //(itemid(quantity,state)) //item.state.quantity
        	
			
		}
	
	}
	public static class StateSalesPartitioner extends
	   Partitioner < Text, LongWritable >
	   {
	      @Override
	      public int getPartition(Text key, LongWritable value, int numReduceTasks)
	      {
	         String[] str = key.toString().split(",");
	      if(str[1].equals("MAH"))
	            return 0;
	         else 
	            return 1 ;
	         
	      }
	   }
	public static class  StateSalesReducer extends Reducer  <Text,LongWritable, Text, LongWritable> 
	{
	
		protected void reduce (Text key, Iterable <LongWritable> values, Context context)
	            throws java.io.IOException, InterruptedException {
					int sum=0;
			for(LongWritable val:values)
				sum=(int) (sum + val.get());
			context.write(new Text(key), new LongWritable(sum));
		}
	}
public static void main(String args[]) throws  IOException, ClassNotFoundException, InterruptedException {
	Configuration conf = new Configuration();
    //conf.set("name", "value")
    Job job = Job.getInstance(conf, "Total Sales by State");
    job.setJarByClass(Total_quantity_by_state.class);
    job.setMapperClass(StateSalesMapper.class);
    //job.setCombinerClass(ReduceClass.class);
    job.setPartitionerClass(StateSalesPartitioner.class);
    job.setReducerClass(StateSalesReducer.class);
    job.setNumReduceTasks(2);
    job.setCombinerClass(StateSalesReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(LongWritable.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
}
}