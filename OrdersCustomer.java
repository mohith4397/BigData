//1
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

	public class OrdersCustomer
	{
		public static class OrdersCustomerMapper extends
		Mapper<LongWritable, Text, Text,Text > 
		{
			public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException 
			{
				String[] record = value.toString().split(";");
				context.write(new Text(record[1]), new Text(record[8]));
			}
		}
		public static class OrdersCustomerReducer extends Reducer <Text, Text, NullWritable, Text> {
			 private TreeMap<LongWritable, Text> OrdersCustomerMap = new TreeMap<LongWritable, Text>();
			long SUMsales=0;String answer=null;int count=0;
			public void reduce(Text key, Iterable <Text> value, Context context) throws IOException, InterruptedException
			{ long orders=0;
				for(Text val:value)
				 {
					orders++;
					String[] record = val.toString().split(",");
					SUMsales=SUMsales+Long.parseLong(record[0]);
					answer=key.toString()+","+Long.toString(SUMsales)+","+String.valueOf(orders);
					
				 }
				OrdersCustomerMap.put(new LongWritable(SUMsales),new Text(answer));
				if (OrdersCustomerMap.size()>5)
					OrdersCustomerMap.remove(OrdersCustomerMap.firstKey());
			}
				protected  void cleanup (Context context) throws IOException, InterruptedException  
				{
					for (Text t : OrdersCustomerMap.descendingMap().values()) 
					context.write(NullWritable.get(), t);
				}
		}
		
		public static void main(String args[]) throws  IOException, ClassNotFoundException, InterruptedException
		{
			Configuration conf = new Configuration();
			Job job = Job.getInstance(conf, "Orders Customer");
		    job.setJarByClass(OrdersCustomer.class);
		    job.setMapperClass(OrdersCustomerMapper.class);
		    job.setReducerClass(OrdersCustomerReducer.class);
		    		    
		    job.setMapOutputKeyClass(Text.class);
		    job.setMapOutputValueClass(Text.class);
		    
		    job.setOutputKeyClass(NullWritable.class);
		    job.setOutputValueClass(Text.class);
		    
		    FileInputFormat.addInputPath(job, new Path(args[0]));
		    FileOutputFormat.setOutputPath(job, new Path(args[1]));
		    System.exit(job.waitForCompletion(true) ? 0 : 1);
		}
	
	
	}
