//10
import java.io.IOException;
import java.util.TreeMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
public class AverageSales {
	public static class AverageSalesMapper extends
	Mapper<LongWritable, Text, Text,Text > 
	{
		
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException 
		{
			String[] record = value.toString().split(";");
			Text finalkey=new Text(record[5]); //prodid
			String finalvalue=record[8]+","+record[6];
			context.write(finalkey, new Text(finalvalue));
		}
	}
	public static class AverageSalesReducer extends
	Reducer<Text, Text, NullWritable,Text > 
	{long SUMtotalcost=0,SUMtotalqty=0;
	
	private TreeMap <DoubleWritable, Text> AvgMap = new TreeMap<DoubleWritable, Text>();
		public void reduce(Text key, Iterable <Text> value, Context context) throws IOException, InterruptedException
		{
			for (Text val:value)
			{
				String[] record=val.toString().split(",");
				SUMtotalcost=SUMtotalcost+Long.parseLong(record[0]);
				SUMtotalqty=SUMtotalqty+Long.parseLong(record[1]);
			}
			Double average=(double)SUMtotalcost/(double)SUMtotalqty;
			String answer= (average+","+SUMtotalcost+","+SUMtotalqty);
		
			AvgMap.put(new DoubleWritable(average),new Text(answer));
			if (AvgMap.size()>5)
				AvgMap.remove(AvgMap.firstKey());
		}
			protected  void cleanup (Context context) throws IOException, InterruptedException  
			{
				for (Text t : AvgMap.descendingMap().values()) 
				context.write(NullWritable.get(), t);
			}
			
		
	}
	public static void main(String args[]) throws  IOException, ClassNotFoundException, InterruptedException
	{
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "Sales/Quantity");
	    job.setJarByClass(AverageSales.class);
	    job.setMapperClass(AverageSalesMapper.class);
	    job.setReducerClass(AverageSalesReducer.class);
	    
	    
	    job.setMapOutputKeyClass(Text.class);
	    job.setMapOutputValueClass(Text.class);
	    
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(Text.class);
	    
	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	    System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}