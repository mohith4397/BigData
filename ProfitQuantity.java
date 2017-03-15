import java.io.IOException;
import java.util.TreeMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
public class ProfitQuantity {
 public static class ProfitQuantityMapper extends Mapper<LongWritable,Text,LongWritable,Text>
 {
	protected void map(LongWritable Text,Text value,Context context) throws IOException, InterruptedException
	{
	String[] record=value.toString().split(";");
	LongWritable finalkey=new LongWritable(Long.parseLong(record[4]));
	String finalvalue=record[8]+","+record[7]+","+record[6];
	context.write(finalkey, new Text(finalvalue));
	}
 }
 public static class ProfitQuantityReducer extends Reducer<LongWritable,Text,LongWritable,Text>
 {
	 
	 private TreeMap<Double, Text> ProfitMap = new TreeMap<Double, Text>();
	 
	 long SUMoftotalsale=0,SUMoftotalcost=0,SUMofquantity=0,profit=0;
	 String answer; double margin=0;
	 
	 int count=0;
	 
	 protected void reduce(LongWritable key,Iterable <Text> value,Context context) throws IOException, InterruptedException
		{
		 for(Text val:value)
		 {
			 String[] record=val.toString().split(",");
			 SUMoftotalsale=SUMoftotalsale+Long.parseLong(record[0]);
			 SUMoftotalcost=SUMoftotalcost+Long.parseLong(record[1]);
			 SUMofquantity=SUMofquantity+Long.parseLong(record[2]);
			 profit=SUMoftotalsale-SUMoftotalcost;
			 margin=(profit*100)/SUMoftotalcost;
			 answer=String.valueOf(profit)+","+String.valueOf(margin)+","+String.valueOf(SUMofquantity);
			 ProfitMap.put(margin, new Text(answer));	
			 
		 }
		 context.write(key, new Text(answer));
					 
		}	 
 }
 public static void main( String args[]) throws IOException, ClassNotFoundException, InterruptedException
 {
	 Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "Profit and Quantity");
	    job.setJarByClass(ProfitQuantity.class);
	    job.setMapperClass(ProfitQuantityMapper.class);
	    job.setReducerClass(ProfitQuantityReducer.class); //job.setNumReduceTasks(0);
	    
	    job.setMapOutputKeyClass(LongWritable.class);
	    job.setMapOutputValueClass(Text.class);
	    
	    job.setOutputKeyClass(LongWritable.class);
	    job.setOutputValueClass(Text.class);
	    
	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	    System.exit(job.waitForCompletion(true) ? 0 : 1);
 }
 
}
