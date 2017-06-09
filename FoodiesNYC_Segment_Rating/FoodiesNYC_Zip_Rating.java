import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class FoodiesNYC_Zip_Rating
{
	 public static void main(String args[]) throws Exception {
		if (args.length !=2)
		{
			System.err.println("Usage:String Counter <input path> <output path>");
			System.exit(-1);
		}
		
		Job job=new Job();
		job.setJarByClass(FoodiesNYC_Zip_Rating.class);
		job.setJobName("FoodiesNYC_Zip_Rating");
		
		FileInputFormat.addInputPath(job,new Path(args[0]));
		FileOutputFormat.setOutputPath(job,new Path(args[1]));
		
		job.setMapperClass(FoodiesNYC_Zip_Rating_Mapper.class);
		job.setReducerClass(FoodiesNYC_Zip_Rating_Reducer.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		System.exit(job.waitForCompletion(true) ? 0:1);
	}
}
