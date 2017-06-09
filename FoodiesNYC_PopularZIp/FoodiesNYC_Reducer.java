import java.io.*;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.util.*;
public class FoodiesNYC_Reducer extends Reducer<Text,Text,Text,Text> {
	@Override
	public void reduce(Text key,Iterable<Text> values,Context context) throws IOException,InterruptedException {
		double total_rating = 0;
		double total_votes = 0;
		double total_rating_votes = 0;
		double total_weighted_rating = 0;
                float latitude=0;
                float longitude=0;
		for (Text value:values) {
				String[] data=  value.toString().split("#");
                                 if(data.length > 1)
                                {
				if(data[0] != null && !data[0].isEmpty() && data[1] != null && !data[1].isEmpty() && data[2] != null && !data[2].isEmpty() && data[3] != null && !data[3].isEmpty())
				{
					float rating = Float.valueOf(data[0].trim()).floatValue();
					float votes = Float.valueOf(data[1].trim()).floatValue();
                                        latitude = Float.valueOf(data[2].trim()).floatValue();
                                        longitude=Float.valueOf(data[3].trim()).floatValue(); 
					total_rating = total_rating + rating;
					total_votes = total_votes + votes;
					total_rating_votes += rating * votes;
				}

		
		}
                }
	          if(total_rating_votes > 0 && total_votes > 0)
		{
			total_weighted_rating = total_rating_votes / total_votes;
			context.write(new Text(key),new Text(Double.toString(total_weighted_rating) + "\t" + Double.toString(latitude) + "\t" + Double.toString(longitude)));
		}
		
	}
}


