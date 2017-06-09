import java.io.*;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.util.*;
public class FoodiesNYC_ZipPopularCuisineReducer extends Reducer<Text,Text,Text,Text> {
	@Override
	public void reduce(Text key,Iterable<Text> values,Context context) throws IOException,InterruptedException {
		double total_rating = 0;
		double total_votes = 0;
		double total_rating_votes = 0;
		double total_weighted_rating = 0;

		for (Text value:values) {
				String[] data=  value.toString().split("#");
                                if(data.length > 1)
                                {
				if(data[0] != null && !data[0].isEmpty() && data[1] != null && !data[1].isEmpty())
				{
					float rating = Float.valueOf(data[0].trim()).floatValue();
					float votes = Float.valueOf(data[1].trim()).floatValue();

					total_rating = total_rating + rating;
					total_votes = total_votes + votes;
					total_rating_votes += rating * votes;
				}
                                }
				
		
		}
	        if(total_rating_votes > 0 && total_votes > 0)
		{
			total_weighted_rating = total_rating_votes / total_votes;
                        
			String[] key_data = key.toString().split("-");
                        if (key_data.length > 1)
                         {
			context.write(new Text(key_data[0] + "\t" + key_data[1]),new Text(Double.toString(total_weighted_rating) +"\t" +total_votes));}
		}
		
		
	}
}


