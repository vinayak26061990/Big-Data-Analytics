import java.io.*;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.util.*;
public class FoodiesNYC_Zip_Rating_Reducer extends Reducer<Text,Text,Text,Text> {
	@Override
	public void reduce(Text key,Iterable<Text> values,Context context) throws IOException,InterruptedException {
		double rating_greater3 = 0;
	
                double rating_percentage=0;
                
		double total_rating = 0;
		
		for (Text value:values) {
				String data=  value.toString();
				    total_rating=total_rating + 1;
                                        if (Float.valueOf(data.trim()).floatValue() >= 3.0)
                                            rating_greater3=rating_greater3 + 1;
					
				

		
		}
	          if(total_rating> 0)
		{
			rating_percentage= (rating_greater3 / total_rating)*100;
			context.write(new Text(key),new Text(Double.toString(rating_percentage) + " "+ total_rating));
		}
		
	}
}


