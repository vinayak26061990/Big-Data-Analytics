import java.io.*;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.lang.*;


public class FoodiesNYC_Mapper extends Mapper<LongWritable,Text,Text,Text>
{
	@Override
	public void map(LongWritable key,Text value,Context context) throws IOException,InterruptedException {
		String line=value.toString();
		String[] splitted =line.split(",");
		String zipCode = splitted[5];
		String rating = splitted[4];
		String totalNumberOfVotes = splitted[3];
                String Latitude=splitted[6];
                String Longitude=splitted[7];
		if(zipCode != null && !zipCode.isEmpty() && rating != null && !rating.isEmpty() && rating != "0" && totalNumberOfVotes != null && !totalNumberOfVotes.isEmpty() && totalNumberOfVotes != "0" && totalNumberOfVotes.matches("\\d+") && !Latitude.isEmpty() && !Longitude.isEmpty() && Latitude!= "0" && Longitude!="0" && Latitude!=null && Longitude!=null)
		{
			context.write(new Text(zipCode),new Text(rating + "#" + totalNumberOfVotes + "#" + Latitude + "#" + Longitude ));
		}
	}
}

