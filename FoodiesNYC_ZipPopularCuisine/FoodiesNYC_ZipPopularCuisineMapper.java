import java.io.*;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.lang.*;


public class FoodiesNYC_ZipPopularCuisineMapper extends Mapper<LongWritable,Text,Text,Text>
{
	@Override
	public void map(LongWritable key,Text value,Context context) throws IOException,InterruptedException {
		String line=value.toString();
		String[] splitted =line.split(",");
		String filteredCuisinesString = "Personal Chefs|Social Clubs|Cheese Shops|bakery|arabian|Food Stands|Meat Shops|austrian|hungarian|Pasta Shops|Beer|falafel|mideastern|Middle Eastern|uzbek|wineries|halal|Seafood Markets|Food Trucks|health|armenian|cafe|Sri Lankan|venezuelan|bar|Farmers Market|slovakian|senegalese|laotian|Iberian|Laotian|Vegan|Himalayan/Nepalese|himalayan|Gay Bars|moroccan|cambodian|polish|Bubble Tea|Cajun/Creole|cajun|candy|Candy Stores|australian|Ice Cream & Frozen Yogurt|salvadoran|breweries|ukrainian|empanadas|persian|Persian/Iranian|vegetarian|portuguese|lebanese|belgian|Beverages|creperies|Modern European|Do-It-Yourself Food|bakeries|argentine|tapas|Tapas Bars|afghani|Afghan|Coffee and Tea|ethiopian|Cocktail Bars|Popcorn Shops|french|Chocolatiers & Shops|chocolate|desserts|cheesesteaks|Coffee & Tea|coffee|Fruits & Veggies|gelato|turkish|peruvian|Wine Bars|hawaiian|mediterranean|fondue|ramen|Specialty Food|gourmet|scottish|greek|spanish|dominican|filipino|cafes|Gluten-Free|basque|Juice Bars & Smoothies|shanghainese|cuban|russian|african|Sandwich|cabaret|Puerto Rican|british|donuts|italian|Champagne Bars|Jazz & Blues|kosher|german|Tea Rooms|tea|scandinavian|latin|Latin American|mongolian|pretzels|seafood|Hot Dogs|indpak|Indian|steak|Steakhouses|sandwiches|japanese|American|gastropubs|korean|egyptian|macarons|Dive Bars|divebars|haitian|malaysian|sushi|Sushi Bars|pizza|bagels|Breakfast & Brunch|soup|Soul Food|delis|Fish & Chips|fishnchips|Beer Bar|vietnamese|southern|bbq|Barbeque|brazilian|pubs|Hot Pot|czech|taiwanese|Organic Stores|caribbean|South African|trinidadian|pakistani|thai|Asian Fusion|salad|indonesian|szechuan|mexican|irish|bangladeshi|burgers|chinese|dimsum|Dim Sum|colombian|American (Traditional)|Irish Pub|nutritionists|Boat Charters|cupcakes|burmese|surfshop|teppanyaki|casino|Beer Gardens|Asian|cantonese|Chicken Shop|karaoke|singaporean|Sports Bars|cafeteria|hookah_bars|Sushi|nightlife|tex-mex|Fast Food|hotdogs|Chicken Wings|Eastern European|Steakhouse|Bar Food|South American|Healthy Food";
		String[] filteredCuisines = filteredCuisinesString.toLowerCase().split("\\|");
		if(splitted.length > 9)
		{ 
			String zip = splitted[5];
			String cuisines = splitted[9];
			String rating = splitted[4];
			String totalNumberOfVotes = splitted[3];

			if(zip != null && !zip.isEmpty() && cuisines != null && !cuisines.isEmpty() && rating != null && !rating.isEmpty() && rating != "0" && totalNumberOfVotes != null && !totalNumberOfVotes.isEmpty() && totalNumberOfVotes != "0" && totalNumberOfVotes.matches("\\d+"))
			{
				String[] cuisine = cuisines.toLowerCase().split("\\|");
				if(cuisine.length > 0)
				{
					for (int i=0;i<cuisine.length;i++)
      					{ 
						String cuisineValue = cuisine[i].trim();
						if(!cuisineValue.contains("_") && Arrays.asList(filteredCuisines).contains(cuisineValue)){
							context.write(new Text(zip + "-" + cuisineValue),new Text(rating + "#" + totalNumberOfVotes));
						}
					}
				}
				
			
			}
		
		}
	}
}

