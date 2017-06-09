#ETL & COMBINE LOGIC

hive -e "drop table yelp_data"
#yelp_data table is used to store the yelp file using load utility
hive -e "create external table yelp_data(source_type string,id string,restaurant_name string,review_count smallint,rating float,zip smallint,latitude double,longitude double,address string,cuisine string) row format delimited fields terminated by ',' stored as textfile location '/user/cloudera/NYCFoodie/input/Yelp'"

##hive -e "load data INPATH '/user/cloudera/big_data_project/Input/nyc-yelp_data.csv' overwrite into table yelp_data"



#filtered_yelp_data table is used to store the yelp data after cleaning and filtering

hive -e "drop table filtered_yelp_data"
hive -e "create  table filtered_yelp_data(source_type string,id string,restaurant_name string,review_count smallint,rating float,zip smallint,latitude double,longitude double,address string,cuisine string) row format delimited fields terminated by ',' stored as textfile"
hive -e "insert into table filtered_yelp_data select distinct A.* from yelp_data A where zip is not  null and rating >0"
hive -e "select * from  filtered_yelp_data"|sed 's/[\t]/,/g' >/home/cloudera/all/Hive/ak5641_rm4149_vr840/Mapper-Reducer/HIVE_OUTPUT/yelp_data.csv
sed '$d'  /home/cloudera/all/Hive/ak5641_rm4149_vr840/Mapper-Reducer/HIVE_OUTPUT/yelp_data.csv|sed '$d' >/home/cloudera/all/Hive/ak5641_rm4149_vr840/Mapper-Reducer/HIVE_OUTPUT/filtered_restaurant_data.csv
rm /home/cloudera/all/Hive/ak5641_rm4149_vr840/Mapper-Reducer/HIVE_OUTPUT/yelp_data.csv
#zomato_data table is used to store the yelp file using load utility

hive -e "drop table zomato_data"
hive -e "create external table zomato_data(source_type string,id string,restaurant_name string,review_count smallint,rating float,zip smallint,latitude double,longitude double,address string,cuisine string) row format delimited fields terminated by ',' stored as textfile location '/user/cloudera/NYCFoodie/input/Zomato'"



##hive -e "load data INPATH '/user/cloudera/big_data_project/Input/nyc_zomato_data.csv' overwrite into table zomato_data"



#filtered_zomato_data table is used to store the zomato data after cleaning and filtering
hive -e "drop table filtered_zomato_data"
hive -e "create table filtered_zomato_data(source_type string,id string,restaurant_name string,review_count smallint,rating float,zip smallint,latitude double,longitude double,address string,cuisine string) row format delimited fields terminated by ',' stored as textfile"
hive -e "insert into table filtered_zomato_data select distinct A.* from zomato_data A where zip is not  null and rating >0"
hive -e "select * from  filtered_zomato_data "|sed 's/[\t]/,/g' >/home/cloudera/all/Hive/ak5641_rm4149_vr840/Mapper-Reducer/HIVE_OUTPUT/zomato.csv
sed '$d'  /home/cloudera/all/Hive/ak5641_rm4149_vr840/Mapper-Reducer/HIVE_OUTPUT/zomato.csv|sed '$d' >>/home/cloudera/all/Hive/ak5641_rm4149_vr840/Mapper-Reducer/HIVE_OUTPUT/filtered_restaurant_data.csv
rm -f /home/cloudera/all/Hive/ak5641_rm4149_vr840/Mapper-Reducer/HIVE_OUTPUT/zomato.csv

#google_places_data table is used to store the google_places file using load utility

hive -e "drop table google_places_data"
hive -e "create external table google_places_data(source_type string,id string,restaurant_name string,review_count smallint,rating float,zip smallint,latitude double,longitude double,address string,cuisine string) row format delimited fields terminated by ',' stored as textfile location '/user/cloudera/NYCFoodie/input/Google_Places'"





#filtered_google_places_data table is used to store the filtered_google_places data after cleaning and filtering
hive -e "drop table filtered_google_places_data"
hive -e "create table filtered_google_places_data(source_type string,id string,restaurant_name string,review_count smallint,rating float,zip smallint,latitude double,longitude double,address string,cuisine string) row format delimited fields terminated by ',' stored as textfile"
hive -e "insert into table filtered_google_places_data select distinct A.* from google_places_data A where zip is not  null and rating >0"
hive -e " select * from filtered_google_places_data "|sed 's/[\t]/,/g'>/home/cloudera/all/Hive/ak5641_rm4149_vr840/Mapper-Reducer/HIVE_OUTPUT/google_places_data.csv
sed '$d'  /home/cloudera/all/Hive/ak5641_rm4149_vr840/Mapper-Reducer/HIVE_OUTPUT/google_places_data.csv|sed '$d' >>/home/cloudera/all/Hive/ak5641_rm4149_vr840/Mapper-Reducer/HIVE_OUTPUT/filtered_restaurant_data.csv
rm -f /home/cloudera/all/Hive/ak5641_rm4149_vr840/Mapper-Reducer/HIVE_OUTPUT/google_places_data.csv






#This  following is done after running the mappers and reducers for the  on the combined Yelp,Zomato and Google Places Data for various analytics



#This creates the zipwise rating output
hive -e "drop  table zip_rating"
hive -e "create external table zip_rating(zip smallint,rating string,latitude string,Longitude string) row format delimited fields terminated by '\t' stored as textfile location '/user/cloudera/NYCFoodie/PopularZip'"

#This lists the "populary zip" rating wise
hive -e "select * from zip_rating order by rating desc" >/home/cloudera/all/Hive/ak5641_rm4149_vr840/Mapper-Reducer/HIVE_OUTPUT/popular_zip_rating_wise_temp.csv
sed '$d'  /home/cloudera/all/Hive/ak5641_rm4149_vr840/Mapper-Reducer/HIVE_OUTPUT/popular_zip_rating_wise_temp.csv|sed '$d' >/home/cloudera/all/Hive/ak5641_rm4149_vr840/Mapper-Reducer/HIVE_OUTPUT/popular_zip_rating_wise.csv
rm -f /home/cloudera/all/Hive/ak5641_rm4149_vr840/Mapper-Reducer/HIVE_OUTPUT/popular_zip_rating_wise_temp.csv

#This lists the "popular cuisine" rating wise
hive -e "drop  table popular_cuisine"
hive -e "create external table popular_cuisine(cuisine string,rating double,review_count double) row format delimited fields terminated by '\t' stored as textfile location '/user/cloudera/NYCFoodie/PopularCuisine/'"
hive -e "select * from popular_cuisine order by rating desc" >/home/cloudera/all/Hive/ak5641_rm4149_vr840/Mapper-Reducer/HIVE_OUTPUT/popular_cuisine_temp.csv
sed '$d'  /home/cloudera/all/Hive/ak5641_rm4149_vr840/Mapper-Reducer/HIVE_OUTPUT/popular_cuisine_temp.csv|sed '$d' >/home/cloudera/all/Hive/ak5641_rm4149_vr840/Mapper-Reducer/HIVE_OUTPUT/popular_cuisine.csv
rm -f /home/cloudera/all/Hive/ak5641_rm4149_vr840/Mapper-Reducer/HIVE_OUTPUT/popular_cuisine_temp.csv
#This lists the "populary  cuisine" zip and rating wise
hive -e "drop  table zip_popular_cuisine"
hive -e "create external table zip_popular_cuisine(zip smallint,cuisine string,rating double,review_count double) row format delimited fields terminated by '\t' stored as textfile location '/user/cloudera/NYCFoodie/ZipPopularCuisine/'"
hive -e "select * from zip_popular_cuisine order by zip asc ,rating desc">/home/cloudera/all/Hive/ak5641_rm4149_vr840/Mapper-Reducer/HIVE_OUTPUT/cuisine_zip_rating_temp.csv
sed '$d'  /home/cloudera/all/Hive/ak5641_rm4149_vr840/Mapper-Reducer/HIVE_OUTPUT/cuisine_zip_rating_temp.csv|sed '$d' >/home/cloudera/all/Hive/ak5641_rm4149_vr840/Mapper-Reducer/HIVE_OUTPUT/cuisine_zip_rating.csv
rm -f /home/cloudera/all/Hive/ak5641_rm4149_vr840/Mapper-Reducer/HIVE_OUTPUT/cuisine_zip_rating_temp.csv



