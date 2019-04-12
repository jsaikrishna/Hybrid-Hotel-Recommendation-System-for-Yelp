import com.mongodb.spark.MongoSpark;
import com.mongodb.spark.config.ReadConfig;
import com.mongodb.spark.rdd.api.java.JavaMongoRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.ForeachFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.bson.Document;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class DataFiltering {
    public static void main(String[] args) {
		//Basic template taken from official website of spark mongo connector
        SparkSession sparkSession=SparkSession.builder()
                .master("local[*]")
                .appName("MongoSparkConnector")
                .config("spark.mongodb.input.uri", "mongodb://ec2-100-26-190-159.compute-1.amazonaws.com/yelp.business")
                .config("spark.mongodb.output.uri", "mongodb://ec2-100-26-190-159.compute-1.amazonaws.com/yelp_filter.business")
                .getOrCreate();
        JavaSparkContext jsc=new JavaSparkContext(sparkSession.sparkContext());
        //Creating a ReadConfig in order to read another collection from mongo db
        Map<String,String> readOverride=new HashMap<String, String>();
        readOverride.put("collection","review");
        readOverride.put("readPreference.name","secondaryPreferred");
        ReadConfig readConfig=ReadConfig.create(jsc).withOptions(readOverride);
        //To read user colelction from mongo db
        Map<String,String> userOverride=new HashMap<String, String>();
        userOverride.put("collection","user");
        userOverride.put("readPreference.name","secondaryPreferred");
        ReadConfig userConfig=ReadConfig.create(jsc).withOptions(userOverride);
        //Storing collections in mongodb into RDD and DF
        JavaMongoRDD<Document> reviewRDD=MongoSpark.load(jsc,readConfig);
        JavaMongoRDD<Document> userRDD=MongoSpark.load(jsc,userConfig);
        JavaMongoRDD<Document> businessRDD=MongoSpark.load(jsc);
        Dataset<Row> review=reviewRDD.toDF();
        Dataset<Row> business=businessRDD.toDF();
        Dataset<Row> user=userRDD.toDF();
        business.printSchema();
        review.printSchema();
        user.printSchema();
        //Create alias names for DF created so that they can be used while using SQL queries
        review.createOrReplaceTempView("review");
        business.createOrReplaceTempView("business");
        user.createOrReplaceTempView("user");
        //Logic for preprocessing the data to get the data in desired format
        Dataset<Row> business_filter_city=sparkSession.sql("select * from business where city='Las Vegas'");
        business_filter_city.createOrReplaceTempView("business_filter_city");
        Dataset<Row> business_categories=sparkSession.sql("select business_id,name,neighborhood,address,city,state,postal_code,latitude,longitude,stars,review_count,is_open,attributes,split(categories,'[,]') as categories,hours from business_filter_city");
        business_categories.createOrReplaceTempView("business_categories");
        business.show();
        business_categories.show();
        Dataset<Row> business_explode=sparkSession.sql("select business_id,name,neighborhood,address,city,state,postal_code,latitude,longitude,stars,review_count,is_open,attributes,explode(categories) as categories,hours from business_categories");
        business_explode.show();
        business_explode.createOrReplaceTempView("business_explode");
        Dataset<Row> business_filter=sparkSession.sql("select * from business_explode where lower(categories)='restaurants'");
        business_filter.show();
        business_filter.createOrReplaceTempView("business_filter");
        user.printSchema();

        Dataset<Row> review_filter=sparkSession.sql("select * from review where business_id in (select business_id from business_filter)");
        review_filter.createOrReplaceTempView("review_filter");
        Dataset<Row> user_filter=sparkSession.sql("select * from user where user_id in (select user_id from review_filter)");
        user_filter.createOrReplaceTempView("user_filter");
		
		//further preprocessing
		review_filter=sparkSession.sql("select * from review_filter where stars not like '%-%-%'");
		review_filter=sparkSession.sql("select *,(select avg(ratings) from review_filter where stars is not null) as avg_rating from review_filter");

        //Saving dataframes in mongodb
        business_filter.write().format("com.mongodb.spark.sql.DefaultSource").mode("append").option("database","yelp_filter").option("collection", "business").save();
        review_filter.write().format("com.mongodb.spark.sql.DefaultSource").mode("append").option("database","yelp_filter").option("collection", "review").save();
        user_filter.write().format("com.mongodb.spark.sql.DefaultSource").mode("append").option("database","yelp_filter").option("collection", "user").save();
		
        //To store the preprocessed data in text file, lambda function is used
        //joined.foreach((ForeachFunction<Row>) DataFiltering::call);

        //For Context Based Filtering - Las Vegas Data
        review.createOrReplaceTempView("review_context");
        Dataset<Row> review_context=sparkSession.sql("select * from review_context where city='Las Vegas'");
        review_context.createOrReplaceTempView("review_context");
        Dataset<Row> review_context_filter=sparkSession.sql("select * from review_context where business_id in (select business_id from business_filter)");
        review_context_filter.createOrReplaceTempView("business_categories");
        review_context_filter.show();

        review_context_filter.write().format("com.mongodb.spark.sql.DefaultSource").mode("append").option("database","yelp_filter").option("collection", "review_context").save();


    }
    //call method writes all the preprocessed data into topical.txt file//This method is not called as of now
    private static void call(Row row) throws IOException {
        BufferedWriter writer = new BufferedWriter(new FileWriter("topical.txt",true));
        writer.append(row.getAs("business_id")+"\t"+row.getAs("business_stars")+ "\t"+row.getAs("business_review_count")+ "\t"+row.getAs("review_id")+ "\t"+row.getAs("review_stars")+ "\t"+row.getAs("text")+ "\t"+row.getAs("user_id")+ "\t"+row.getAs("name")+ "\t"+row.getAs("user_review_count")+ "\t"+row.getAs("yelping_since")+ "\t"+row.getAs("friends")+ "\t"+row.getAs("useful")+ "\t"+row.getAs("funny")+ "\t"+row.getAs("cool")+ "\t"+row.getAs("fans")+ "\t"+row.getAs("elite")+"\n");
        writer.close();
    }
}
