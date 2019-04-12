import com.mongodb.spark.MongoSpark;
import com.mongodb.spark.config.ReadConfig;
import com.mongodb.spark.rdd.api.java.JavaMongoRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.ml.recommendation.ALS;
import org.apache.spark.ml.recommendation.ALSModel;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.bson.Document;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class RecommendationsBasedOnModel {
    public static void main(String[] args) {
		//Basic template taken from official website of spark mongo connector
        SparkSession sparkSession=SparkSession.builder()
                .master("local[*]")
                .appName("MongoSparkConnector")
                .config("spark.mongodb.input.uri", "mongodb://ec2-54-86-114-144.compute-1.amazonaws.com/yelp_filter.business")
                .config("spark.mongodb.output.uri", "mongodb://ec2-54-86-114-144.compute-1.amazonaws.com/yelp_filter.collaborative")
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

        ALSModel model=ALSModel.load("ALSModel");
        Dataset<Row> recommendation=model.recommendForAllUsers(10);
        recommendation.printSchema();
        recommendation.show();



        //recommendation.write().format("com.mongodb.spark.sql.DefaultSource").mode("append").option("database","yelp_filter").option("collection", "collaborative").save();

        //To store the preprocessed data in text file, lambda function is used
        //review_filter.foreach((ForeachFunction<Row>) DataFiltering::call);

    }
    //call method writes all the preprocessed data into topical.txt file//This method is not called as of now
    private static void call(Row row) throws IOException {
        BufferedWriter writer = new BufferedWriter(new FileWriter("topical.txt",true));
        //if(!row.getAs("review_stars").toString().contains("-"))
        writer.append(row.getAs("business_id")+"\t"+row.getAs("business_stars")+ "\t"+row.getAs("business_review_count")+ "\t"+row.getAs("review_id")+ "\t"+row.getAs("review_stars")+ "\t"+row.getAs("text")+ "\t"+row.getAs("user_id")+ "\t"+row.getAs("name")+ "\t"+row.getAs("user_review_count")+ "\t"+row.getAs("yelping_since")+ "\t"+row.getAs("friends")+ "\t"+row.getAs("useful")+ "\t"+row.getAs("funny")+ "\t"+row.getAs("cool")+ "\t"+row.getAs("fans")+ "\t"+row.getAs("elite")+"\n");
        writer.close();
    }
}
