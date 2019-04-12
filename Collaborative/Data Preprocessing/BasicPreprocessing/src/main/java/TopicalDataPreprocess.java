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

public class TopicalDataPreprocess {
    public static void main(String[] args) {
		//Basic template taken from official website of spark mongo connector
        SparkSession sparkSession=SparkSession.builder()
                .master("local[*]")
                .appName("MongoSparkConnector")
                .config("spark.mongodb.input.uri", "mongodb://ec2-52-91-24-183.compute-1.amazonaws.com/yelp.business")
                //.config("spark.mongodb.output.uri", "mongodb://student:student@54.173.174.196/test.myCollection")
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
        Dataset<Row> business_categories=sparkSession.sql("select business_id,stars,review_count,split(categories,'[,]') as categories from business");
        business_categories.createOrReplaceTempView("business_categories");
        business.show();
        business_categories.show();
        Dataset<Row> business_explode=sparkSession.sql("select business_id,stars,review_count,explode(categories) as categories from business_categories");
        business_explode.show();
        business_explode.createOrReplaceTempView("business_explode");
        Dataset<Row> business_filter=sparkSession.sql("select * from business_explode where lower(categories)='restaurants'");
        business_filter.show();
        business_filter.createOrReplaceTempView("business_filter");
        user.printSchema();
        Dataset<Row> joined=sparkSession.sql("select b.business_id,b.stars as business_stars,b.review_count as business_review_count,r.review_id,r.stars as review_stars,r.text,r.user_id,u.name,u.review_count as user_review_count,u.yelping_since,u.friends,u.useful,u.funny,u.cool,u.fans,u.elite from business_filter b,review r,user u where b.business_id=r.business_id and r.user_id=u.user_id");
        joined.show();
        joined.printSchema();
        //To store the preprocessed data in text file, lambda function is used
        joined.foreach((ForeachFunction<Row>) TopicalDataPreprocess::call);
        joined.printSchema();

    }
    //call method writes all the preprocessed data into topical.txt file
    private static void call(Row row) throws IOException {
        BufferedWriter writer = new BufferedWriter(new FileWriter("topical.txt",true));
        writer.append(row.getAs("business_id")+"\t"+row.getAs("business_stars")+ "\t"+row.getAs("business_review_count")+ "\t"+row.getAs("review_id")+ "\t"+row.getAs("review_stars")+ "\t"+row.getAs("text")+ "\t"+row.getAs("user_id")+ "\t"+row.getAs("name")+ "\t"+row.getAs("user_review_count")+ "\t"+row.getAs("yelping_since")+ "\t"+row.getAs("friends")+ "\t"+row.getAs("useful")+ "\t"+row.getAs("funny")+ "\t"+row.getAs("cool")+ "\t"+row.getAs("fans")+ "\t"+row.getAs("elite")+"\n");
        writer.close();
    }
}
