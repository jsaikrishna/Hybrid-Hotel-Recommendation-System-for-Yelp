import com.mongodb.spark.MongoSpark;
import com.mongodb.spark.config.ReadConfig;
import com.mongodb.spark.rdd.api.java.JavaMongoRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.ml.evaluation.RegressionEvaluator;
import org.apache.spark.ml.recommendation.ALS;
import org.apache.spark.ml.recommendation.ALSModel;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.*;
import org.bson.Document;
import scala.Tuple2;

import javax.xml.crypto.Data;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CollaborativeFiltering {
    public static List<Row> u=new ArrayList<>();
    public static void main(String[] args) {
		//Basic template taken from official website of spark mongo connector
        SparkSession sparkSession=SparkSession.builder()
                .master("local[*]")
                .appName("MongoSparkConnector")
                .config("spark.mongodb.input.uri", "mongodb://ec2-54-86-114-144.compute-1.amazonaws.com/yelp_filter.business")
                .config("spark.mongodb.output.uri", "mongodb://ec2-54-86-114-144.compute-1.amazonaws.com/yelp_rec.collaborative")
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

        SQLContext sqlContext=new SQLContext(sparkSession);

        //For creation of integer equivalent of userID
        Dataset<Row> userid=sparkSession.sql("select user_id from user");

        StructType schema=DataTypes.createStructType(new StructField[]{
                DataTypes.createStructField("user_id",DataTypes.StringType,false,Metadata.empty()),
                DataTypes.createStructField("userId",DataTypes.LongType,false,Metadata.empty())
        });
        //Logic to map user_id which is of String type to some unique integer
        JavaPairRDD<Row,Long> userPairRDD=userid.toJavaRDD().zipWithIndex();
        JavaRDD<Row> userRowRDD=userPairRDD.map(tuple -> RowFactory.create(tuple._1().getAs("user_id").toString(),tuple._2()));
        Dataset<Row> user_with_id=sqlContext.createDataFrame(userRowRDD,schema);
        user_with_id.createOrReplaceTempView("user_with_id");
        user_with_id.printSchema();
        user_with_id.show();


        Dataset<Row> user_updated=sparkSession.sql("select u.*,int(ui.userId) as userId from user u,user_with_id ui where u.user_id=ui.user_id");
        user_updated.createOrReplaceTempView("user_updated");
        user_updated.printSchema();
        user_updated.show();

        Dataset<Row> businessid=sparkSession.sql("select business_id from business");

        StructType businessSchema=DataTypes.createStructType(new StructField[]{
                DataTypes.createStructField("business_id",DataTypes.StringType,false,Metadata.empty()),
                DataTypes.createStructField("businessId",DataTypes.LongType,false,Metadata.empty())
        });

        JavaPairRDD<Row,Long> businessPairRDD=businessid.toJavaRDD().zipWithIndex();
        JavaRDD<Row> businessRowRDD=businessPairRDD.map(tuple -> RowFactory.create(tuple._1().getAs("business_id").toString(),tuple._2()));
        Dataset<Row> business_with_id=sqlContext.createDataFrame(businessRowRDD,businessSchema);
        business_with_id.createOrReplaceTempView("business_with_id");
        business_with_id.printSchema();
        business_with_id.show();

        Dataset<Row> business_updated=sparkSession.sql("select b.*,int(bi.businessId) as businessId from business b,business_with_id bi where b.business_id=bi.business_id");
        business_updated.createOrReplaceTempView("business_updated");
        business_updated.printSchema();
        business_updated.count();
        business_updated.show();

        Dataset<Row> review_projection=sparkSession.sql("select int(ui.userId) as userId,int(bi.businessId) as businessId,float(r.stars) as rating from review r,user_with_id ui,business_with_id bi where ui.user_id=r.user_id and bi.business_id=r.business_id");
        review_projection.createOrReplaceTempView("review_projection");
        review_projection.printSchema();
        review_projection.show();
		//The logic for ALS model is inspired from the official website https://spark.apache.org/docs/latest/mllib-collaborative-filtering.html
        Dataset<Row>[] splits=review_projection.randomSplit(new double[]{0.8,0.2});
        Dataset<Row> training=splits[0];
        Dataset<Row> test=splits[1];

        ALS als=new ALS()
                .setMaxIter(5)
                .setRegParam(0.01)
                .setUserCol("userId")
                .setItemCol("businessId")
                .setRatingCol("rating");
        ALSModel model=als.fit(training);
        model.setColdStartStrategy("drop");

        //Measuring the accuracy
        Dataset<Row> predictions=model.transform(test);
        RegressionEvaluator regressionEvaluator=new RegressionEvaluator()
                .setMetricName("rmse")
                .setLabelCol("rating")
                .setPredictionCol("prediction");
        Double rmse=regressionEvaluator.evaluate(predictions);
        System.out.println("Root mean square error: "+rmse);
        System.out.println("*********************************************************************");

        //Saving the model
        try {
            model.write().overwrite().save("als");
        } catch (IOException e) {
            e.printStackTrace();
        }
        Dataset<Row> prediction=model.transform(test);
        Dataset<Row> recommendations=model.recommendForAllUsers(10);
        recommendations.createOrReplaceTempView("recommendation");
        recommendations.printSchema();
        recommendations.show();

        Dataset<Row> user_recommendations=sparkSession.sql("select r.*,ui.user_id from recommendation r,user_with_id ui where r.userId=ui.userId");
        user_recommendations.createOrReplaceTempView("user_recommendation");
        user_recommendations.printSchema();
        user_recommendations.show();

        Dataset<Row> user_rec_explode=sparkSession.sql("select userId,user_id,explode(recommendations) as recommendations from user_recommendation");
        user_rec_explode.createOrReplaceTempView("user_rec_explode");
        user_rec_explode.printSchema();
        user_rec_explode.show();

//        Dataset<Row> temp=user_recommendations.select(user_recommendations.col("user_id"),user_recommendations.col("recommendations").getItem(0));
//        temp.printSchema();
//        temp.show();

        Dataset<Row> user_temp=user_rec_explode.select(user_rec_explode.col("user_id"),user_rec_explode.col("recommendations.businessId"));
        user_temp.createOrReplaceTempView("user_temp");
        user_temp.printSchema();
        user_temp.show();

//        Dataset<Row> user_rec_explode_explode=sparkSession.sql("select userId,user_id,explode(recommendations) as recommendations from user_rec_explode");
//        user_rec_explode_explode.createOrReplaceTempView("user_rec_explode_explode");
//        user_rec_explode_explode.printSchema();
//        user_rec_explode_explode.show();

        Dataset<Row> business_rec=sparkSession.sql("select ut.*,bi.business_id from user_temp ut,business_with_id bi where ut.businessId=bi.businessId");
        business_rec.createOrReplaceTempView("business_rec");
        business_rec.printSchema();
        business_rec.show();

        Dataset<Row> final_rec=sparkSession.sql("select user_id,collect_set(business_id) as businessid from business_rec group by user_id");
        final_rec.createOrReplaceTempView("final_rec");
        final_rec.printSchema();
        final_rec.show();

        //Dataset<Row> final=
        //Dataset<Row> final=

        final_rec.write().format("com.mongodb.spark.sql.DefaultSource").mode("append").option("database","yelp_rec").option("collection", "collaborative").save();



        //Dataset<Row> business_recommendation=sparkSession.sql("select ");
        //String user_id="-3bsS2i9xqjNnIA1fRnzIQ";



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
    private static void call_userid(Row row) throws IOException {
        u.add(row.getAs("user_id"));
    }
}
