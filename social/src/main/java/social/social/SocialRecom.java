package social.social;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.ForeachFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.*;
import org.apache.spark.sql.functions;
import static org.apache.spark.sql.functions.lit;
import org.apache.spark.sql.types.*;
import org.bson.Document;

import com.mongodb.spark.MongoSpark;
import com.mongodb.spark.config.ReadConfig;
import com.mongodb.spark.rdd.api.java.JavaMongoRDD;

import scala.Tuple2;

public class SocialRecom implements Serializable {
public static HashMap<String,String> original = new HashMap<String,String>();
	@SuppressWarnings({ "unchecked", "rawtypes", "serial", "deprecation" })
	public static void main(String[] args) throws IOException {
		// Basic template from official website
		SparkSession sparkSession=SparkSession.builder()
                .master("local[*]")
                .appName("MongoSparkConnector")
                .config("spark.mongodb.input.uri", "mongodb://ec2-54-86-114-144.compute-1.amazonaws.com/yelp.business")
                .config("spark.mongodb.output.uri", "mongodb://ec2-54-86-114-144.compute-1.amazonaws.com/yelp_rec.social")
                .getOrCreate(); 
		JavaSparkContext jsc=new JavaSparkContext(sparkSession.sparkContext());
		SQLContext sqlContext = new SQLContext(jsc);
		Map<String,String> readOverride=new HashMap<String, String>();
        readOverride.put("collection","review");
        readOverride.put("readPreference.name","secondaryPreferred");
        ReadConfig readConfig=ReadConfig.create(jsc).withOptions(readOverride);
        //To read user collection from mongo db
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
        business_categories.printSchema();
        Dataset<Row> business_explode=sparkSession.sql("select business_id,name,neighborhood,address,city,state,postal_code,latitude,longitude,stars,review_count,is_open,attributes,explode(categories) as categories,hours from business_categories");
        business_explode.show();
        business_explode.createOrReplaceTempView("business_explode");
        
        Dataset<Row> business_filter=sparkSession.sql("select * from business_explode where lower(categories)='restaurants'");
        business_filter.show();    
        business_filter.createOrReplaceTempView("business_filter");
        //user.printSchema();
        business_filter.printSchema();
        

        Dataset<Row> review_filter=sparkSession.sql("select * from review where business_id in (select business_id from business_filter)");
        review_filter.createOrReplaceTempView("review_filter");
        review_filter.printSchema();
        //review_filter.show();
        Dataset<Row> user_filter=sparkSession.sql("select * from user where user_id in (select user_id from review_filter)");
        user_filter.createOrReplaceTempView("user_filter");
        user_filter.printSchema();
        //user_filter.show();
        //System.out.println("DONE1");
        
        
        // for user_id and friends list
        Dataset<Row> user_id_friends=sparkSession.sql("select user_id, split(friends,'[,]') as friends from user_filter ");
        user_id_friends.createOrReplaceTempView("user_id_friends");
        user_id_friends.printSchema();
        user_id_friends.show();
        //System.out.println("DONE2");
        
        // For user_id and friends as explode-form (multiple friends for single user-id will be split as the multiple same userid and single friend id for each user_id)
        Dataset<Row> user_friends= sparkSession.sql("select user_id, explode(friends) as friends from user_id_friends");
        user_friends.createOrReplaceTempView("user_friends");
        user_friends.printSchema();
        user_friends.show();
       // System.out.println("DONE3");
        
        // Total Original Data
        user_friends.foreach(new ForeachFunction<Row>() {
			@Override
			public void call(Row arg0) throws Exception {
				// TODO Auto-generated method stub
				original.put((String) arg0.getString(0), (String) arg0.getString(1));	
			}});
       // The below small part of code is inspired from https://stackoverflow.com/questions/39718021/converting-java-map-to-spark-dataframe-java-api?rq=1
        ArrayList<Tuple2<String, String>> list_or = new ArrayList<Tuple2<String, String>>();
        Set<String> allKeys_or = original.keySet();
        for (String key : allKeys_or) {
        	list_or.add(new Tuple2<String, String>(key, original.get(key)));
        };

        JavaRDD<Tuple2<String, String>> rdd = jsc.parallelize(list_or);

        StructType schema_org=DataTypes.createStructType(new StructField[]{
                DataTypes.createStructField("user_id",DataTypes.StringType,false,Metadata.empty()),
                DataTypes.createStructField("friends",DataTypes.StringType,false,Metadata.empty())
        });
        JavaPairRDD<String,  String> pairrdd = JavaPairRDD.fromJavaRDD(rdd);
        JavaRDD<Row> rowRDD = pairrdd.map(tuple -> RowFactory.create(tuple._1(),tuple._2()));
        Dataset<Row> df_original = sqlContext.createDataFrame(rowRDD, schema_org);
        df_original.createOrReplaceTempView("df_original");
        df_original.printSchema();
        System.out.println("Original Data");
        df_original.show();
 

        // First-Level Graph Data or Neighbours graph data
        HashMap<String, Set<String>> neighbours = new HashMap<String, Set<String>>();
        for(Map.Entry o: original.entrySet()) {
        	if(neighbours.containsKey((String) o.getKey())) {
        		HashSet<String> from0 = (HashSet<String>) neighbours.get( (String) o.getKey());
    			from0.add((String) o.getValue());
    			neighbours.put((String) o.getKey(), from0);
        	}
        	else {
        		HashSet<String> s0 = new HashSet<String>();
        		s0.add((String) o.getValue());
        		neighbours.put((String) o.getKey(), s0);
        		}	
        }
        System.out.println(neighbours.size());
        
        HashMap<String, String[]> neighbours_updated = new HashMap<String, String[]>();
        for(Map.Entry on: neighbours.entrySet()) {
        	neighbours_updated.put((String) on.getKey(),  ((Set<String>) on.getValue()).toArray(new String[((Set<String>) on.getValue()).size()]));
        	}
     // The below small part of code is inspired from https://stackoverflow.com/questions/39718021/converting-java-map-to-spark-dataframe-java-api?rq=1
        ArrayList<Tuple2<String, String[]>> list_ne = new ArrayList<Tuple2<String, String[]>>();

        Set<String> allKeys_ne = neighbours_updated.keySet();
        for (String key_ne : allKeys_ne) {
        		list_ne.add(new Tuple2<String, String[]>(key_ne, neighbours_updated.get(key_ne)));
        };

        JavaRDD<Tuple2<String, String[]>> rdd_ne = jsc.parallelize(list_ne);

        //System.out.println(rdd_ne.first());

        StructType struct_ne=DataTypes.createStructType(new StructField[]{
                DataTypes.createStructField("user_id",DataTypes.StringType,false,Metadata.empty()),
                DataTypes.createStructField("friends", new ArrayType(DataTypes.StringType, true),false,Metadata.empty())
          
        });
        
        JavaPairRDD<String,  String[]> pairrdd_ne = JavaPairRDD.fromJavaRDD(rdd_ne);
        JavaRDD<Row> rowRDD_ne = pairrdd_ne.map(tuple_ne -> RowFactory.create(tuple_ne._1(),tuple_ne._2()));
        Dataset<Row> df_neighbours_updated = sqlContext.createDataFrame(rowRDD_ne, struct_ne);
        //df_neighbours_updated.withColumn("weight", functions.lit((Integer) 3));
        df_neighbours_updated = df_neighbours_updated.withColumn("weight", lit(3));
        df_neighbours_updated.createOrReplaceTempView("df_neighbours_updated");
        df_neighbours_updated.printSchema();
        System.out.println("Neighbours Data");
        df_neighbours_updated.show();
        Dataset<Row> neighbours_explode= sparkSession.sql("select user_id, explode(friends) as friends, weight from df_neighbours_updated");
        neighbours_explode.createOrReplaceTempView("neighbours_explode");
        neighbours_explode.printSchema();
        System.out.println("neighbours_explode");
        neighbours_explode.show();
        
        
        // Second-Level Graph Data
        HashMap<String, HashSet<String>> second_level= new HashMap<String, HashSet<String>>();
        	for(Map.Entry f1: neighbours.entrySet()) {
        			HashSet<String> s1 = new HashSet<String>();
        			HashSet<String> from = (HashSet<String>) neighbours.get(f1.getKey());
        			//System.out.println(from);
        			Iterator<String> setIterator = from.iterator();
        				while(setIterator.hasNext()) {
        					String next = setIterator.next();
        					//System.out.println(next);
        					if(neighbours.containsKey(next))
        						s1.addAll( (HashSet<String>) neighbours.get(next));
        				}
        				from.addAll(s1);
        				second_level.put((String) f1.getKey(), from);
        		}
        	
          System.out.println(second_level.size());
          
          HashMap<String, String[]> second_level_updated = new HashMap<String, String[]>();
          for(Map.Entry s: second_level.entrySet()) {
        	  second_level_updated.put((String) s.getKey(),  ((Set<String>) s.getValue()).toArray(new String[((Set<String>) s.getValue()).size()]));
          	}
       // The below small part of code is inspired from https://stackoverflow.com/questions/39718021/converting-java-map-to-spark-dataframe-java-api?rq=1
          ArrayList<Tuple2<String, String[]>> list_sl = new ArrayList<Tuple2<String, String[]>>();

          Set<String> allKeys_sl = second_level_updated.keySet();
          for (String key_sl : allKeys_sl) {
        	  list_sl.add(new Tuple2<String, String[]>(key_sl, second_level_updated.get(key_sl)));
          };

          JavaRDD<Tuple2<String, String[]>> rdd_sl = jsc.parallelize(list_sl);

          
          StructType schema_sl=DataTypes.createStructType(new StructField[]{
                  DataTypes.createStructField("user_id",DataTypes.StringType,false,Metadata.empty()),
                  DataTypes.createStructField("friends", new ArrayType(DataTypes.StringType, true),false,Metadata.empty())
          });
          
          
          JavaPairRDD<String,  String[]> pairrdd_sl = JavaPairRDD.fromJavaRDD(rdd_sl);
          JavaRDD<Row> rowRDD_sl = pairrdd_sl.map(tuple_ne -> RowFactory.create(tuple_ne._1(),tuple_ne._2()));
          Dataset<Row> df_sl_updated = sqlContext.createDataFrame(rowRDD_sl, schema_sl);
         // df_sl_updated.withColumn("weight", functions.lit((Integer) 2));
          df_sl_updated = df_sl_updated.withColumn("weight", lit(2));
          System.out.println("Second Level");
          df_sl_updated.createOrReplaceTempView("df_sl_updated");
          df_sl_updated.printSchema();
          System.out.println("Second Level");
          df_sl_updated.show();
          Dataset<Row> sl_explode= sparkSession.sql("select user_id, explode(friends) as friends, weight from df_sl_updated");
          sl_explode.createOrReplaceTempView("sl_explode");
          sl_explode.printSchema();
          System.out.println("sl_explode");
          sl_explode.show();
          
          
          
        // Third-Level Graph Data  
        	HashMap<String, HashSet<String>> third_level= new HashMap<String, HashSet<String>>();
        	for(Map.Entry f2: second_level.entrySet()) {
        			HashSet<String> s2 = new HashSet<String>();
        			HashSet<String> from2 = (HashSet<String>) second_level.get(f2.getKey());
        			Iterator<String> setIterator2 = from2.iterator();
        				while(setIterator2.hasNext()) {
        					String next2 = setIterator2.next();
        					if(second_level.containsKey(next2))
        						s2.addAll( (HashSet<String>) second_level.get(next2));
        				}
        				from2.addAll(s2);
        				third_level.put((String) f2.getKey(), from2);
        		}
        	
            System.out.println(third_level.size());
            
            HashMap<String, String[]> third_level_updated = new HashMap<String, String[]>();
            for(Map.Entry t: third_level.entrySet()) {
            	third_level_updated.put((String) t.getKey(),  ((Set<String>) t.getValue()).toArray(new String[((Set<String>) t.getValue()).size()]));
            	}
         // The below small part of code is inspired from https://stackoverflow.com/questions/39718021/converting-java-map-to-spark-dataframe-java-api?rq=1
            ArrayList<Tuple2<String, String[]>> list_tl = new ArrayList<Tuple2<String, String[]>>();

            Set<String> allKeys_tl = third_level_updated.keySet();
            for (String key_tl : allKeys_tl) {
            	list_tl.add(new Tuple2<String, String[]>(key_tl, third_level_updated.get(key_tl)));
            };

            JavaRDD<Tuple2<String, String[]>> rdd_tl = jsc.parallelize(list_tl);

            //System.out.println(rdd_ne.first());

            StructType schema_tl=DataTypes.createStructType(new StructField[]{
                    DataTypes.createStructField("user_id",DataTypes.StringType,false,Metadata.empty()),
                    DataTypes.createStructField("friends", new ArrayType(DataTypes.StringType, true),false,Metadata.empty())
            });
            
            JavaPairRDD<String,  String[]> pairrdd_tl = JavaPairRDD.fromJavaRDD(rdd_tl);
            JavaRDD<Row> rowRDD_tl = pairrdd_tl.map(tuple_tl -> RowFactory.create(tuple_tl._1(),tuple_tl._2()));
            Dataset<Row> df_tl_updated = sqlContext.createDataFrame(rowRDD_tl, schema_tl);
            df_tl_updated = df_tl_updated.withColumn("weight", lit(1));
            System.out.println("Third Level");
            df_tl_updated.createOrReplaceTempView("df_tl_updated");
            df_tl_updated.printSchema();
            System.out.println("Third Level");
            df_tl_updated.show();
            Dataset<Row> tl_explode= sparkSession.sql("select user_id, explode(friends) as friends, weight from df_tl_updated");
            tl_explode.createOrReplaceTempView("tl_explode");
            tl_explode.printSchema();
            System.out.println("tl_explode");
            tl_explode.show();
            
            

            Dataset<Row> final_df= sparkSession.sql("select * from neighbours_explode union select * from sl_explode union select * from tl_explode");
            final_df.createOrReplaceTempView("final_df");
            final_df.printSchema();
            final_df.show();
            
            
			
			//ArrayList<String> userId=new ArrayList<>();
            Dataset<Row> userIDDRow= user_filter.select("user_id").toDF();
			List<String> userId = userIDDRow.as(Encoders.STRING()).collectAsList();
            Dataset<Row> Recommendations = sparkSession.sql("select business_id, count(*) as topReviews from review_filter where user_id in (select distinct f.friends from final_df f where f.user_id in (select u.user_id from user_filter u) order by weight)  and stars>=3 and business_id not in (select business_id from review_filter where user_id in (select u1.user_id  from user_filter u1 where u.user_id=u1.user_id)) group by business_id order by count(*) desc limit 10");
			Recommendations = Recommendations.withColumn("user_id", functions.lit(userId.get(0)));
            Recommendations.createOrReplaceTempView("Recommendations");
            Recommendations.printSchema();
            Recommendations.show();
			
			for(int i=1;i<userId.size();i++)
			{
				Dataset<Row> temp = sparkSession.sql("select business_id, count(*) as topReviews from review_filter where user_id in (select distinct f.friends from final_df f where f.user_id in (select u.user_id from user_filter u) order by weight)  and stars>=3 and business_id not in (select business_id from review_filter where user_id in (select u1.user_id  from user_filter u1 where u.user_id=u1.user_id)) group by business_id order by count(*) desc limit 10");
				temp = temp.withColumn("user_id", functions.lit(userId.get(i)));
				temp.createOrReplaceTempView("temp");
				Recommendations=sparkSession.sql("select * from Recommendations union select * from temp");
				Recommendations.createOrReplaceTempView("Recommendations");
			}
			Recommendations.printSchema();
			Recommendations.show();
            
			Recommendations.write().format("com.mongodb.spark.sql.DefaultSource").mode("append").option("database","yelp_rec").option("collection", "social").save();
	}


}
