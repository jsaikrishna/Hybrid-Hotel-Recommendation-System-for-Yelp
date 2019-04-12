import com.mongodb.spark.MongoSpark;
import com.mongodb.spark.config.ReadConfig;
import com.mongodb.spark.rdd.api.java.JavaMongoRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.feature.*;
import org.apache.spark.ml.linalg.DenseVector;

import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.*;

import static org.apache.spark.sql.functions.lit;
import org.apache.spark.api.java.function.ForeachFunction;

import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.bson.Document;
import scala.Tuple2;
import scala.collection.JavaConversions;


import java.io.IOException;
import java.util.*;

public class ContentFilterModel {

    public static void main(String[] args) {
        SparkSession sparkSession = SparkSession.builder()
                .master("local[*]")
                .appName("MongoSparkConnector")
                .config("spark.mongodb.input.uri", "mongodb://ec2-54-86-114-144.compute-1.amazonaws.com/yelp_filter.business")
                .config("spark.mongodb.output.uri", "mongodb://ec2-54-86-114-144.compute-1.amazonaws.com/yelp_rec.context")
                .getOrCreate();
        JavaSparkContext jsc = new JavaSparkContext(sparkSession.sparkContext());

        Map<String, String> readOverride = new HashMap<String, String>();
        readOverride.put("collection", "review");
        readOverride.put("readPreference.name", "secondaryPreferred");
        ReadConfig readConfig = ReadConfig.create(jsc).withOptions(readOverride);

        Map<String, String> userOverride = new HashMap<String, String>();
        userOverride.put("collection", "user");
        userOverride.put("readPreference.name", "secondaryPreferred");
        ReadConfig userConfig = ReadConfig.create(jsc).withOptions(userOverride);

        JavaMongoRDD<Document> reviewRDD = MongoSpark.load(jsc, readConfig);
        JavaMongoRDD<Document> userRDD = MongoSpark.load(jsc, userConfig);
        JavaMongoRDD<Document> businessRDD = MongoSpark.load(jsc);

        //Loading the required RDD's for the program

        Dataset<Row> review = reviewRDD.toDF();
        Dataset<Row> business = businessRDD.toDF();
        Dataset<Row> user = userRDD.toDF();

        business.printSchema();
        review.printSchema();
        user.printSchema();

        review.createOrReplaceTempView("reviews");
        business.createOrReplaceTempView("businesses");
        user.createOrReplaceTempView("users");

        //Selecting the business_id and it's corresponding reviews

        Dataset<Row> main_reviews = sparkSession.sql("SELECT business_id, text FROM reviews");
        main_reviews.printSchema();

        //Map Reduce implemented to append all the reviews to it's respective business_id

        JavaPairRDD<String, String> mapRDD = main_reviews.toJavaRDD().mapToPair(new PairFunction<Row, String, String>() {
            public Tuple2<String, String> call(Row row) throws Exception {
                return new Tuple2<String, String>((String) row.get(0), (String) row.get(1));
            }
        });
        Function2<String, String, String> reduceRDD = (String a, String b) -> {
            return a+" "+b;
        };
        JavaPairRDD<String, String> temp = mapRDD.reduceByKey(reduceRDD);

        Dataset<Row> grouped_dataset = sparkSession.createDataset(temp.collect(), Encoders.tuple(Encoders.STRING(),Encoders.STRING())).toDF("business_id","text");
        //grouped_dataset.show(1,false);
        //System.out.println("************************************" + grouped_dataset.count());

        //Creating the Model of the Context Filtering

        //Step 1: Remove all non alpha numeric words and tokenize them
        RegexTokenizer regexTokenizer = new RegexTokenizer()
                .setInputCol("text")
                .setOutputCol("tokens")
                .setPattern("\\w+").setGaps(false);
        //Step 2: Remove all StopWords
        StopWordsRemover stopWordsRemover = new StopWordsRemover()
                .setInputCol("tokens")
                .setOutputCol("nsw");
        //Step 3: Create a countVectorizer which essentially transforms all words to float values
        CountVectorizer countVectorizer = new CountVectorizer()
                .setInputCol("nsw")
                .setOutputCol("features");
        //Step 4: Using the IDF with the Vector model
        IDF iDF = new IDF()
                .setInputCol("features")
                .setOutputCol("id_vector");
        //Step 5: Creating the word to Vector Array of size 100
        Word2Vec word2Vec = new Word2Vec()
                .setInputCol("nsw")
                .setOutputCol("word_vec")
                .setVectorSize(100)
                .setMinCount(5)
                .setSeed(123);
        //Step 6: This is to combine the two vectors of IDF and Word_Vec
        VectorAssembler vectorAssembler = new VectorAssembler()
                .setInputCols(new String[]{"id_vector", "word_vec"})
                .setOutputCol("full_vec");
        //Step 7: Created a Pipeline of the above jobs
        Pipeline pipeline = new Pipeline()
                .setStages(new PipelineStage[] {regexTokenizer, stopWordsRemover, countVectorizer, iDF, word2Vec, vectorAssembler});

        //PipelineModel model = pipeline.fit(grouped_dataset);

        //Saving the Model
        /*try {
            model.write().overwrite().save("./context_model");
        } catch (IOException e) {
            e.printStackTrace();
        }*/

        //Model would be loaded/saved in the hdfs
        PipelineModel load_model = PipelineModel.read().load("./context_model");

        //Using the above created Model to transform it to the required Vector Model
        Dataset<Row> predictions = load_model.transform(grouped_dataset);
        predictions.printSchema();

        //Map function to get all Vectors of each business and create a List of them
        JavaRDD<Tuple2<String, DenseVector>> x = predictions.toJavaRDD().map(new Function<Row, Tuple2<String, DenseVector>>() {
            public Tuple2<String, DenseVector> call(Row row) throws Exception {
                return new Tuple2<String, DenseVector>(row.getString(0), row.getAs("word_vec"));
            }
        });
        List<Tuple2<String, DenseVector>> full_vectors = x.collect();

        //System.out.println("$$$$$$$$$$$$$$$************************************" + full_vectors.get(0));

        //sqRX-XLlhx4rs2c1TpBf8A

        SQLContext sqlContext=new SQLContext(sparkSession);
        JavaSparkContext sc=new JavaSparkContext(sparkSession.sparkContext());

        Dataset<Row> users_ids = sparkSession.sql("SELECT user_id from users");
        users_ids.printSchema();
        List<String> l_users_ids = users_ids.as(Encoders.STRING()).collectAsList();
        System.out.println(l_users_ids.get(0));
        //Call to the main function to get all recommendations
        List<String> main_final = new ArrayList<>();
        for(int i = 0; i< l_users_ids.size(); i++) {
            Dataset<Row> final_dataset = recommendations(l_users_ids.get(i), sc, sqlContext, full_vectors, business, review);
            //final_dataset.show(false);
            List<String> temp_df = final_dataset.select("business_id").javaRDD().map(r -> r.getString(0)).collect();
            List<String> final_df = new ArrayList<>();
            String main = new String();
            main = main + l_users_ids.get(i) + ",";
            for (int j = 0; j < temp_df.size(); j++) {
                main = main + temp_df.get(j) + " ";
            }
            main_final.add(main);
        }
        Dataset<Row> df = sqlContext.createDataset(main_final, Encoders.STRING()).toDF();
        df.printSchema();
        df.show();
        // Convert
        Dataset<Row> df1 = df.selectExpr("split(value, ',')[0] as userid", "split(value, ',')[1] as businessid");
        df1.printSchema();
        df1.show();
        df1.write().format("com.mongodb.spark.sql.DefaultSource").mode("append").option("database","yelp_rec").option("collection", "context").save();

    }

    //Function to calculate Cosine Similarity of two DenseVectors
    public static double cosineSimilarity(DenseVector vectorA, DenseVector vectorB) {
        double dotProduct = 0.0;
        double normA = 0.0;
        double normB = 0.0;
        for (int i = 0; i < vectorA.size(); i++) {
            dotProduct += vectorA.toArray()[i] * vectorB.toArray()[i];
            normA += Math.pow(vectorA.toArray()[i], 2);
            normB += Math.pow(vectorB.toArray()[i], 2);
        }
        return dotProduct / (Math.sqrt(normA) * Math.sqrt(normB));
    }

    //Function to get all the similar hotels to which the user has given reviews
    public static Dataset<Row> allSimilarHotels(List<String>hotel_ids, JavaSparkContext sc, SQLContext sqlContext, List<Tuple2<String, DenseVector>> full_vectors){
        //Limiting to 10 hotels for each user
        Integer limit_of_hotels=10;

        //Created a Schema for the Final RDD which has the input hotel and all its similar hotels with it's corresponding score
        StructType businessSchema= DataTypes.createStructType(new StructField[]{
                DataTypes.createStructField("business_id",DataTypes.StringType,true, Metadata.empty()),
                DataTypes.createStructField("score",DataTypes.IntegerType,true,Metadata.empty()),
                DataTypes.createStructField("input",DataTypes.StringType,true, Metadata.empty())
        });

        Dataset<Row> similar_hotels = sqlContext.createDataFrame(sc.emptyRDD(),businessSchema);

        DenseVector input_vec = full_vectors.get(0)._2;

        //Go through all hotel_ids to find the cosine similarity between all hotels and user rated hotels
        for (int i = 0; i < hotel_ids.size(); i++) {
            //Loop to find the Vector of the user rated hotels
            for (int j = 0; j < full_vectors.size(); j++){
                if (hotel_ids.get(i).equals(full_vectors.get(j)._1)) {
                    input_vec = full_vectors.get(j)._2;
                    break;
                }
            }

            List<Row> temp = new ArrayList<Row>();
            //Loop to find the cosine similarity between all hotels and user rated hotels
            for (int j = 0; j < full_vectors.size(); j++){
                Row row = RowFactory.create(full_vectors.get(j)._1,cosineSimilarity(input_vec,full_vectors.get(j)._2));
                temp.add(row);
            }
            JavaRDD<Row> hotels_rdd = sc.parallelize(temp);

            //Create a Structure for the above found out hotels and the scores
            StructType tempSchema = DataTypes.createStructType(new StructField[]{
                    DataTypes.createStructField("_1",DataTypes.StringType,true, Metadata.empty()),
                    DataTypes.createStructField("_2",DataTypes.DoubleType,true,Metadata.empty()),
            });

            //Taking the top values using "score" field
            Dataset<Row> hotels_dataset = sqlContext.createDataFrame(hotels_rdd,tempSchema).withColumnRenamed("_1", "business_id").withColumnRenamed("_2", "score").orderBy(org.apache.spark.sql.functions.col("score").desc());
            //hotels_dataset.show(1,false);

            //All the top hotels similar to the hotel other than itself
            hotels_dataset = hotels_dataset.filter(hotels_dataset.col("business_id").notEqual(hotel_ids.get(i))).limit(limit_of_hotels);
            hotels_dataset = hotels_dataset.withColumn("input", lit(hotel_ids.get(i)));
            //Union to not repeat the same hotel again
            similar_hotels = similar_hotels.union(hotels_dataset);
            //hotels_dataset.show(false);
        }
        return similar_hotels;
    }

    public static Dataset<Row> hotelDetails(Dataset<Row> temp_dataset, Dataset<Row> business){
        //From the actual dataset we take other details to show on the map
        Dataset<Row> a = temp_dataset.alias("a");
        Dataset<Row> b = business.alias("b");

        List<Column> filterColumns = new ArrayList<>();
        for (String s: a.columns()) {
            filterColumns.add(new Column("a."+s));
        }
        filterColumns.add(new Column("b.name"));
        filterColumns.add(new Column("b.categories"));
        filterColumns.add(new Column("b.stars"));
        filterColumns.add(new Column("b.review_count"));
        filterColumns.add(new Column("b.latitude"));
        filterColumns.add(new Column("b.longitude"));

        return a.join(b, a.col("a.business_id").equalTo(b.col("b.business_id")), "inner").select(JavaConversions.asScalaBuffer(filterColumns).seq());
    }

    public static Dataset<Row> recommendations(String u_id, JavaSparkContext sc, SQLContext sqlContext, List<Tuple2<String, DenseVector>> full_vectors,Dataset<Row> business,Dataset<Row> reviews){
        reviews.createOrReplaceTempView("reviews");
        //Using the user id, we find all the hotels to which user has given >= 3 stars
        String query = "SELECT distinct business_id FROM reviews where stars >= 3.0 and user_id = '"+u_id+"'";
        Dataset<Row>all_user_hotels = sqlContext.sql(query);
        //all_user_hotels.show(false);
        //This step is to take random 5 hotels out of all the hotels which are >= 3 rating
        //all_user_hotels = all_user_hotels.sample(false, 0.5).limit(5);

        //Printing all the hotels details that are taken for the user
        Dataset<Row> all_hotel_details = hotelDetails(all_user_hotels, business);
        //all_hotel_details.select("business_id", "name", "categories").show(false);
        //all_user_hotels.collect();
        List<Row> temp = new ArrayList<>();

        //Creating a list of all the hotel ids of the user hotels
        all_user_hotels.collect();
        List<String> user_values = new ArrayList<>();

        user_values = all_hotel_details.select("business_id").javaRDD().map(r-> r.getString(0)).collect();

        //System.out.println("&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&$$$$$$$$$$$$$$$"+user_values);
        //Function call to get all the similar hotels to the users rated hotels
        Dataset<Row> all_similar_hotels = allSimilarHotels(user_values,sc,sqlContext,full_vectors);

        //all_similar_hotels.show(false);

        List<Column> filterColumns = new ArrayList<>();
        filterColumns.add(new Column("a.business_id"));
        filterColumns.add(new Column("a.score"));

        //Joining the two Datasets to get the final dataset
        Dataset<Row> a = all_similar_hotels.alias("a");
        Dataset<Row> b = all_user_hotels.alias("b");
        Dataset<Row> c = a.join(b, a.col("a.business_id").equalTo(b.col("b.business_id")) , "left_outer").where(b.col("b.business_id").isNull()).select(JavaConversions.asScalaBuffer(filterColumns).seq());

        //Top 10 of the hotels depending on the score
        Dataset<Row> final_dataset = c.orderBy(org.apache.spark.sql.functions.col("score").desc()).limit(10);

        //Call to get additional information to display on map
        return hotelDetails(final_dataset,business);
    }

}
