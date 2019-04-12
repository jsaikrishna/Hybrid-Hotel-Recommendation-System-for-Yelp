#Windows - Script to start mongodb and import the json files into mongodb

#Starting MongoDb
"C:\Program Files\MongoDB\Server\4.0\bin\mongod.exe" --dbpath "F:\Data mining project\mongoData"

#Extracting tar file
tar -xvf yelp_dataset.tar

#Importing json file into mongodb
mongoimport --db yelp --collection business --file "F:\Data mining project\YelpDataset\yelp_academic_dataset_business.json"
mongoimport --db yelp --collection user --file "F:\Data mining project\YelpDataset\yelp_academic_dataset_user.json"
mongoimport --db yelp --collection review --file "F:\Data mining project\YelpDataset\yelp_academic_dataset_review.json"

mongoimport --db yelp_filter --collection business --file "F:\Data mining project\YelpDataset\business_filter.json"
mongoimport --db yelp_filter --collection user --file "F:\Data mining project\YelpDataset\user_filter.json"
mongoimport --db yelp_filter --collection review --file "F:\Data mining project\YelpDataset\review_filter.json"


#Exporting json files from mongodb
mongoexport --db yelp_filter --collection business --out business_filter.json
mongoexport --db yelp_filter --collection review --out review_filter.json
mongoexport --db yelp_filter --collection user --out user_filter.json

#Command for creating a zip file for all the files in the current directory
 tar -zcvf output.tar.gz imdb/*
