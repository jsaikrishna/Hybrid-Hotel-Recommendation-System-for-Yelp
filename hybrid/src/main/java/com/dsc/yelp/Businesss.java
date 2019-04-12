package com.dsc.yelp;

import org.bson.types.ObjectId;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;

public class Businesss {

        @Id
        public ObjectId _id;

        public String businessid;
        public String name;
        public double latitude;
        public double longitude;


        public String getBusinessid() {
                return businessid;
        }

        public void setBusinessid(String businessid) {
                this.businessid = businessid;
        }

        public String getName() {
                return name;
        }

        public void setName(String name) {
                this.name = name;
        }

        public double getLatitude() {
                return latitude;
        }

        public void setLatitude(double latitude) {
                this.latitude = latitude;
        }

        public double getLongitude() {
                return longitude;
        }

        public void setLongitude(double longitude) {
                this.longitude = longitude;
        }
}
