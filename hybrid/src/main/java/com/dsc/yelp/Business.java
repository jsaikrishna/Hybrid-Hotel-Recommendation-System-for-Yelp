package com.dsc.yelp;

import org.bson.types.ObjectId;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;

public class Business {

        @Id
        public ObjectId _id;

        public String businessId;
        public String name;
        public double latitude;
        public double longitude;

        public String getBusinessId() {
                return businessId;
        }

        public void setBusinessId(String businessId) {
                this.businessId = businessId;
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
