package com.dsc.yelp;

import org.bson.types.ObjectId;
import org.springframework.data.annotation.Id;

import java.util.List;

public class Collaborative {

    @Id
    public ObjectId _id;

    public List<String> businessid;

    public String userid;

    public List<String> getBusinessid() {
        return businessid;
    }

    public void setBusinessid(List<String> businessid) {
        this.businessid = businessid;
    }

    public String getUserid() {
        return userid;
    }

    public void setUserid(String userid) {
        this.userid = userid;
    }
}
