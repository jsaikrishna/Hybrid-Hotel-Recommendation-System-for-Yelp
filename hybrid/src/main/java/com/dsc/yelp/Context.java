package com.dsc.yelp;

import org.bson.types.ObjectId;
import org.springframework.data.annotation.Id;

public class Context {


    @Id
    public ObjectId _id;

    public String userid;

    public String businessid;

    public String getUserid() {
        return userid;
    }

    public void setUserid(String userid) {
        this.userid = userid;
    }

    public String getBusinessid() {
        return businessid;
    }

    public void setBusinessid(String businessid) {
        this.businessid = businessid;
    }
}
