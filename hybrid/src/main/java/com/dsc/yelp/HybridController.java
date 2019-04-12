package com.dsc.yelp;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.ArrayList;
import java.util.List;

@CrossOrigin
@RestController
public class HybridController {

    @Autowired
    private BusinessRepository businessRepository;

    @Autowired
    CollaborativeRepository collaborativeRepository;

    @Autowired
    ContextRepository contextRepository;

    @CrossOrigin
    @RequestMapping(value = "/hybrid", method = RequestMethod.GET)
    public List<Recommendation> home(@RequestParam(value = "user") String user) {
        System.out.println(user);

        Collaborative collaborativeRecs = collaborativeRepository.findByUserid(user);
        Context contextRecs = contextRepository.findByUserid(user);
        List<String> businessIds = new ArrayList<>();
        System.out.println("Collaborative "+collaborativeRecs.businessid.size());
        System.out.println("Context "+contextRecs.businessid.split(" ").length);

        for(String businessId : collaborativeRecs.businessid){
            businessIds.add(businessId);
        }

//        businessIds.add("IeXVSmtOx0-kntCM_ZX0Zw");
//        businessIds.add("LFs5jyYdXlzi0SpAYi1eSA");

        List<Recommendation> list = new ArrayList<>();
        for(String businessId : businessIds){
//            Business businessPojo = businessRepository.findByName(businessId);
            System.out.println(businessId);
            Businesss businessPojo = businessRepository.findByBusinessid(businessId);
            System.out.println(businessPojo);
            Recommendation recommendation = new Recommendation();
            recommendation.setLatitude(businessPojo.getLatitude());
            recommendation.setLongitude(businessPojo.getLongitude());
            recommendation.setType("collaborative");
            recommendation.setName(businessPojo.getName());
            list.add(recommendation);
        }

        businessIds.clear();
        for(String businessId : contextRecs.businessid.split(" ")){
            businessIds.add(businessId);
        }

        for(String businessId : businessIds){
//            Business businessPojo = businessRepository.findByName(businessId);
            System.out.println(businessId);
            Businesss businessPojo = businessRepository.findByBusinessid(businessId);
            System.out.println(businessPojo);
            Recommendation recommendation = new Recommendation();
            recommendation.setLatitude(businessPojo.getLatitude());
            recommendation.setLongitude(businessPojo.getLongitude());
            recommendation.setType("context");
            recommendation.setName(businessPojo.getName());
            list.add(recommendation);
        }

        return list;
    }

}
