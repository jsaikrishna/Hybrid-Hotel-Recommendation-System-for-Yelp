package com.dsc.yelp;

import org.springframework.data.mongodb.repository.MongoRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface BusinessRepository extends MongoRepository<Businesss, String> {

    public Businesss findByName(String businessId);

    public Businesss findByBusinessid(String businessId);


}
