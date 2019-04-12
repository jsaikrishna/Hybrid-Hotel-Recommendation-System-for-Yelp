package com.dsc.yelp;

import org.springframework.data.mongodb.repository.MongoRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface CollaborativeRepository extends MongoRepository<Collaborative,String> {

    public Collaborative findByUserid(String userid);
}
