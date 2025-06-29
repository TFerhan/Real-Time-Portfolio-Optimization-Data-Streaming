package com.kafmongo.kafmongo.Repo;

import java.util.Date;

import org.springframework.data.mongodb.repository.MongoRepository;
import com.kafmongo.kafmongo.model.IndexRTModel;



public interface IndexRTRepo extends MongoRepository<IndexRTModel, String> {
	
	boolean existsByIndexAndFieldTransactTime(String indice, Date fieldTransactTime);

}
