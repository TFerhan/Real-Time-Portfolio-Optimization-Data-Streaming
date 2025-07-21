package com.kafmongo.kafmongo.Service;


import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.kafmongo.kafmongo.Repo.IndexRTRepo;
import com.kafmongo.kafmongo.model.IndexRTModel;

@Service
public class IndexRTService {
	
	@Autowired
	private IndexRTRepo indexRepo;
	
	public void save(IndexRTModel indexModel) {
		boolean exists = indexRepo.existsByIndexAndFieldTransactTime(
	            indexModel.getIndex(), indexModel.getFieldTransactTime()
	        );
			if (!exists) {
				indexRepo.save(indexModel);
			}
		
	}

}
