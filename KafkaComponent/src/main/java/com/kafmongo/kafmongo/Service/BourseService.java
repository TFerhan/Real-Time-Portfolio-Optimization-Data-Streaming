package com.kafmongo.kafmongo.Service;
import org.springframework.stereotype.Service;
import org.springframework.beans.factory.annotation.Autowired;
import com.kafmongo.kafmongo.Repo.BourseRepo;
import com.kafmongo.kafmongo.model.BourseModel;
import java.util.Date;


@Service
public class BourseService {
	
	@Autowired
	private BourseRepo bourseRepo;
	
	public void save(BourseModel bourseModel) {
		boolean exists = bourseRepo.existsByTickerAndFieldTransactTime(
	            bourseModel.getTicker(), bourseModel.getFieldDateApplication()
	        );
			if (!exists) {
				bourseRepo.save(bourseModel);
			}
		
	}
	
	
	
	
	

}
