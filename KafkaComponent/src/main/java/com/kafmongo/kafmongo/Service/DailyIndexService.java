package com.kafmongo.kafmongo.Service;

import com.kafmongo.kafmongo.Repo.DailyIndexRepo;
import com.kafmongo.kafmongo.model.DailyIndexModel;
import com.kafmongo.kafmongo.model.DailyPrice;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
public class DailyIndexService {
    @Autowired
    private DailyIndexRepo dailyIndexRepo;

    @Transactional
    public DailyIndexModel saveDailyIndex(DailyIndexModel dailyindex) {
        // Ensure the table is a hypertable before saving
        dailyIndexRepo.ensureHypertableExists();

        // Save the entity
        return dailyIndexRepo.save(dailyindex);
    }
}