package com.kafmongo.kafmongo.Repo;

import com.kafmongo.kafmongo.model.DailyIndexId;
import com.kafmongo.kafmongo.model.DailyIndexModel;
import com.kafmongo.kafmongo.model.DailyPrice;
import com.kafmongo.kafmongo.model.DailyPriceId;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

@Repository
public interface DailyIndexRepo extends JpaRepository<DailyIndexModel, DailyIndexId> {

    @Modifying
    @Transactional
    @Query(value = "DO $$ BEGIN " +
            "IF NOT EXISTS (SELECT 1 FROM timescaledb_information.hypertables WHERE hypertable_name = 'daily_index') THEN " +
            "PERFORM create_hypertable('daily_index', 'date', migrate_data => true); " +
            "END IF; " +
            "END $$;", nativeQuery = true)
    void ensureHypertableExists();
}