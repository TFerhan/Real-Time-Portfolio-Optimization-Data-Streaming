package com.kafmongo.kafmongo.Repo;

import com.kafmongo.kafmongo.model.DailyPriceId;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;
import com.kafmongo.kafmongo.model.DailyPrice;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.data.repository.query.Param;
import java.util.List;
import com.kafmongo.kafmongo.utils.DailyMetrics;
import java.util.Map;
import com.kafmongo.kafmongo.utils.MetricByTicker;

@Repository
public interface DailyPriceRepo extends JpaRepository<DailyPrice, DailyPriceId> {

    @Modifying
    @Transactional
    @Query(value = "DO $$ BEGIN " +
            "IF NOT EXISTS (SELECT 1 FROM timescaledb_information.hypertables WHERE hypertable_name = 'daily_price') THEN " +
            "PERFORM create_hypertable('daily_price', 'created', migrate_data => true); " +
            "END IF; " +
            "END $$;", nativeQuery = true)
    void ensureHypertableExists();

    @Query(value = "WITH weekly_returns AS ( " +
            "SELECT ticker, " +
            "(DATE_TRUNC('week', created) + INTERVAL '1 day')::DATE AS week_start, " +
            "LOG(LAST_VALUE(closing_price) OVER (PARTITION BY ticker, DATE_TRUNC('week', created) ORDER BY created RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) / " +
            "FIRST_VALUE(closing_price) OVER (PARTITION BY ticker, DATE_TRUNC('week', created) ORDER BY created)) AS weekly_log_return " +
            "FROM daily_price " +
            "WHERE created >= to_date(:date_start, 'YYYY-MM-DD') AND created <= to_date(:date_end, 'YYYY-MM-DD') " +
            "AND EXTRACT(DOW FROM created) BETWEEN 1 AND 5 " +
            "AND ticker = :ticker " +
            ") " +
            "SELECT DISTINCT ticker, " +
            "week_start as created, " +
            "weekly_log_return as dreturns " +
            "FROM weekly_returns " +
            "WHERE weekly_log_return IS NOT NULL " +
            "ORDER BY ticker, week_start;", nativeQuery = true)
    List<DailyMetrics> getWeeklyReturns(@Param("ticker") String ticker, @Param("date_start") String date_start, @Param("date_end") String date_end);

    @Query(value = "WITH monthly_prices AS ( " +
            "SELECT ticker, " +
            "DATE_TRUNC('month', created)::DATE AS month_start, " +
            "LOG(LAST_VALUE(closing_price) OVER (PARTITION BY ticker, DATE_TRUNC('month', created) ORDER BY created RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) / " +
            "FIRST_VALUE(closing_price) OVER (PARTITION BY ticker, DATE_TRUNC('month', created) ORDER BY created)) AS monthly_log_return " +
            "FROM daily_price " +
            "WHERE created >= to_date(:date_start, 'YYYY-MM-DD') AND created <= to_date(:date_end, 'YYYY-MM-DD')" +
            "AND ticker = :ticker " +
            ") " +
            "SELECT DISTINCT ticker, " +
            "month_start, " +
            "monthly_log_return " +
            "FROM monthly_prices " +
            "WHERE monthly_log_return IS NOT NULL " +
            "ORDER BY ticker, month_start;", nativeQuery = true)
    List<DailyMetrics> getMonthlyReturns(@Param("ticker") String ticker, @Param("date_start") String date_start, @Param("date_end") String date_end);

    @Query(value = "WITH daily_returns AS ( " +
            "SELECT ticker, " +
            "created, " +
            "ABS(LOG(closing_price / LAG(closing_price) OVER (PARTITION BY ticker ORDER BY created))) AS abs_return " +
            "FROM daily_price " +
            "WHERE created >= to_date(:date_start, 'YYYY-MM-DD') AND created <= to_date(:date_end, 'YYYY-MM-DD') " +
            "AND ticker = :ticker " +
            ") " +
            "SELECT d.ticker, " +
            "d.created, " +
            "d.abs_return / p.volume AS illiquidity_ratio " +
            "FROM daily_returns d " +
            "JOIN daily_price p ON d.ticker = p.ticker AND d.created = p.created " +
            "WHERE d.abs_return IS NOT NULL AND p.volume IS NOT NULL;", nativeQuery = true)
    List<DailyMetrics> getIlliquidityRatio(@Param("ticker") String ticker, @Param("date_start") String date_start, @Param("date_end") String date_end);
    
    
    @Query(value = "SELECT  SUM(cumul_volume_echange)/AVG(capitalisation) AS metric, " +
    		"ticker " +
            "FROM daily_price " +
            "GROUP BY ticker", 
    nativeQuery = true)
    List<MetricByTicker> getLiquidityRatiosByTicker();
    
    @Query(value = "SELECT AVG(cumul_titres_echanges) AS metric, " +
            "ticker " +
            "FROM daily_price " +
            "GROUP BY ticker", 
    nativeQuery = true)
    List<MetricByTicker> getAverageTitresEchangesByTicker();
    
    @Query(value = "SELECT capitalisation AS metric, ticker " +
            "FROM daily_price " +
            "WHERE (ticker, created) IN (" +
            "   SELECT ticker, MAX(created) " +
            "   FROM daily_price " +
            "   GROUP BY ticker" +
            ")", nativeQuery = true)
    List<MetricByTicker> findLatestCapitalisations();


    @Query(value = "WITH base_data AS ( " +
            "  SELECT ticker, created, closing_price, " +
            "         LAG(closing_price, :num_lags) OVER (PARTITION BY ticker ORDER BY created) AS prev_price " +
            "  FROM daily_price " +
            "  WHERE ticker = :ticker " +
            "    AND created >= to_date(:date_start, 'YYYY-MM-DD') " +
            "    AND created <= to_date(:date_end, 'YYYY-MM-DD') " +
            "), " +
            "daily_stock_log_returns AS ( " +
            "  SELECT ticker, created, " +
            "         LOG(closing_price / prev_price) AS daily_returns " +
            "  FROM base_data " +
            "  WHERE closing_price IS NOT NULL AND prev_price IS NOT NULL " +
            "        AND closing_price != 0 AND prev_price != 0 " +
            ") " +
            "SELECT ticker, created, daily_returns as dreturns " +
            "FROM daily_stock_log_returns " +
            "WHERE daily_returns IS NOT NULL;", nativeQuery = true)
    List<DailyMetrics> getDailyReturns(@Param("ticker") String ticker, @Param("date_start") String date_start, @Param("date_end") String date_end, @Param("num_lags") int num_lags);

    @Query(value = "SELECT * FROM daily_price " +
            "WHERE ticker = :ticker " +
            "AND created >= to_date(:date_start, 'YYYY-MM-DD') " +
            "AND created <= to_date(:date_end, 'YYYY-MM-DD')" +
            "ORDER BY created DESC", nativeQuery = true)
    List<DailyPrice> getDailyPrices(@Param("ticker") String ticker, @Param("date_start") String date_start, @Param("date_end") String date_end);
    
    
    @Query(value = "SELECT DISTINCT ticker FROM daily_price " 
            , nativeQuery = true)
    List<String> getAllTickers();
}
