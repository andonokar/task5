-- creating bronze, silvers and gold tables
CREATE TABLE bronze (
                        id SERIAL PRIMARY KEY,
                        user_id bigint,
                        event_name varchar(255),
                        advertiser varchar(255),
                        campaign int,
                        gender varchar(255),
                        income varchar(255),
                        page_url varchar(255),
                        region varchar(255),
                        country varchar(255),
                        ingestion_time timestamp,
                        processed boolean
);

CREATE TABLE silver (
                        id bigint PRIMARY KEY,
                        user_id bigint,
                        event_name varchar(255),
                        advertiser varchar(255),
                        campaign int,
                        gender varchar(255),
                        income varchar(255),
                        page_url varchar(255),
                        location varchar(255),
                        cleaning_time timestamp,
                        processed boolean
);

CREATE TABLE silver_nok (
                            id bigint PRIMARY KEY,
                            user_id bigint,
                            event_name varchar(255),
                            advertiser varchar(255),
                            campaign int,
                            gender varchar(255),
                            income varchar(255),
                            page_url varchar(255),
                            location varchar(255),
                            cleaning_time timestamp,
                            processed boolean
);

CREATE TABLE gold (
                      advertiser varchar(255),
                      ctr float,
                      conversion_rate float,
                      conversion_click_rate float,
                      male_female_rate float,
                      income_25k_50k_rate varchar(255),
                      income_25k_below_rate varchar(255),
                      income_50k_75k_rate varchar(255),
                      income_75k_99k_rate float,
                      income_100k_above_rate float,
                      city_most_engaged varchar(255),
                      campaign_most_engaged varchar(255),
                      agg_time timestamp
);
-- creating function that process bronze table and insert into silver
CREATE OR REPLACE FUNCTION bronze_processing()
    -- void because the function itself doesn't return anything
    RETURNS VOID AS $$
DECLARE
    -- variable which will be used in the function
    event RECORD;
BEGIN
    -- Loop through records in the bronze table where processed is false
    FOR event IN
        -- Selecting only not processed data
        SELECT * FROM bronze WHERE processed = FALSE
        LOOP
            -- Check business rules
            IF event.gender = 'unknown' OR event.region = 'undefined' OR event.income = 'unknown' THEN
                -- Insert the record into the wrong table
                INSERT INTO silver_nok (id, user_id, event_name, advertiser, campaign, gender, income,
                                        page_url, location, cleaning_time, processed)
                VALUES (event.id, event.user_id, event.event_name, event.advertiser, event.campaign,
                        event.gender, event.income, event.page_url, event.region || '-' || event.country,
                        current_timestamp, false);
            ELSE
                -- Insert the record into the right table
                INSERT INTO silver (id, user_id, event_name, advertiser, campaign, gender, income,
                                    page_url, location, cleaning_time, processed)
                VALUES (event.id, event.user_id, event.event_name, event.advertiser, event.campaign,
                        event.gender, event.income, event.page_url, event.region || ' - ' || event.country,
                        current_timestamp, false);
            END IF;
            -- Update the processed flag to true for the processed records
            UPDATE bronze
            SET processed = TRUE
            WHERE id = event.id;
        END LOOP;
END
$$ LANGUAGE plpgsql;
-- creating function that process silver table and insert into gold
CREATE OR REPLACE FUNCTION silver_processing()
    -- void because the function itself doesn't return anything
    RETURNS VOID AS $$
DECLARE
    -- variable which will be used in the function
    adv RECORD;
BEGIN
    -- clearing the gold table to process everything
    TRUNCATE TABLE gold;
    -- Loop through the advertisers resulted in this complex query
    FOR adv in
        -- Selecting all silver table
        WITH silver_to_process AS (
            SELECT * FROM silver
        ),
            -- Creating a table with the count of the impressions, clicks, conversions per advertiser
             event_counts AS (
                 SELECT advertiser,
                        CAST(COUNT(CASE WHEN event_name = 'Click' THEN 1 END) AS FLOAT) AS click_count,
                        CAST(COUNT(CASE WHEN event_name = 'Impression' THEN 1 END) AS FLOAT) AS impression_count,
                        CAST(COUNT(CASE WHEN event_name = 'Downstream Conversion' THEN 1 END) AS FLOAT)
                            AS conversion_count
                 FROM silver_to_process
                 GROUP BY advertiser
             ),
            -- Creating a table with count of male and female per advertiser
             gender_counts AS (
                 SELECT advertiser,
                        CAST(COUNT(CASE WHEN gender = 'Male' THEN 1 END) AS FLOAT) AS male_count,
                        CAST(COUNT(CASE WHEN gender = 'Female' THEN 1 END) AS FLOAT) AS female_count
                 FROM silver_to_process
                 GROUP BY advertiser
             ),
            -- Creating a table with count of different incomes per advertiser
             income_counts AS (
                 SELECT advertiser,
                        CAST(COUNT(CASE WHEN income = '25k and below' THEN 1 END) AS FLOAT) AS income_25k_below_count,
                        CAST(COUNT(CASE WHEN income = '25k - 50k' THEN 1 END) AS FLOAT) AS income_25k_50k_count,
                        CAST(COUNT(CASE WHEN income = '50k - 75k' THEN 1 END) AS FLOAT) AS income_50k_75k_count,
                        CAST(COUNT(CASE WHEN income = '75k - 99k' THEN 1 END) AS FLOAT) AS income_75k_99k_count,
                        CAST(COUNT(CASE WHEN income = '100k+' THEN 1 END) AS FLOAT) AS income_100k_above_count
                 FROM silver_to_process
                 GROUP BY advertiser
             ),
            -- Creating a table with the best engaged city per advertiser
             location_max AS (
                 SELECT advertiser,
                        MAX(location) AS city_most_engaged
                 FROM (
                          SELECT advertiser,
                                 location,
                                 ROW_NUMBER() OVER (PARTITION BY advertiser ORDER BY city_count DESC) AS rn
                          FROM (
                                   SELECT advertiser, location, COUNT(location) AS city_count
                                   FROM silver_to_process
                                   GROUP BY advertiser, location
                               ) AS city_counts
                      ) AS ranked_cities
                 WHERE rn = 1
                 GROUP BY advertiser
             ),
             -- Creating a table with the best engaged city per advertiser
             campaign_max AS (
                 SELECT advertiser,
                        MAX(campaign) AS campaign_most_engaged
                 FROM (
                          SELECT advertiser,
                                 campaign,
                                 ROW_NUMBER() OVER (PARTITION BY advertiser ORDER BY campaign_count DESC) AS rn
                          FROM (
                                   SELECT advertiser, campaign, COUNT(campaign) AS campaign_count
                                   FROM silver_to_process
                                   GROUP BY advertiser, campaign
                               ) AS campaign_counts
                      ) AS ranked_campaigns
                 WHERE rn = 1
                 GROUP BY advertiser
             )
        -- Creating the final table based on the business rule and the temporary tables created
        SELECT ec.advertiser,
               ec.click_count / NULLIF(ec.impression_count, -1) AS CTR,
               ec.conversion_count / NULLIF(ec.impression_count, -1) AS conversion_rate,
               ec.conversion_count / NULLIF(ec.click_count, -1) AS conversion_click_rate,
               gender_counts.male_count / NULLIF(gender_counts.female_count, -1) AS male_female_rate,
               income_counts.income_25k_50k_count / NULLIF(
                       income_counts.income_25k_below_count + income_counts.income_25k_below_count +
                       income_counts.income_50k_75k_count + income_counts.income_75k_99k_count +
                       income_counts.income_100k_above_count, -1) AS income_25k_50k_rate,
               income_counts.income_25k_below_count / NULLIF(
                       income_counts.income_25k_below_count + income_counts.income_25k_50k_count +
                       income_counts.income_50k_75k_count + income_counts.income_75k_99k_count +
                       income_counts.income_100k_above_count, -1) AS income_25k_below_rate,
               income_counts.income_50k_75k_count / NULLIF(
                       income_counts.income_50k_75k_count + income_counts.income_25k_below_count +
                       income_counts.income_25k_50k_count + income_counts.income_75k_99k_count +
                       income_counts.income_100k_above_count, -1) AS income_50k_75k_rate,
               income_counts.income_75k_99k_count / NULLIF(
                       income_counts.income_75k_99k_count + income_counts.income_25k_below_count +
                       income_counts.income_25k_50k_count + income_counts.income_50k_75k_count +
                       income_counts.income_100k_above_count, -1) AS income_75k_99k_rate,
               income_counts.income_100k_above_count / NULLIF(
                       income_counts.income_75k_99k_count + income_counts.income_25k_below_count +
                       income_counts.income_25k_50k_count + income_counts.income_50k_75k_count +
                       income_counts.income_100k_above_count, -1) AS income_100k_above_rate,
               location_max.city_most_engaged AS city_most_engaged,
               campaign_max.campaign_most_engaged AS campaign_most_engaged,
               CURRENT_TIMESTAMP AS agg_time
        FROM event_counts ec
                 LEFT JOIN gender_counts ON ec.advertiser = gender_counts.advertiser
                 LEFT JOIN income_counts ON ec.advertiser = income_counts.advertiser
                 LEFT JOIN location_max ON ec.advertiser = location_max.advertiser
                 LEFT JOIN campaign_max ON ec.advertiser = campaign_max.advertiser
        LOOP
            INSERT INTO gold (advertiser, ctr, conversion_rate, conversion_click_rate, male_female_rate,
                              income_25k_50k_rate, income_25k_below_rate, income_50k_75k_rate, income_75k_99k_rate,
                              income_100k_above_rate, city_most_engaged, campaign_most_engaged, agg_time)
            VALUES (adv.advertiser, adv.CTR, adv.conversion_rate, adv.conversion_click_rate, adv.male_female_rate,
                    adv.income_25k_50k_rate, adv.income_25k_below_rate, adv.income_50k_75k_rate, adv.income_75k_99k_rate,
                    adv.income_100k_above_rate, adv.city_most_engaged, adv.campaign_most_engaged, adv.agg_time);
        END LOOP;

END
$$ LANGUAGE plpgsql;
-- Creating a cron for the bronze processing - each 10 minutes
SELECT cron.schedule('*/10 * * * *', $$
    SELECT bronze_processing()
$$);
-- Creating a cron for silver processing - every 10 hours
SELECT cron.schedule('0 * * * *', $$
    SELECT silver_processing()
$$);