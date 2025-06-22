-------------------------------------------------- WEEK 1: SQL Foundations & Intermediate Concepts-----------------------------
-------------------- Day 1 : Basic SQL Refresher (SELECT, WHERE, GROUP BY, JOINs) 
-- Article- https://sqlbolt.com/
-- Questions:
-- https://leetcode.com/problems/combine-two-tables/
SELECT
    p.firstname,
    p.lastname,
    coalesce(a.city) AS city,
    coalesce(a.state) AS state
FROM
    person p
    LEFT JOIN address a ON a.personid = p.personid;

-- https://leetcode.com/problems/second-highest-salary/
SELECT
    DISTINCT CASE
        WHEN count(DISTINCT salary) < 2 THEN NULL
        ELSE (
            SELECT
                DISTINCT salary
            FROM
                employee
            ORDER BY
                salary DESC
            LIMIT
                1 OFFSET 1
        )
    END AS SecondHighestSalary
FROM
    employee;

----------------------- Day 2 : Advanced Joins & Subqueries
-- Article- https://www.w3schools.com/sql/sql_join.asp
-- Questions:
-- https://leetcode.com/problems/employees-earning-more-than-their-managers/
SELECT
    e.name AS Employee
FROM
    employee e
    JOIN employee m ON e.managerid = m.id
WHERE
    e.salary > m.salary;

-- https://leetcode.com/problems/department-top-three-salaries/description/
WITH ct AS (
    SELECT
        d.name AS Department,
        e.name AS Employee,
        e.salary AS Salary,
        dense_rank() over(
            PARTITION by d.name
            ORDER BY
                e.Salary DESC
        ) AS ranked
    FROM
        employee e
        JOIN department d ON e.departmentid = d.id
)
SELECT
    Department,
    Employee,
    Salary
FROM
    ct
WHERE
    ranked <= 3;

-- https://platform.stratascratch.com/coding/10300-premium-vs-freemium?code_type=1
-- finding total downloads for paying and non paying users by date 
-- non paying has more number that paying
WITH paid_customer AS (
    SELECT
        df.date,
        sum(df.downloads)
    FROM
        ms_user_dimension ud
        JOIN ms_acc_dimension ad ON ad.acc_id = ud.acc_id
        AND paying_customer = 'yes'
        JOIN ms_download_facts df ON df.user_id = ud.user_id
    GROUP BY
        df.date
),
non_paid_customer AS (
    SELECT
        df.date,
        sum(df.downloads)
    FROM
        ms_user_dimension ud
        JOIN ms_acc_dimension ad ON ad.acc_id = ud.acc_id
        AND paying_customer = 'no'
        JOIN ms_download_facts df ON df.user_id = ud.user_id
    GROUP BY
        df.date
)
SELECT
    np.date,
    np.sum AS non_paying,
    pc.sum AS paying
FROM
    non_paid_customer np
    JOIN paid_customer pc ON pc.date = np.date
WHERE
    np.sum > pc.sum
ORDER BY
    np.date;

--------------------------- Day 3 :CTEs & Nested Queries
-- Article- https://www.udemy.com/course/the-complete-sql-bootcamp/
-- Questions:
-- https://leetcode.com/problems/customers-who-bought-all-products/
SELECT
    customer_id
FROM
    customer
GROUP BY
    customer_id
HAVING
    count(DISTINCT product_key) = (
        SELECT
            count(1)
        FROM
            product
    );

-- https://platform.stratascratch.com/coding/10064-highest-energy-consumption
-- 1st Approach
-- Execution time: 0.01104 seconds
WITH ct AS (
    SELECT
        coalesce(
            e.recorded_date,
            a.recorded_date,
            n.recorded_date
        ) recorded_date,
        sum(
            COALESCE(e.consumption, 0) + COALESCE(a.consumption, 0) + COALESCE(n.consumption, 0)
        ) total_consumption
    FROM
        fb_eu_energy e FULL
        OUTER JOIN fb_asia_energy a ON a.recorded_date = e.recorded_date FULL
        OUTER JOIN fb_na_energy n ON n.recorded_date = a.recorded_date
        AND n.recorded_date = e.recorded_date
    GROUP BY
        1
)
SELECT
    recorded_date,
    total_consumption
FROM
    ct
WHERE
    total_consumption = (
        SELECT
            max(total_consumption)
        FROM
            ct
    );

-- 2nd Approach 
-- Execution time: 0.04681 seconds
WITH
UNION
ALL WITH ct AS (
    SELECT
        recorded_date,
        sum(consumption) AS total_consumption
    FROM
        (
            SELECT
                recorded_date,
                consumption
            FROM
                fb_eu_energy e
            UNION
            ALL
            SELECT
                recorded_date,
                consumption
            FROM
                fb_asia_energy
            UNION
            ALL
            SELECT
                recorded_date,
                consumption
            FROM
                fb_na_energy
        )
    GROUP BY
        1
)
SELECT
    recorded_date,
    total_consumption
FROM
    ct
WHERE
    total_consumption = (
        SELECT
            max(total_consumption)
        FROM
            ct
    );

-- https://datalemur.com/questions/linkedin-power-creators
SELECT
    pp.profile_id
FROM
    personal_profiles pp
    JOIN company_pages cp ON cp.company_id = pp.employer_id
WHERE
    pp.followers > cp.followers
ORDER BY
    1;

--------------------------- Day 4 : Window Functions (ROW_NUMBER, RANK, DENSE_RANK)
-- Article:
--  https://mode.com/sql-tutorial/sql-window-functions  
-- Questions:
-- https://leetcode.com/problems/rank-scores/
SELECT
    score,
    ranked AS `rank`
FROM
    (
        SELECT
            score,
            dense_rank() over(
                ORDER BY
                    score DESC
            ) ranked
        FROM
            scores
        ORDER BY
            ranked
    ) s;

-- https://platform.stratascratch.com/coding/9680-most-profitable-companies
SELECT
    company,
    profits
FROM
    (
        SELECT
            company,
            profits,
            dense_rank() over(
                ORDER BY
                    profits DESC
            ) ranked
        FROM
            forbes_global_2010_2014
    ) s
WHERE
    ranked <= 3
ORDER BY
    ranked
LIMIT
    3;

-- https://datalemur.com/questions/supercloud-customer
SELECT
    c.customer_id
FROM
    customer_contracts c
    LEFT JOIN products p ON p.product_id = c.product_id
GROUP BY
    c.customer_id
HAVING
    count(DISTINCT p.product_category) = (
        SELECT
            count(DISTINCT product_category)
        FROM
            products
    );

------------------------------ Day 5: Window Functions (LEAD, LAG, FIRST_VALUE)
-- Article: 
-- https://www.sqlservertutorial.net/sql-server-window-functions/
-- https://www.udemy.com/course/the-advanced-sql-course-2021/
-- Questions:
-- https://leetcode.com/problems/consecutive-numbers/
-- https://platform.stratascratch.com/coding/2053-monthly-retention-rate
-- https://datalemur.com/questions/yoy-growth-rate
----
----
----- 
----------------------------- Day 6 : Date/Time Manipulation (EXTRACT, DATE_TRUNC, DATEDIFF)
-- Articles:
--  https://www.sqltutorial.org/sql-date-functions/
-- https://mode.com/blog/postgres-sql-date-functions
-- https://mode.com/blog/date-trunc-sql-timestamp-function-count-on
-- https://mode.com/sql-tutorial/sql-datetime-format
-- https://docs.snowflake.com/en/sql-reference/functions-conversion#label-date-time-format-conversion
--  Section 1. Getting current date and time functions
--  * current_date * current_time  * current_timestamp
-- Section 2. Extracting Date and Time Components
-- * EXTRACT- Standard SQL Function * DATEPART-works only on SQL Server
-- Extract part-> Year, month, week, day, hour, minute, second, quarter, dayofweek, dayofyear
-- Section 3. Arithmetic Functions
-- * DATEADD (datepart , number , date ) -> SQL Server Only
-- * DATEDIFF ( datepart , startdate , enddate ) -> SQL Server,
-- * DATEDIFF(end_date, start_date) -> MySQL returns in days only
-- * DATE_TRUNC(unit, date)
-- Section 4: Date and Time Conversion Functions
-- * CAST (string AS DATE) * CAST(date AS string) * TO_DATE(string, format) * TO_CHAR(value, format)
-- SELECT TO_DATE('3/4/2024', 'dd/mm/yyyy') -> converts string to date
-- SELECT TO_CHAR('2024-04-05'::DATE, 'mon dd, yyyy') -> converts date to string
-- How to Extract Date from Datetime in SQL
-- * CAST('2025-01-21 10:20:30' AS DATE) * CONVERT(DATE, '2025-01-21 10:20:30') * DATE('2025-01-21 10:20:30')

-- Questions
-- https://leetcode.com/problems/rising-temperature/
SELECT
    id AS Id
FROM
    (
        SELECT
            id,
            recorddate,
            temperature,
            (
                temperature > lag(temperature) over(
                    ORDER BY
                        recordDate
                )
            )
            AND datediff(
                recorddate,
                lag(recorddate) over(
                    ORDER BY
                        recorddate
                )
            ) = 1 bool
        FROM
            weather
    ) d
WHERE
    bool = 1;

-- https://platform.stratascratch.com/coding/2131-user-streaks?code_type=1
WITH ct1 AS (
    SELECT
        DISTINCT user_id,
        date_visited
    FROM
        user_streaks
),
ct2 AS (
    SELECT
        user_id,
        date_visited,
        row_number() oveR(
            PARTITION by user_id
            ORDER BY
                date_visited
        ) AS row_num,
        date_visited - row_number() over(
            PARTITION by user_id
            ORDER BY
                date_visited
        ) * INTERVAL '1 DAY' AS user_group
    FROM
        ct1
    WHERE
        1 = 1
        AND date_visited <= '2022-08-10'
),
ct3 AS (
    SELECT
        -- *
        user_id,
        user_group,
        count(*) AS streak_length,
        rank() over(
            ORDER BY
                count(*) DESC
        ) AS ranked
    FROM
        ct2
    GROUP BY
        user_id,
        user_group
) -- ,ct4 as (
SELECT
    -- *
    user_id,
    streak_length
FROM
    ct3
WHERE
    ranked <= 3;

----- Alternative
WITH unique_site_visits AS (
    SELECT
        DISTINCT *
    FROM
        user_streaks
    WHERE
        date_visited <= '2022-08-10' -- and user_id = 'u003'
),
detected_streaks AS (
    SELECT
        *,
        CASE
            WHEN datediff(
                date_visited,
                lag(date_visited) over(
                    PARTITION by user_id
                    ORDER BY
                        date_visited
                )
            ) = 1 THEN 0
            ELSE 1
        END AS streak_marker
    FROM
        unique_site_visits
),
streak_ids AS (
    SELECT
        *,
        sum(streak_marker) over(
            PARTITION by user_id
            ORDER BY
                date_visited
        ) AS streak_id
    FROM
        detected_streaks
),
streak_count AS (
    SELECT
        user_id,
        streak_id,
        count(*) AS streak_length,
        rank() over(
            ORDER BY
                count(*) DESC
        ) AS ranked
    FROM
        streak_ids
    GROUP BY
        user_id,
        streak_id
),
final_ct AS (
    SELECT
        user_id,
        streak_length
    FROM
        streak_count
    WHERE
        ranked <= 3
)
SELECT
    *
FROM
    final_ct -- order by ranked
;

-- https://platform.stratascratch.com/coding/2054-consecutive-days?code_type=1
WITH ct1 AS (
    SELECT
        record_date,
        user_id,
        record_date - row_number() over(
            ORDER BY
                record_date
        ) * INTERVAL '1 DAY' AS start_date
    FROM
        sf_events
)
SELECT
    user_id
FROM
    ct1
GROUP BY
    user_id,
    start_date
HAVING
    count(*) >= 3;

https: / / datalemur.com / questions / yoy - growth - rate WITH ct1 AS (
    SELECT
        extract(
            year
            FROM
                transaction_date
        ) AS year,
        product_id,
        sum(spend) AS curr_year_spend
    FROM
        user_transactions -- where product_id = '123424'
    GROUP BY
        product_id,
        year -- limit 5
),
ct2 AS (
    SELECT
        year,
        product_id,
        curr_year_spend,
        lag(curr_year_spend) over(
            PARTITION by product_id
            ORDER BY
                year
        ) AS prev_year_spend
    FROM
        ct1
)
SELECT
    year,
    product_id,
    curr_year_spend,
    prev_year_spend,
    round(
        (curr_year_spend - prev_year_spend) / prev_year_spend * 100,
        2
    ) AS yoy_rate
FROM
    ct2
ORDER BY
    2,
    1;

------------------------- DAY 7: Mock Test (Solve 5 Mixed Difficulty Problems) 
-- Questions
-- https://leetcode.com/problems/
WITH ct1 AS (
    SELECT
        d.name AS Department,
        e.name AS Employee,
        e.salary AS Salary,
        rank() over(
            PARTITION by d.id
            ORDER BY
                e.salary DESC
        ) AS ranked
    FROM
        employee e
        JOIN department d ON e.departmentId = d.id
)
SELECT
    Department,
    Employee,
    Salary
FROM
    ct1
WHERE
    ranked = 1;

-- https://platform.stratascratch.com/coding/9744-artist-of-the-decade?code_type=1
WITH ct1 AS (
    SELECT
        max(year) AS max_year
    FROM
        billboard_top_100_year_end
)
SELECT
    artist,
    count(*) AS counts
FROM
    billboard_top_100_year_end
    JOIN ct1 ON 1 = 1
WHERE
    max_year - year <= 20
GROUP BY
    artist
ORDER BY
    counts DESC;

-- https://www.hackerrank.com/challenges/challenges/problem
WITH max_challenges AS (
    SELECT
        max(counts) AS max_count
    FROM
        (
            SELECT
                count(*) AS counts
            FROM
                challenges
            GROUP BY
                hacker_id
        ) a
),
per_hacker AS (
    SELECT
        hacker_id,
        count(*) AS counts
    FROM
        challenges
    GROUP BY
        1
),
other_distinct_hackers AS (
    SELECT
        counts
    FROM
        per_hacker
        JOIN max_challenges ON 1 = 1
    WHERE
        counts <> max_count
    GROUP BY
        counts
    HAVING
        count(*) = 1
),
final_ct AS (
    SELECT
        per_hacker.hacker_id,
        per_hacker.counts
    FROM
        per_hacker
        JOIN max_challenges ON 1 = 1
        JOIN other_distinct_hackers ON other_distinct_hackers.counts = per_hacker.counts
    WHERE
        per_hacker.counts <> max_challenges.max_count
        AND per_hacker.counts = other_distinct_hackers.counts
    UNION
    SELECT
        hacker_id,
        counts
    FROM
        per_hacker
        JOIN max_challenges ON 1 = 1
    WHERE
        counts = max_count
)
SELECT
    h.hacker_id,
    h.name,
    counts
FROM
    final_ct f
    JOIN hackers h ON f.hacker_id = h.hacker_id
ORDER BY
    3 DESC,
    1;

