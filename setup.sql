-- CREATE DATABASE IF NOT EXISTS wikilynx;

-- USE wikilynx;

/*
=========
= qn 1a =
=========
filtered for top 10 categories via JOIN to answer qn 2b later and to reduce row count
*/
DROP TABLE IF EXISTS tbl_1a;

CREATE TABLE tbl_1a AS
SELECT
	p.page_id,
    p.page_title,
    p.page_namespace,
    STR_TO_DATE(p.page_touched, '%Y%m%d') AS page_last_modified,
    c.cat_id AS category_id,
    cl.cl_to AS category_title
FROM page p
JOIN categorylinks cl
	ON p.page_id = cl.cl_from
JOIN (SELECT cat_id, cat_title FROM category ORDER BY cat_pages DESC LIMIT 10) c
	ON cl.cl_to = c.cat_title;



/*
=========
= qn 1b =
=========
limit 100k to reduce row count and save time
*/
DROP TABLE IF EXISTS tbl_1b;

CREATE TABLE tbl_1b AS
SELECT
	pl_from AS source_page_id,
	pl_from_namespace AS source_namespace,
	pl_title AS target_page_title,
	pl_namespace AS target_namespace,
	ROW_NUMBER() OVER(PARTITION BY pl_title ORDER BY pl_from) AS link_position
FROM pagelinks pl
LIMIT 100000;



/*
=========
= qn 2b =
=========
Steps:
1) For each page_id, get max days_outdated (= target_last_modified - source_last_modified).
	1.1) Join page_id in tbl_1a to tbl_1b to get source_last_modified
    1.2) Repeat 1.1) to get target_last_modified
    1.3) Keep page_id rows with highest days_outdated
    
SELECT DISTINCT where possible to limit row count.

2) For each category, get most outdated page
    2.1) Join result from step 1.2) to tbl_1a, keeping only top 10 categories
    2.2) Keep category_id rows with highest days_outdated

Alternative: Join everything first, then group by at the end. But will take a long time to execute.
*/

-- 1)
DROP TABLE IF EXISTS s1_page_od;

CREATE TABLE s1_page_od AS
WITH base AS (
	SELECT
		tb.source_page_id,
		ta1.page_title AS source_page_title,
		ta1.page_last_modified AS source_last_modified,
		ta2.page_id AS target_page_id,
		tb.target_page_title,
		ta2.page_last_modified AS target_last_modified,
		ta2.page_last_modified - ta1.page_last_modified AS days_outdated
	FROM (SELECT DISTINCT source_page_id, target_page_title FROM tbl_1b) tb
    -- 1.1)
	JOIN (SELECT DISTINCT page_id, page_title, page_last_modified FROM tbl_1a) ta1
		ON tb.source_page_id = ta1.page_id
	-- 1.2)
	JOIN (SELECT DISTINCT page_id, page_title, page_last_modified FROM tbl_1a) ta2
		ON tb.target_page_title = ta2.page_title
	WHERE
		ta2.page_last_modified > ta1.page_last_modified
)
-- 1.3)
SELECT
	a.source_page_id,
    a.source_page_title,
    a.days_outdated
FROM base a
-- self-join to replicate MAX_BY(page_id, outdatedness) behaviour in presto
-- alternative: use [SELECT category_id, MAX(outdatedness)...] for join
LEFT JOIN base b
	ON a.source_page_id = b.source_page_id
	AND (
		-- join is unsuccessful if a.outdatedness is the max value
		a.days_outdated < b.days_outdated
		OR (
			-- if >1 page with same max outdatedness, page with highest page_id will be unsuccessful
			a.days_outdated = b.days_outdated
			AND a.source_page_id < b.source_page_id
		)
	)
WHERE
	-- return only unsuccessful joins (max value)
	b.days_outdated IS NULL;

SELECT * FROM s1_page_od limit 10;

-- 2)


-- 2.1)
DROP TABLE IF EXISTS s2_top10;

CREATE TABLE s2_top10 AS 
SELECT
	ta.category_id,
	ta.category_title,
    c.cat_pages AS category_num_pages,
	ta.page_id,
	ta.page_title,
	s1.days_outdated
FROM tbl_1a ta
JOIN (SELECT cat_id, cat_pages FROM category ORDER BY cat_pages DESC LIMIT 10) c
	ON ta.category_id = c.cat_id
JOIN s1_page_od s1
	ON ta.page_id = s1.source_page_id;


-- 2.2)
DROP TABLE IF EXISTS tbl_2b;

CREATE TABLE tbl_2b AS 
SELECT
	a.category_id,
	a.category_title,
    a.category_num_pages,
    -- ensure unique ranking
    ROW_NUMBER() OVER(ORDER BY a.category_num_pages DESC) AS category_rank,
    a.page_id AS most_outdated_page_id,
	a.page_title AS most_outdated_page_title,
	a.days_outdated
FROM s2_top10 a
-- self-join to replicate MAX_BY(page_id, outdatedness) behaviour in presto
-- alternative: use [SELECT category_id, MAX(outdatedness)...] for join
LEFT JOIN s2_top10 b
	ON a.category_id = b.category_id
	AND (
		-- join is successful only if a.outdatedness is not the max value
		a.days_outdated < b.days_outdated
		OR (
			-- if >1 page with same max outdatedness, page with highest page_id will be unsuccessful
			a.days_outdated = b.days_outdated
			AND a.page_id < b.page_id
		)
	)
WHERE
	-- return only unsuccessful joins (max value)
    b.days_outdated IS NULL;