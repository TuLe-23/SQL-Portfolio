-- Tech Layoffs - Data Cleaning Project
-- https://www.kaggle.com/datasets/swaptr/layoffs-2022

SELECT * 
FROM world_layoffs.layoffs
;


-- Create a staging table

CREATE TABLE world_layoffs.layoffs_staging 
LIKE world_layoffs.layoffs
;

INSERT layoffs_staging 
SELECT * FROM world_layoffs.layoffs
;


-- now when we are data cleaning we usually follow a few steps
-- 1. check for duplicates and remove any
-- 2. standardize data and fix errors
-- 3. Look at null values and see what 
-- 4. remove any columns and rows that are not necessary - few ways


-- 1. Remove Duplicates

SELECT *
FROM world_layoffs.layoffs_staging
;

SELECT 
	company, 
	industry, 
	total_laid_off,
	`date`,
	ROW_NUMBER() 
 	OVER(PARTITION BY 
	company, 
	industry, 
	total_laid_off,
        `date`
        ) AS row_num
FROM 	world_layoffs.layoffs_staging
;

WITH duplicate_cte AS
	(SELECT *,
	ROW_NUMBER() OVER(PARTITION BY
		company, 
		industry, 
		total_laid_off, 
		percentage_laid_off,
		`date`
		) 
		AS row_num
	FROM world_layoffs.layoffs_staging
	)
SELECT *
FROM duplicate_cte
WHERE row_num > 1
;
    
-- Checking 'Oda' company to confirm

SELECT *
FROM world_layoffs.layoffs_staging
WHERE company = 'Oda'
;

-- Concluded that these are all legitimate entries and shouldn't be deleted. Need to really look at every single row to be accurate.
-- Must recreate CTE table to include ALL columns.

WITH duplicate_cte AS
	(SELECT *,
	ROW_NUMBER() OVER(PARTITION BY
		company,
		location,
		industry, 
		total_laid_off, 
		percentage_laid_off,
		`date`,
		stage,
		country,
		funds_raised_millions
		) 
		AS row_num
	FROM world_layoffs.layoffs_staging
	)
SELECT *
FROM duplicate_cte
WHERE row_num > 1
;

-- If row_num is > 1, that indicates there are duplicates.
-- These are the ones that need to be deleted.

-- Working to remove duplicates...

WITH duplicate_cte AS
	(SELECT *,
	ROW_NUMBER() OVER(PARTITION BY
		company,
		location,
		industry, 
		total_laid_off, 
		percentage_laid_off,
		`date`,
		stage,
		country,
		funds_raised_millions
		) 
		AS row_num
	FROM world_layoffs.layoffs_staging
	)
DELETE
FROM duplicate_cte
WHERE row_num > 1
;

-- An issue was encountered due to an updating problem with CTE.
-- A new staging table must be created to remove duplicates.

CREATE TABLE `layoffs_staging2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `row_num` INT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

SELECT *
FROM world_layoffs.layoffs_staging2
;

-- Inserting new relevant column (row_num) for duplicates.

INSERT INTO layoffs_staging2
SELECT *,
ROW_NUMBER() OVER(PARTITION BY
	company,
	location,
	industry, 
	total_laid_off, 
	percentage_laid_off,
	`date`,
	stage,
	country,
	funds_raised_millions
	)
	AS row_num
FROM world_layoffs.layoffs_staging
;

-- Reconfirming duplicates in the new table.

SELECT *
FROM world_layoffs.layoffs_staging2
WHERE row_num > 1
;

-- Duplicates confirmed. Now deleting duplicates.

DELETE
FROM world_layoffs.layoffs_staging2
WHERE row_num > 1
;



-- 2. Standardize Data


-- After investigating data, blank whitespaces are discovered in company names.

SELECT * 
FROM world_layoffs.layoffs_staging2
;

SELECT company
FROM world_layoffs.layoffs_staging2
;

-- Trimming evident whitespaces in company names and updating the table.

SELECT
	company,
	TRIM(company)
FROM world_layoffs.layoffs_staging2
;

UPDATE world_layoffs.layoffs_staging2
SET company = TRIM(company)
;

-- Multiple names for Crpto/Cryptocurrency discovered in 'industry'.

SELECT DISTINCT industry
FROM world_layoffs.layoffs_staging2
ORDER BY industry
;

SELECT *
FROM world_layoffs.layoffs_staging2
WHERE industry LIKE 'Crypto%'
;

-- Updating multiple Crypto/cryptocurrency names to just 'Crypto'.

UPDATE world_layoffs.layoffs_staging2
SET industry = 'Crypto'
WHERE industry LIKE 'Crypto%'
;

-- Confirming names are updated to labeled as just "Crypto".

SELECT DISTINCT industry
FROM world_layoffs.layoffs_staging2
ORDER BY 1
;




-- --------------------------------------------------- completion line




-- we also need to look at 

SELECT *
FROM world_layoffs.layoffs_staging2;

-- everything looks good except apparently we have some "United States" and some "United States." with a period at the end. Let's standardize this.
SELECT DISTINCT country
FROM world_layoffs.layoffs_staging2
ORDER BY country;

UPDATE layoffs_staging2
SET country = TRIM(TRAILING '.' FROM country);

-- now if we run this again it is fixed
SELECT DISTINCT country
FROM world_layoffs.layoffs_staging2
ORDER BY country;


-- Let's also fix the date columns:
SELECT *
FROM world_layoffs.layoffs_staging2;

-- we can use str to date to update this field
UPDATE layoffs_staging2
SET `date` = STR_TO_DATE(`date`, '%m/%d/%Y');

-- now we can convert the data type properly
ALTER TABLE layoffs_staging2
MODIFY COLUMN `date` DATE;


SELECT *
FROM world_layoffs.layoffs_staging2;





-- 3. Look at Null Values

-- the null values in total_laid_off, percentage_laid_off, and funds_raised_millions all look normal. I don't think I want to change that
-- I like having them null because it makes it easier for calculations during the EDA phase

-- so there isn't anything I want to change with the null values




-- 4. remove any columns and rows we need to

SELECT *
FROM world_layoffs.layoffs_staging2
WHERE total_laid_off IS NULL;


SELECT *
FROM world_layoffs.layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

-- Delete Useless data we can't really use
DELETE FROM world_layoffs.layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

SELECT * 
FROM world_layoffs.layoffs_staging2;

ALTER TABLE layoffs_staging2
DROP COLUMN row_num;


SELECT * 
FROM world_layoffs.layoffs_staging2;
