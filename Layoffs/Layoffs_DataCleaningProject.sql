-- Tech Layoffs - Data Cleaning Project
-- https://www.kaggle.com/datasets/swaptr/layoffs-2022

-- Data Cleaning Guideline:
-- 1. Remover dulicates (if any)
-- 2. Standardize data and fix errors
-- 3. Handle NULL or blank values
-- 4. Remove any rows or columns that aren't necessary


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
-- Again, if row_num is > 1, that indicates there are duplicates.
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

-- Updating multiple Crypto/cryptocurrency names to just "Crypto".
UPDATE world_layoffs.layoffs_staging2
SET industry = 'Crypto'
WHERE industry LIKE 'Crypto%'
;

-- Confirming names are updated to just "Crypto".
SELECT DISTINCT industry
FROM world_layoffs.layoffs_staging2
ORDER BY 1
;


-- Duplicate/misspelling found in country column for "United States". 
-- Some "United States" and some "United States." with a period at the end.
SELECT DISTINCT country
FROM world_layoffs.layoffs_staging2
ORDER BY 1

SELECT *
FROM layoffs_staging2
WHERE country LIKE 'United States%'
;
	
SELECT DISTINCT
	country,
	TRIM(TRAILING '.' FROM country)
FROM world_layoffs.layoffs_staging2
ORDER BY 1
;

-- Updating duplicate/misspellings to just "United States"
UPDATE world_layoffs.layoffs_staging2
SET country = TRIM(TRAILING '.' FROM country)
WHERE country LIKE 'United States%'
;

-- Change confirmed.
SELECT DISTINCT location
FROM world_layoffs.layoffs_staging2
ORDER BY 1
;


-- Investigating further...
SELECT *
FROM world_layoffs.layoffs_staging2;


-- Problem in date column found.
-- Date is a text data type. Needs to be converted to the proper format and data type.
SELECT 
	`date`,
	str_to_date(`date`, '%m/%d/%Y')
FROM world_layoffs.layoffs_staging2
;

-- Updating table with the proper date format...
UPDATE world_layoffs.layoffs_staging2
SET `date` = str_to_date(`date`, '%m/%d/%Y')
;

-- Confirming update.
SELECT `date`
FROM world_layoffs.layoffs_staging2
;

-- Now updating data type from text to date.
ALTER TABLE world_layoffs.layoffs_staging2
MODIFY COLUMN `date` DATE
;


-- Discovered 'funds_raised millions" is a also a text data type.
-- funds_raised_millions column needs to converted into an integer data type.
SELECT funds_raised_millions
FROM layoffs_staging2
ORDER BY 1 DESC;

UPDATE layoffs_staging2
SET funds_raised_millions = NULL
WHERE funds_raised_millions = 'NULL';

-- Now updating data type from text to an integer.
ALTER TABLE layoffs_staging2
MODIFY COLUMN funds_raised_millions INT;

-- Confirming update.
SELECT funds_raised_millions
FROM world_layoffs.layoffs_staging2
;



-- 3. NULL/Blank Values


-- Inspecting nulls in total_laid_off and percentage_laid_off column.
SELECT *
FROM world_layoffs.layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

-- Inspecting nulls and blanks in industry column.
SELECT *
FROM world_layoffs.layoffs_staging2
WHERE industry IS NULL
OR industry = ''
;

-- Discovered row where 'industry' cell is blank for Airbnb. Looking for other Airbnb rows where 'industry' cell is filled in for reference.
SELECT *
FROM world_layoffs.layoffs_staging2
WHERE company = 'Airbnb'
;

-- Discovered other Airbnb rows where 'industry' cell is filled in.
-- Attempting to populate NULL/Blank Values for Airbnb and other companies with known data.
SELECT *
FROM world_layoffs.layoffs_staging2 AS t1
JOIN world_layoffs.layoffs_staging2 AS t2
	ON t1.company = t2.company
    AND t1.location = t2.location
WHERE (t1.industry IS NULL OR t1.industry = '')
AND t2.industry IS NOT NULL
;

UPDATE world_layoffs.layoffs_staging2 AS t1
JOIN world_layoffs.layoffs_staging2 AS t2
	ON t1.company = t2.company
SET t1.industry = t2.industry
WHERE (t1.industry IS NULL OR t1.industry = '')
AND t2.industry IS NOT NULL
;

-- Update had not effect.
-- Concluded that blanks to must be turned into NULL values, due to issues when updating with a WHERE clause that has an OR.
UPDATE world_layoffs.layoffs_staging2
SET industry = NULL
WHERE industry = ''
;

-- Attempting update again.
UPDATE world_layoffs.layoffs_staging2 AS t1
JOIN world_layoffs.layoffs_staging2 AS t2
	ON t1.company = t2.company
SET t1.industry = t2.industry
WHERE t1.industry IS NULL
AND t2.industry IS NOT NULL
;

-- Update confirmed.
SELECT *
FROM world_layoffs.layoffs_staging2
WHERE company = 'Airbnb'
;



-- 4. Removing rows and columns that aren't necessary.


-- Investigating if rows should be deleted or not because both total_laid_off and percentage_laid_off columns are NULL.
SELECT *
FROM world_layoffs.layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL
;

-- Decision to delete rows where total_laid_off and percentage_laid_off columns are NULL.
-- Rows have no value to us because relevant values are NULL.
DELETE
FROM world_layoffs.layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL
;

-- Row deletion confirmed!
SELECT *
FROM world_layoffs.layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL
;


-- row_num column created earlier is no longer needed and should be deleted.
ALTER TABLE world_layoffs.layoffs_staging2
DROP COLUMN row_num
;

-- row_numn column deletion confirmed!
SELECT *
FROM world_layoffs.layoffs_staging2
;
