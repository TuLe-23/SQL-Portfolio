-- Exploratory Data Analysis


SELECT * 
FROM world_layoffs.layoffs_staging2
;

-- Data date range
SELECT
  MIN(date),
  MAX(date)
FROM world_layoffs.layoffs_staging2
;

-- Most laid off.
SELECT *
FROM layoffs_staging2
ORDER BY total_laid_off DESC
;

-- Companies that went completely under.
-- Companies that had percentage_laid_off = 1 means 100 percent of the company was laid off.
SELECT *
FROM world_layoffs.layoffs_staging2
WHERE  percentage_laid_off = 1;

-- Companies that went completely under by funds raised.
SELECT *
FROM world_layoffs.layoffs_staging2
WHERE percentage_laid_off = 1
ORDER BY funds_raised_millions DESC
;

-- Total laid off by industry.
SELECT
  industry,
  SUM(total_laid_off)
FROM world_layoffs.layoffs_staging2
GROUP BY industry
ORDER BY 2 DESC
;

-- Companies with the most total layoffs.
SELECT
  company,
  SUM(total_laid_off)
FROM world_layoffs.layoffs_staging2
GROUP BY company
ORDER BY 2 DESC
;

-- Total layoffs by company stage.
SELECT
	stage,
  SUM(total_laid_off)
FROM world_layoffs.layoffs_staging2
GROUP BY stage
ORDER BY 2 DESC
;

-- Total laid off by country.
SELECT
	country,
  SUM(total_laid_off)
FROM world_layoffs.layoffs_staging2
GROUP BY country
ORDER BY 2 DESC

-- Total layoffs in the United States.
SELECT *
FROM layoffs_staging2
WHERE country LIKE 'United States'
ORDER BY total_laid_off DESC
;

-- Total layoffs by year.
SELECT
	YEAR(`date`),
  SUM(total_laid_off)
FROM world_layoffs.layoffs_staging2
GROUP BY YEAR(`date`)
ORDER BY 1 DESC
;

-- Total layoffs by year and month.
SELECT
	substring(`date`, 1, 7) AS `month`,
  SUM(total_laid_off)
FROM world_layoffs.layoffs_staging2
WHERE substring(`date`, 1, 7) IS NOT NULL
GROUP BY `month`
ORDER By 1 ASC
;

-- Rolling total of layoffs by month.
WITH rolling_total AS
	(SELECT
	  substring(`date`, 1, 7) AS `month`,
	  SUM(total_laid_off) AS total_off
	FROM world_layoffs.layoffs_staging2
	WHERE substring(`date`, 1, 7) IS NOT NULL
	GROUP BY `month`
	ORDER By 1 ASC
  )
SELECT
	`month`,
  total_off,
  SUM(total_off) OVER(ORDER BY `month`) AS Rolling_total
FROM rolling_total
;

-- Ranking of total laid off by year and company
WITH company_year (company, years, total_laid_off) AS
	(SELECT
		company,
		YEAR(`date`),
		SUM(total_laid_off)
	FROM world_layoffs.layoffs_staging2
	GROUP BY company, YEAR(`date`)
    ), 
    company_year_rank AS
	(SELECT *,
	DENSE_RANK() OVER(PARTITION BY years ORDER BY total_laid_off DESC) AS ranking
	FROM company_year
	WHERE years IS NOT NULL
	)
SELECT *
FROM company_year_rank
WHERE ranking <= 5
;

-- Ranking of total laid off by year and industry
WITH Industry_Year (industry, years, total_laid_off) AS
(
SELECT industry, YEAR(`date`), SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY industry, YEAR(`date`)
), Industry_Year_Ranked AS
(
SELECT *,
DENSE_RANK() OVER (PARTITION BY years ORDER BY total_laid_off DESC) AS Ranking
FROM Industry_Year
WHERE years IS NOT NULL
)
SELECT *
FROM Industry_Year_Ranked
WHERE Ranking <= 5
;
