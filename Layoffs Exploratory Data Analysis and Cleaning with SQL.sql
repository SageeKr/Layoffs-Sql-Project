-- Step 1: Create a staging table for cleaning operations
CREATE TABLE layoffs_staging AS
SELECT * FROM layoffs;

-- Step 2: Check for duplicates
WITH duplicate_cte AS (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) AS row_num
    FROM layoffs_staging
)
SELECT *
FROM duplicate_cte
WHERE row_num > 1;

-- Step 3: Create a new staging table to hold cleaned data
CREATE TABLE layoffs_staging2 (
  company TEXT,
  location TEXT,
  industry TEXT,
  total_laid_off INT DEFAULT NULL,
  percentage_laid_off TEXT,
  `date` TEXT,
  stage TEXT,
  country TEXT,
  funds_raised_millions INT DEFAULT NULL,
  row_num INT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- Step 4: Insert data into the new staging table with row numbers for duplicate checking
INSERT INTO layoffs_staging2
SELECT *,
       ROW_NUMBER() OVER (PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) AS row_num
FROM layoffs_staging;

-- Step 5: Remove duplicates
DELETE 
FROM layoffs_staging2
WHERE row_num > 1;

-- Step 6: Standardize data - Trim whitespace from company and industry names
UPDATE layoffs_staging2
SET company = TRIM(company),
    industry = TRIM(industry);

-- Step 7: Standardize industry names
UPDATE layoffs_staging2
SET industry = 'Crypto'
WHERE industry LIKE 'Crypto%';

-- Step 8: Standardize country names
UPDATE layoffs_staging2
SET country = 'United States'
WHERE country LIKE 'United States%';

-- Step 9: Convert date format from text to DATE type
UPDATE layoffs_staging2
SET `date` = STR_TO_DATE(`date`, '%m/%d/%Y');

ALTER TABLE layoffs_staging2
MODIFY COLUMN `date` DATE;

-- Step 10: Handle NULL or empty industry values
UPDATE layoffs_staging2
SET industry = NULL
WHERE TRIM(industry) = '';

-- Step 11: Fill NULL industry values based on the same company's non-null industry values
UPDATE layoffs_staging2 t1
JOIN layoffs_staging2 t2
    ON t1.company = t2.company
SET t1.industry = t2.industry
WHERE t1.industry IS NULL
  AND t2.industry IS NOT NULL;

-- Step 12: Drop the row_num column as it's no longer needed
ALTER TABLE layoffs_staging2
DROP COLUMN row_num;

-- Verify the cleaned data
SELECT * FROM layoffs_staging2;


-- SQL Project - 
-- After the data cleaning is done it is time for the, Exploratory Data Analysis

-- View the entire dataset
SELECT *
FROM layoffs_staging2;

-- Select records where 100% of the workforce was laid off, ordered by funds raised in descending order
SELECT *
FROM layoffs_staging2
WHERE percentage_laid_off = 1
ORDER BY funds_raised_millions DESC;

-- Total number of layoffs by company, ordered by the highest total layoffs
SELECT company, SUM(total_laid_off) AS total_laid_off
FROM layoffs_staging2
GROUP BY company
ORDER BY total_laid_off DESC;

-- Total number of layoffs by industry, ordered by the highest total layoffs
SELECT industry, SUM(total_laid_off) AS total_laid_off
FROM layoffs_staging2
GROUP BY industry
ORDER BY total_laid_off DESC;

-- Find the range of dates in the dataset
SELECT MIN(`date`) AS min_date, MAX(`date`) AS max_date
FROM layoffs_staging2;

-- Total number of layoffs per year, ordered by year in descending order
SELECT YEAR(`date`) AS year, SUM(total_laid_off) AS total_laid_off
FROM layoffs_staging2
GROUP BY year
ORDER BY year DESC;

-- Total number of layoffs per month, ordered by month in ascending order
SELECT DATE_FORMAT(`date`, '%Y-%m') AS `month`, SUM(total_laid_off) AS total_laid_off
FROM layoffs_staging2
GROUP BY `month`
ORDER BY `month` ASC;

-- Calculate rolling total of layoffs by month
WITH Rolling_Total AS (
    SELECT DATE_FORMAT(`date`, '%Y-%m') AS `month`, SUM(total_laid_off) AS total_off
    FROM layoffs_staging2
    GROUP BY `month`
    ORDER BY `month` ASC
)
SELECT `month`, total_off, SUM(total_off) OVER (ORDER BY `month`) AS Rolling_Total
FROM Rolling_Total;

-- Layoffs by company and year, ordered by the highest total layoffs
WITH Company_Year AS (
    SELECT company, YEAR(`date`) AS year, SUM(total_laid_off) AS total_laid_off
    FROM layoffs_staging2
    GROUP BY company, year
    ORDER BY total_laid_off DESC
)
SELECT *
FROM Company_Year;














