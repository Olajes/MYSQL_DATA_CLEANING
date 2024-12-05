SELECT *
FROM layoffs;
/*

-- 1. Remove Duplicates If any
-- 2. Standardize the Data
-- 3. Null values or blank Values
-- 4. Remove Any Columns

*/

/* 
CREATE TABLE new_layoffs AS 
SELECT * FROM layoffs;

SELECT * 
FROM new_layoffs 
LIMIT 10;

*/
-- FIRST CREATED A COPY OF ORIGINAL TABLE
CREATE TABLE layoffs_staging 
LIKE layoffs;

SELECT *
FROM layoffs_staging;

INSERT layoffs_staging
SELECT * 
FROM layoffs;



-- 1. Remove Duplicates If any



WITH duplicate_cte AS
(
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company,location,industry,total_laid_off,percentage_laid_off,`date`, stage,country,funds_raised_millions) AS row_num
FROM layoffs_staging
)
SELECT * 
FROM duplicate_cte
WHERE row_num > 1;


CREATE TABLE layoffs_staging2 (
company text, 
location text, 
industry text, 
total_laid_off int,
percentage_laid_off text,
`date` text,
stage text,
country text, 
funds_raised_millions int,
row_num int
);

SELECT *
FROM layoffs_staging2;

INSERT INTO layoffs_staging2
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company,location,
industry,total_laid_off,percentage_laid_off,`date`, stage,
country,funds_raised_millions) AS row_num
FROM layoffs_staging;


DELETE
FROM layoffs_staging2
WHERE row_num > 1;

SELECT *
FROM layoffs_staging2;

-- STANDARDIZING DATA


SELECT company,TRIM(company)
FROM layoffs_staging2;

UPDATE layoffs_staging2
SET company=TRIM(company);


-- STANDARDIZING INDUSTRY CLOUMN/ATTRIBUTE/FIELD
SELECT industry
FROM layoffs_staging2;


SELECT *
FROM layoffs_staging2
WHERE industry LIKE "Crypto%";

UPDATE layoffs_staging2
SET industry= "Crypto"
WHERE industry LIKE "Crypto%";


-- STANDARDIZING COUNTRY CLOUMN/ATTRIBUTE/FIELD
SELECT *
FROM layoffs_staging2
WHERE country LIKE "United States%";

UPDATE layoffs_staging2
SET country= "United States"
WHERE country LIKE "United States%";

SELECT DISTINCT country
FROM layoffs_staging2
WHERE country LIKE "United States";


-- CONVERTING DATE FROM TEXT DTYPE TO DATE DTYPE
SELECT `date`
FROM layoffs_staging2;

SELECT `date`, str_to_date(`date`,"%m/%d/%Y")
FROM layoffs_staging2;

UPDATE layoffs_staging2
SET `date`= str_to_date(`date`,"%m/%d/%Y");

SELECT `date`
FROM layoffs_staging2;

ALTER TABLE layoffs_staging2
MODIFY column `date` DATE;

SELECT *
FROM layoffs_staging2;

-- WORKING WITH NULL AND BLANK VALUES

SELECT *
FROM layoffs_staging2
WHERE total_laid_off IS NULL;

SELECT total_laid_off,percentage_laid_off
FROM layoffs_staging2
WHERE total_laid_off IS NULL 
	AND percentage_laid_off IS NULL;


UPDATE layoffs_staging2
SET industry = Null
WHERE industry = "";

SELECT *
FROM layoffs_staging2
WHERE industry IS NULL
	OR industry = '';

SELECT *
FROM layoffs_staging2
WHERE company LIKE "Bally%";

SELECT 
	t1.company,t1.location,t1.industry,
	t2.company,t2.location,t2.industry
FROM layoffs_staging2 t1
JOIN layoffs_staging2 t2
	ON t1.company=t2.company
    AND t1.location=t2.location
WHERE (t1.industry IS NULL OR t1.industry='')
	AND t2.industry IS NOT NULL;

UPDATE layoffs_staging2 t1
JOIN layoffs_staging2 t2
	ON t1.company=t2.company
SET t1.industry = t2.industry
WHERE t1.industry IS NULL 
	AND t2.industry IS NOT NULL;
    
SELECT *
FROM layoffs_staging2;    

SELECT *
FROM layoffs_staging2
WHERE total_laid_off IS NULL 
	AND percentage_laid_off IS NULL;


DELETE
FROM layoffs_staging2
WHERE total_laid_off IS NULL 
	AND percentage_laid_off IS NULL;

SELECT *
FROM layoffs_staging2;

ALTER TABLE layoffs_staging2
DROP COLUMN row_num;

SELECT *
FROM layoffs_staging2;


-- -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
-- --------------------------------------------------------------------------------------EXPLORATORY DATA ANALYSIS -----------------------------------------------------------------------------------------------------------------------------------

SELECT MAX(total_laid_off),MAX(percentage_laid_off)
FROM layoffs_staging2;

SELECT *
FROM layoffs_staging2
WHERE percentage_laid_off= 1
ORDER BY funds_raised_millions DESC;

SELECT company, SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY company
ORDER BY SUM(total_laid_off) DESC;

SELECT MIN(`date`), MAX(`date`)
FROM layoffs_staging2;

SELECT industry, SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY industry
ORDER BY SUM(total_laid_off) DESC;

SELECT MIN(`date`), MAX(`date`)
FROM layoffs_staging2;

SELECT *
FROM layoffs_staging2;

SELECT YEAR(`date`), SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY YEAR(`date`)
ORDER BY  YEAR(`date`) DESC;

SELECT stage, SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY stage
ORDER BY 2 DESC;

SELECT company, AVG(percentage_laid_off)
FROM layoffs_staging2
GROUP BY company
ORDER BY 2 DESC;

SELECT SUBSTRING(`date`,1,7) AS `MONTH`,SUM(total_laid_off)
FROM layoffs_staging2
WHERE SUBSTRING(`date`,1,7) IS NOT NULL
GROUP BY `MONTH`
ORDER BY `MONTH`;
    
WITH Rolling_Total AS
(
SELECT SUBSTRING(`date`,1,7) AS `MONTH`,SUM(total_laid_off) total_off
FROM layoffs_staging2
WHERE SUBSTRING(`date`,1,7) IS NOT NULL
GROUP BY `MONTH`
ORDER BY `MONTH`
 )   
 SELECT `MONTH`, total_off,SUM(total_off) OVER (ORDER BY `MONTH`
 ) AS rolling_total
 FROM Rolling_Total;
    
    
SELECT company,YEAR(`date`) ,SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY company,YEAR(`date`)
ORDER BY company ASC;    
    
SELECT company,YEAR(`date`) ,SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY company,YEAR(`date`)
ORDER BY SUM(total_laid_off) DESC;     


WITH company_Year(company,years,total_laid_off) AS
(
SELECT company,YEAR(`date`),SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY company,YEAR(`date`)
)
SELECT *, DENSE_RANK() OVER (PARTITION BY years ORDER BY total_laid_off DESC) AS Ranking
FROM company_Year
WHERE years IS NOT NULL
ORDER BY Ranking ASC;
    


WITH company_Year(company,years,total_laid_off) AS
(
SELECT company,YEAR(`date`),SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY company,YEAR(`date`)
),company_Year_Rank AS 
(SELECT *, DENSE_RANK() OVER (PARTITION BY years ORDER BY total_laid_off DESC) AS Ranking
FROM company_Year
WHERE years IS NOT NULL
)
SELECT *
FROM Company_Year_Rank
WHERE Ranking <= 5;  

SELECT *
FROM layoffs_staging2; 
    