SELECT *  FROM layoffs;
-- 1 remove duplicates
-- 2 standardize the data
-- 3 remove null values or blank values
-- 4   remove any coloumns


-- 1 DELETING THE DUPLICATES
CREATE TABLE layoff_staging
LIKE layoffs;
INSERT  layoff_staging
SELECT * FROM layoffs;

SELECT * FROM layoff_staging;


  #CANNOT UPDATE DATA IN CTE
WITH duplicate_cte AS(
SELECT * ,ROW_NUMBER() OVER(PARTITION BY company,location,industry,total_laid_off,percentage_laid_off,`date`,stage,country,funds_raised_millions) AS row_num
  FROM layoff_staging

)
SELECT * FROM  duplicate_cte
where row_num>1;

SELECT * FROM layoff_staging
where company="2U";

CREATE TABLE `layoff_staging2` (
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

select * FROM layoff_staging2;

-- insert into  layoff_staging2 select * ,ROW_NUMBER() OVER(PARTITION BY company,location,industry,total_laid_off,percentage_laid_off,`date`,stage,country,funds_raised_millions) AS row_num
--   FROM layoff_staging;
  
select * from layoff_staging2
where row_num>1;
delete from layoff_staging2
where row_num>1;

select count(*) from  layoff_staging2;

-- Standardizing the data
select * from layoff_staging2;
select trim(company) from   layoff_staging2;
update layoff_staging2 
set company=trim(company);

select distinct(industry)
from layoff_staging2 order by 1;

select * from layoff_staging2
where industry like 'Crypto%';

update layoff_staging2 set industry="Crypto"
where  industry like "Crypto%";

select  distinct( country)
from  layoff_staging2 order by 1 ;

select  distinct( country),trim(trailing '.' from country )
from  layoff_staging2 order by 1 ;

update layoff_staging2 set country = trim(trailing '.' from country )
where country like 'United States%';

select `date`, str_to_date (`date`,'%m/%d/%Y')
from layoff_staging2;

update layoff_staging2
 set `date`= str_to_date (`date`,'%m/%d/%Y');

select `date` from layoff_staging2;

alter table layoff_staging2
modify column `date` DATE;

-- DUPLICATES WHERE NOT DELETED DELETING ONCE AGAIN BY USING THE ABOVE APPROACH
select * from layoff_staging2 order by company;
select * from layoff_staging2 where company = "Airbnb" ;
select * from layoff_staging2 where industry='' or  industry is null order by 1;

WITH duplicate_cte AS(
SELECT * ,ROW_NUMBER() OVER(PARTITION BY company,location,industry,total_laid_off,percentage_laid_off,`date`,stage,country,funds_raised_millions) as row_S
  FROM layoff_staging2

)
SELECT * FROM  duplicate_cte
where row_S>1;


-- CREATE TABLE `layoff_staging3` (
--   `company` text,
--   `location` text,
--   `industry` text,
--   `total_laid_off` int DEFAULT NULL,
--   `percentage_laid_off` text,
--   `date` text,
--   `stage` text,
--   `country` text,
--   `funds_raised_millions` int DEFAULT NULL,
--   `row_num` INT,
--    `row_S` INT
-- ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

--  insert into  layoff_staging3 select * ,row_number() over(PARTITION BY company,location,industry,total_laid_off,percentage_laid_off,`date`,stage,country,funds_raised_millions) as row_S
--   from layoff_staging2;

delete from layoff_staging3
where row_S>1;

select * from layoff_staging3;
select industry from layoff_staging3 where industry like 'Cryp%';

alter table layoff_staging3
modify column `date` DATE;
select  distinct( country),trim(trailing '.' from country )
from  layoff_staging3 order by 1 ;

select * from layoff_staging3 order by company;
WITH duplicate_cte AS(
SELECT * ,ROW_NUMBER() OVER(PARTITION BY company,location,industry,total_laid_off,percentage_laid_off,`date`,stage,country,funds_raised_millions) AS row_nums
  FROM layoff_staging3

)
SELECT * FROM  duplicate_cte
where row_nums>1 order by company ;







  
  

























  
  








