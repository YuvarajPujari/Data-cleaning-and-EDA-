SELECT count(*)  FROM layoffs order by company;
-- 1 remove duplicates
-- 2 standardize the data
-- 3 remove null values or blank values
-- 4   remove any coloumns

-- CREATE TABLE layoff_staging
-- LIKE layoffs;
-- INSERT  layoff_staging
-- SELECT * FROM layoffs;

CREATE TABLE layoff_staging2
LIKE layoffs;
-- INSERT  layoff_staging2
-- SELECT * FROM layoffs;
SELECT *  FROM layoff_staging2 order by company;
WITH duplicate_cte AS(
SELECT * ,ROW_NUMBER() OVER(PARTITION BY company,location,industry,total_laid_off,percentage_laid_off,`date`,stage,country,funds_raised_millions) AS row_num
  FROM layoff_staging2

)
SELECT count(*) FROM  duplicate_cte
 where row_num>1;
 
 
 CREATE TABLE `layoff_staging4` (
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
 insert into  layoff_staging4 select * ,ROW_NUMBER() OVER(PARTITION BY company,location,industry,total_laid_off,percentage_laid_off,`date`,stage,country,funds_raised_millions) AS row_num
   FROM layoff_staging2;
 select count(*) from layoff_staging4;
 
 delete from layoff_staging4
where row_num>1;

--  standardizing the data
update layoff_staging4 
set company=trim(company);

select company,trim(company) from layoff_staging4; 
update layoff_staging4 set industry="Crypto"
where  industry like "Crypto%";
update layoff_staging4 set country = trim(trailing '.' from country )
where country like 'United States%';
update layoff_staging4
 set `date`= str_to_date (`date`,'%m/%d/%Y');
 alter table layoff_staging4
modify column `date` DATE;

-- working like null type values 
select count(*) from layoff_staging4;
select distinct(industry) from layoff_staging4 order by 1;

select * from layoff_staging4 where industry is null or industry ='';
update layoff_staging4
 set industry=null
 where industry='';
select * from layoff_staging4 t1
join layoff_staging4 t2
 on t1.company=t2.company
 where(t1.industry is null )
 and t2.industry is not null;
 
 update layoff_staging4 t1
join layoff_staging4 t2
 on t1.company=t2.company
 set t1.industry=t2.industry
 where t1.industry is null 
 and t2.industry is not null;
 
 select *
 from layoff_staging4 
 where total_laid_off is null
 and percentage_laid_off is null;
 
 delete  from layoff_staging4 
 where total_laid_off is null
 and percentage_laid_off is null;
 
 select *from layoff_staging4;
 
 alter table layoff_staging4
 drop column row_num;
 
 -- EXPLORATORY DATA ANYLYTICS
 select * from 
 layoff_staging4
 where percentage_laid_off=1
 order by funds_raised_millions DESC;
 
 select company,sum(total_laid_off)
 from layoff_staging4
 group by company 
 order by 2 DESC;
 
 select min(`date`),max(`date`)
 from layoff_staging4;
 
 
select industry,company,sum(total_laid_off)
 from layoff_staging4
 group by company ,industry
 order by 3 DESC;
 
 select country,sum(total_laid_off)
 from layoff_staging4
 group by country
 order by 2 DESC;
 
 select year(`date`),sum(total_laid_off)
 from layoff_staging4
 group by year(`date`)
 order by 1 DESC;
 
 select stage ,sum(total_laid_off)
 from layoff_staging4
 group by stage
 order by 2 DESC;
 
 select substring(`date`,1,7) from layoff_staging4;
 
 select substring(`date`,1,7) as `month`,sum(total_laid_off)  
 from layoff_staging4
 where substring(`date`,1,7) is not null
 group by `month`
 order by 1 ASC;
 
 with rolling_total as(
 
  select substring(`date`,1,7) as `month`,sum(total_laid_off) as total_off  
 from layoff_staging4
 where substring(`date`,1,7) is not null
 group by `month`
 order by 1 ASC
 
 )
 select `month`,total_off,sum(total_off) over(order by `month`) as rolling_total
   from rolling_total; 
 
 select company,year(`date`),sum(total_laid_off)
 from layoff_staging4
 group by company,year(`date`)
 order by 3 DESC;
 
  with Company_year(company,years,total_laid_off)as
 (
  select company,year(`date`),sum(total_laid_off)
 from layoff_staging4
 group by company,year(`date`)
 order by 3 DESC
 )
 select * ,dense_rank() over(partition by years order by total_laid_off DESC   ) as ranking from  Company_year
   where years is not null 
   order by ranking ASC;

 
 
 
 with Company_year(company,years,total_laid_off)as
 (
  select company,year(`date`),sum(total_laid_off)
 from layoff_staging4
 group by company,year(`date`)
 order by 3 DESC
 ),
   Company_Ranking as(
 select * ,dense_rank() over(partition by years order by total_laid_off DESC   ) as ranking from  Company_year
   where years is not null 
   order by ranking ASC
   
   )
   select * from Company_Ranking
   where ranking<=5;
 
 
 
 
 
 
 
 
 
 
 
 
 
 
 
 
 
 
 
 
 
 







 
 
