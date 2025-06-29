-- SQL Project - Data Cleaning

-- https://www.kaggle.com/datasets/swaptr/layoffs-2022 



-- Create another table to work on this table instead of raw data

create table layoffs_staging
like layoffs;

select * from layoffs_staging;

insert layoffs_staging
select*from layoffs;

select * from layoffs_staging;

-- Remove duplicates

select *,
row_number() over(partition by company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) as row_num 
from layoffs_staging
having row_num>1;

-- CREATE CTE TO COUNT DUPLICATES THROUGH ROW_NUMBER
with duplicate_check as
(select *,
row_number() over(partition by company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) as row_num 
from layoffs_staging)

select*from duplicate_check
where row_num>1;

-- CREATE ANOTHER TABLE TO UPDATE OR DELETE 

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
  `row_num` int
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- inserting data with row number as well
select*from layoffs_staging2;
insert into layoffs_staging2
select *,
row_number() over(partition by company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) as row_num 
from layoffs_staging;

select * from layoffs_staging2
where row_num>1;

-- deleting column with same values
delete 
from layoffs_staging2
where row_num>1;

select * from layoffs_staging2
where row_num>1;

select * from layoffs_staging2;

-- standarized data
-- removing white space
select company, trim(company)
from layoffs_staging2;

-- updating with values without white space
update layoffs_staging2
set company= trim(company);

select distinct company from layoffs_staging2;

select distinct industry from layoffs_staging2;

select distinct industry
from layoffs_staging2
where industry like 'Crypto%';

-- updating values with same type of thing
update layoffs_staging2
set industry= 'Crypto'
where industry like 'Crypto%';

select distinct country 
from layoffs_staging2; 

select distinct country, trim(trailing '.' from country)
from layoffs_staging2;

-- updating without mark in the same name to find unique value
update layoffs_staging2
set country= trim(trailing '.' from country); 

select distinct country
from layoffs_staging2; 

select `date`, 
str_to_date(`date`, '%m/%d/%Y')
from layoffs_staging2 
;

-- updating date
update layoffs_staging2
set `date` = str_to_date(`date`, '%m/%d/%Y'); 

select *
from layoffs_staging2;  

-- change type of date
alter table layoffs_staging2
modify column `date` DATE; 

select *
from layoffs_staging2;   

-- Populate Null/Blank values

select * from layoffs_staging2
where industry is null 
or industry = '';

update layoffs_staging2
set industry=NULL
where industry='';


select *
from layoffs_staging2 st1
join layoffs_staging2 st2
	on st1.company= st2.company
    
    
where (st1.industry is null ) and st2.industry is not null;

update layoffs_staging2 st1
join layoffs_staging2 st2
	on st1.company= st2.company
set st1.industry= st2.industry
    
where (st1.industry is null ) and st2.industry is not null;

select * from layoffs_staging2
where company='Airbnb';

-- remove column

select * from layoffs_staging2
where total_laid_off is null and percentage_laid_off is null;

delete
from layoffs_staging2
where total_laid_off is null and percentage_laid_off is null;

-- remove row_num

alter table layoffs_staging2
drop column row_num;

select*from layoffs_staging2;



