/* Min Temp */ 
WITH myconstants (var_city, var_year1, var_year2) as (
   values ('Changi', 2002, 2012)
) 
SELECT DISTINCT(year, month, day), temperature from weather, myconstants where city=var_city and (year, month, temperature) IN (select year, month, min(temperature) from weather, myconstants where temperature > 0 and (year = var_year1 or year = var_year2) and city=var_city group by year, month );

/* Max Temp */ 
WITH myconstants (var_city, var_year1, var_year2) as (
   values ('Changi', 2002, 2012)
) 
SELECT DISTINCT(year, month, day), temperature from weather, myconstants where city=var_city and (year, month, temperature) IN (select year, month, max(temperature) from weather, myconstants where temperature > 0 and (year = var_year1 or year = var_year2) and city=var_city group by year, month );

/* Max Humid */ 
WITH myconstants (var_city, var_year1, var_year2) as (
   values ('Changi', 2002, 2012)
) 
SELECT DISTINCT(year, month, day), humidity from weather, myconstants where city=var_city and (year, month, humidity) IN (select year, month, max(humidity) from weather, myconstants where humidity > 0 and (year = var_year1 or year = var_year2) and city=var_city group by year, month );

/* Min Humid */ 
WITH myconstants (var_city, var_year1, var_year2) as (
   values ('Changi', 2002, 2012)
) 
SELECT DISTINCT(year, month, day), humidity from weather, myconstants where city=var_city and (year, month, humidity) IN (select year, month, min(humidity) from weather, myconstants where humidity > 0 and (year = var_year1 or year = var_year2) and city=var_city group by year, month );

/* Count check */

/* Min Temp */ 
WITH myconstants (var_city, var_year1, var_year2) as (
   values ('Changi', 2002, 2012)
) 
SELECT COUNT(DISTINCT(year, month, day)) from weather, myconstants where city=var_city and (year, month, temperature) IN (select year, month, min(temperature) from weather, myconstants where temperature > 0 and (year = var_year1 or year = var_year2) and city=var_city group by year, month );

/* Max Temp */ 
WITH myconstants (var_city, var_year1, var_year2) as (
   values ('Changi', 2002, 2012)
) 
SELECT COUNT(DISTINCT(year, month, day)) from weather, myconstants where city=var_city and (year, month, temperature) IN (select year, month, max(temperature) from weather, myconstants where temperature > 0 and (year = var_year1 or year = var_year2) and city=var_city group by year, month );

/* Max Humid */ 
WITH myconstants (var_city, var_year1, var_year2) as (
   values ('Changi', 2002, 2012)
) 
SELECT COUNT(DISTINCT(year, month, day)) from weather, myconstants where city=var_city and (year, month, humidity) IN (select year, month, max(humidity) from weather, myconstants where humidity > 0 and (year = var_year1 or year = var_year2) and city=var_city group by year, month );

/* Min Humid */ 
WITH myconstants (var_city, var_year1, var_year2) as (
   values ('Changi', 2002, 2012)
) 
SELECT COUNT(DISTINCT(year, month, day)) from weather, myconstants where city=var_city and (year, month, humidity) IN (select year, month, min(humidity) from weather, myconstants where humidity > 0 and (year = var_year1 or year = var_year2) and city=var_city group by year, month );
