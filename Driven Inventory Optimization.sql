create schema tech_electro;
use tech_electro;

-- DATA EXPLORATION
select * from External_Factors limit 5;
select * from sales_data limit 5;
select * from inventory_data limit 5;
select * from product_information limit 5;
-- --Understanding the structure of the the datasets 
show columns from External_Factors;
describe product_information;
desc sales_data;

-- Data cleaning
-- Changing to the right data type for all columns
-- SalesDate DATE, GDP DECIMAL(15, 2), InflationRate DECIMAL(5, 2), seasonalFactor Decimal(5, 2)
alter table external_factors
add column New_Sales_Date date;
set SQL_SAFE_UPDATES = 0; -- turning off safe updates
update external_factors
set New_Sales_Date = str_to_date(Sales_Date,'%d/%m/%Y');
alter table external_factors
drop column Sales_Date;
alter table external_factors
change column New_Sales_Date Sales_Date Date;
alter table external_factors
modify column GDP decimal(15, 2);

alter table external_factors 
modify column Seasonal_Factor decimal(5, 2);

alter table external_factors
modify column Inflation_Rate decimal(5, 2);

show columns from external_factors;
-- Product
-- Product_ID INT NOT NULL, Product_Category TEXT, Promotions ENUM('yes', 'no') 
alter table product_information
add column NewPromotions enum('yes', 'no');

update product_information
set NewPromotions = case
	when Promotions = 'yes' then 'yes'
    when Promotions = 'no' then 'no'
    else null
end;
alter table product_information
drop column Promotions;
alter table product_information
change column NewPromotions Promotions enum('yes', 'no');
describe product_information;

alter table external_factors
modify column GDP decimal(15, 2);

-- sale data
-- Product_ID INT NOT NULL, Sales_Date DATE, Inventory_Quantity INT, Product_Cost Decimal(10, 2)
alter table sales_data
add column New_Sales_Date date;
set SQL_SAFE_UPDATES=0;
update sales_data
set New_Sales_Date = str_to_date(Sales_Date,'%d/%m/%Y');
alter table sales_data
drop column Sales_Date;
alter table sales_data
change column New_Sales_Date Sales_Date Date;
desc sales_data;

-- Identify missing values using 'is null' function
-- external factor
select
sum(case when Sales_Date is null then 1 else 0 end) as missing_sales_date,
sum(case when GDP is null then 1 else 0 end) as missing_gdp,
sum(case when Inflation_Rate is null then 1 else 0 end) as missing_inflation_rate,
sum(case when Seasonal_Factor is null then 1 else 0 end) as missing_seasonal_factor
from external_factors;
-- product_data
select
sum(case when Product_ID is null then 1 else 0 end) as missing_Product_ID,
sum(case when Product_Category is null then 1 else 0 end) as missing_Product_Category,
sum(case when Promotions is null then 1 else 0 end) as missing_Promotions
from product_information;

-- sales_data
select
sum(case when Product_ID is null then 1 else 0 end) as missing_Product_ID,
sum(case when Sales_Date is null then 1 else 0 end) as missing_sales_date,
sum(case when Inventory_Quantity is null then 1 else 0 end) as missing_Inventory_Quantity,
sum(case when Product_Cost is null then 1 else 0 end) as missing_Product_Cost
from sales_data;

-- Check for duplicates using 'group by' and 'having' clauses and remove them if necessary.alter
-- EXTERNAL FACTORS
select sales_date, count(*) as count
from external_factors
group by sales_date
having count > 1;

select count(*) from (select sales_date, count(*) as count
from external_factors
group by sales_date
having count > 1) as dup;

-- Product data
SELECT product_id, ANY_VALUE(product_category) as product_category, COUNT(*) as count
FROM product_information
GROUP BY product_id
HAVING count > 1;

select count(*) from (SELECT product_id, ANY_VALUE(product_category) as product_category, COUNT(*) as count
FROM product_information
GROUP BY product_id
HAVING count > 1) as dup;

-- sales_data
SELECT product_id, sales_date, COUNT(*) as count
FROM sales_data
GROUP BY product_id, sales_date
HAVING count > 1;

-- dealing with duplicates for external_factors and Product_data
-- external factor
delete e1 from external_factors e1
inner join (
select Sales_Date,
row_number() over (partition by Sales_Date order by Sales_Date) as rn
from external_factors
) e2 on e1.Sales_Date = e2.Sales_Date
where e2.rn > 1;

-- product data 
delete p1 from product_information p1
inner join (
select product_id, row_number() over (partition by product_id order by product_id) as rn
from product_information
) p2 on p1.product_id = p2.product_id
where p2.rn > 1;

-- DATA INTEGRATION
-- sales_data and product_information first
create view sales_product_data as
select
s.Product_ID,
s.Sales_Date,
s.Inventory_Quantity,
s.Product_Cost,
p.Product_Category,
p.Promotions
from sales_data s 
join product_information p on s.Product_ID = p.Product_ID;

-- sales_product_data and external_factors
create view Inventory_Information as 
select
sp.Product_ID,
sp.Sales_Date,
sp.Inventory_Quantity,
sp.Product_Cost,
sp.Product_Category,
sp.Promotions,
e.GDP,
e.Inflation_Rate,
e.Seasonal_Factor
from sales_product_data sp
left join external_factors e
on sp.Sales_Date = e.Sales_Date;

-- Descriptive Analysis
-- Basics Statistics:
-- Average sales (calculated as the product of "Inventory Quantity" and "Product Cost").
select product_id,
avg(inventory_quantity * product_cost)as avg_sales
from Inventory_information
group by product_id
order by avg_sales desc;

-- Median stock levels (i.e., "Inventory_Quantity")
select product_id, avg(Inventory_Quantity) as median_stock
from(
select product_id,
		inventory_Quantity,
row_number() over(partition by product_id order by Inventory_Quantity) as row_num_asc,
row_number() over(partition by product_id order by Inventory_Quantity desc) as row_num_desc
from inventory_data
) as subquery
where row_num_asc in (row_num_desc, row_num_desc -1, row_num_desc +1)
group by product_id;        

-- Product performance metrics (total sales per product).
select Product_ID,
round(sum(Inventory_Quantity * Product_Cost)) as total_sales
from inventory_data
group by Product_ID
order by total_sales desc;

-- Identify high demand products based on average sales
WITH highdemandproducts AS (
    SELECT 
        product_id, 
        AVG(inventory_quantity) AS avg_sales
    FROM 
        inventory_data
    GROUP BY 
        product_id
    HAVING 
        avg_sales > (
            SELECT 
                AVG(inventory_quantity) * 0.95 
            FROM 
                sales_data
        )
)
-- calculate stockout frequency for high-demand products
select s.product_id,
count(*) as stockout_frequency
from inventory_data s
where s.product_id in (select product_id from highdemandproducts)
and s.inventory_quantity = 0
group by s.Product_id;

-- Influence of external factors
-- GDP
select product_id,
avg(case when 'gdp' > 0 then inventory_quantity else null end) as avg_sales_positive_gdp,
avg(case when 'gdp' <= 0 then inventory_quantity else null end) as avg_sales_non_positive_gdp
from inventory_data
group by Product_ID
having avg_sales_positive_gdp is not null;
-- inflation
select product_id,
avg(case when inflation_rate > 0 then inventory_quantity else null end) as avg_sales_positive_inflation,
avg(case when inflation_rate <= 0 then inventory_quantity else null end) as avg_sales_non_positive_inflation
from inventory_data
group by product_id
having avg_sales_positive_inflation is not null;

-- Optimizing Inventory
-- Reorder point for each product based on historical sales data and external factors.
-- Reorder point = lead time demand + safety stock
-- Lead time demand = rolling average sales * leadtime
-- Reorder Point = rolling average sales * leadtime + Z * lead time^-2 * standard Deviation of demand
-- Safety stock = Z * lead time^-2 * standard Deviation of demand
-- Z = 1.645
-- A constant lead time of 7 days for all products
-- we aim for a 95% service level
with InventoryCalculations as (
 select product_id,
 avg(rolling_avg_sales) as avg_rolling_sales,
 avg(rolling_variance) as avg_rolling_variance
 from(
 select product_id,
 avg(daily_sales) over (partition by product_id order by sales_date rows between 6 preceding and current row) as rolling_avg_sales,
 avg(squared_diff) over (partition by product_id order by sales_date rows between 6 preceding and current row) as rolling_variance
 from (
 select product_id,
  sales_date, Inventory_quantity * product_cost as daily_sales,
  (Inventory_quantity * Product_Cost - avg(inventory_quantity * product_cost) over (partition by product_id order by sales_date rows between 6 preceding and current row))
 * (Inventory_quantity * Product_Cost - avg(inventory_quantity * product_cost) over (partition by product_id order by sales_date rows between 6 preceding and current row)) as squared_diff 
  from inventory_data
  ) subquery
   ) subquery2
    group by product_id
)
select product_id,
avg_rolling_sales * 7 as lead_time_demand,
  1.645 * (avg_rolling_variance * 7) as safety_stock,
(avg_rolling_sales * 7) + (1.645 * (avg_rolling_variance * 7)) as reorder_point
from InventoryCalculations;
-- create the inventory_optimization table
create table inventory_optimization (
   product_id int,
 Reorder_point double
 );
 
-- step 2: create the stored procedure to recalculate reorder point 
DELIMITER //
create procedure RecalculateReorderPoint(productID int)
begin
    declare avgRollingSales double;
    declare avgRollingVariance double;
    declare leadTimeDemand double;
    declare safetystock double;
    declare reorderpoint double;
    select
 avg(rolling_avg_sales), avg(rolling_variance)
 into avgRollingSales, avgRollingVariance
 from(
 select product_id,
 avg(daily_sales) over (partition by product_id order by sales_date rows between 6 preceding and current row) as rolling_avg_sales,
 avg(squared_diff) over (partition by product_id order by sales_date rows between 6 preceding and current row) as rolling_variance
 from (
 select product_id,
  sales_date, Inventory_quantity * product_cost as daily_sales,
  (Inventory_quantity * Product_Cost - avg(inventory_quantity * product_cost) over (partition by product_id order by sales_date rows between 6 preceding and current row))
 * (Inventory_quantity * Product_Cost - avg(inventory_quantity * product_cost) over (partition by product_id order by sales_date rows between 6 preceding and current row)) as squared_diff 
  from inventory_data
  ) innerDerived
   ) outerDerived;
set leadTimeDemand = avgRollingSales * 7;
set safetystock = 1.645 * sqrt(avgRollingVariance * 7);
set reorderPoint = leadTimeDemand + safetyStock;

insert into inventory_optimization (Product_id, Reorder_Point)
   values (productId, reorderPoint)
   on duplicate key update Reorder_Point = reorderPoint;
   end//
   DELIMITER ;
   
   -- step 3: make inventory_data a permanent table
   create table inventory_table as select * from Inventory_data;
--    step 4: Create the Trigger
DELIMITER //
create trigger AfterInsertUnifiedTable
after insert on Inventory_data
for each row 
Begin
 call RecalculateReorderPoint(New.Product_ID);
 end//
 DELIMITER ;
   
-- Overstocking and understocking
with RollingSales as (
 select Product_Id, 
 Sales_Date,
avg(Inventory_Quantity * Product_Cost) over (partition by product_ID order by Sales_Date rows between 6 preceding and current row) as rolling_avg_sales
 from inventory_table
),
-- Calculate the number of days a product was out of stock
StockoutDays as (
select Product_Id,
 count(*) as stockout_days
 from inventory_table
 where Inventory_Quantity = 0
 group by product_id
 )
--  join the above CTEs with the main table to get the results
select f.Product_id,
avg(f.Inventory_Quantity * f.Product_Cost) as avg_inventory_value,
avg(rs.rolling_avg_sales) as avg_rolling_sales,
 coalesce(sd.stockout_days, 0) as stockout_days
 from inventory_table f
 join RollingSales rs on f.product_id = rs.Product_ID and f.sales_Date = rs.Sales_Date
 left join stockoutDays sd on f.Product_ID = sd.Product_ID
 group by f.Product_ID, sd.stockout_days;
 
 -- MONITOR AND ADJUST
--   MONITOR INVENTORY LEVELS
DELIMITER //
create procedure MonitorInventoryLevels()
begin
select product_id, avg(Inventory_Quantity) as AvgInventory
from Inventory_Table
group by Product_Id
order by AvgInventory desc;
end//
DELIMITER ;

-- Monitor Sales Trends
DELIMITER //
Create procedure MonitorSalesTrends()
begin 
select product_ID, Sales_Date,
AVG(Inventory_Quantity * Product_Cost) over (partition by Product_ID order by Sales_Date rows between 6 preceding and current row) as RollingAvgSales
    from inventory_table
      order by Product_Id, Sales_Dates;
end//
DELIMITER ;

-- Monitor stockout frequencies
DELIMITER //
create procedure MonitorStockouts()
begin
select Product_id, count(*) as StockoutDays
from inventory_table
 where Inventory_Quantity = 0
group by Product_ID
order by StockoutDays desc;
 end//
 DELIMITER ;
 
--  FEEDBACK LOOP
--  Feedback Loop Establishment:
--  Feedback Portal: Develop an online platform for shareholders to easily submit feedback on inventory performance and challenges.
--  Review Meetings: Organize periodic Meetings to discuss inventory system performance and gather direct insights.
--  System Monitoring: Use established SQL procedures to track system metrics, with deviations from expectations flagged for review.
--  
--  Refinement Based on Feedback:
--  Feedback Analysis: Regularly compile and scrutinize feedback to identify recurring themes or pressing issues.
--  Action Implementation: Prioritize and act on the feedback to adjust reorder points, safety stock levels, or overall processes.
--  Change Communication: Inform stakeholders about changes, underscoring the value of their feedback and ensuring transparency.


-- General insights:
-- Inventory Discrepancies: The initial stages of the analysis revealed significant discrepancies in inventory levels, with instances of both overstocking and understocking.
-- These inconsistencies were contributing to capital inefficiencies and customer dissatisfaction.

-- Sales Trends and Eternal Influences: The analysis indicated that sales trends were notably influenced by various external factors.
-- Recognizing these patterns provides an opportunity to forecast demand more accurately.

-- Suboptimal Inventory Levels: Through the inventory optimization analysis, it was evident that the existing inventory levels were not optimized for current sales trends.
-- Products was identified that had either close excess inventory.


-- Recommendations:
-- 1. Implement Dynamic Inventory Management; The Company sould transition from static to a dynamic inventory management system,
-- adjusting inventory levels based on real-time sales trends, seasonality and external factors.

-- 2. Optimize Reorder Points and Safety Stocks: Utilize the reorder points and safety stocks calculated during the analysis to minimize stockouts and reduce excess inventory.
-- Regularly review these metrics to ensure they align with current market conditions.

-- 3.Enhance Pricing Strategies: Conduct a thorough review of product pricing strategies, especially for products identified as un profitable.
-- Consider Factors such as competitor pricing, market demand and product acquisition costs.

-- 4.Reduce Overstock: Identify products that are consistently overstocked and take steps to reduce their inventory levels.
-- This could include promotional sales, discounts or even discounting products with low sales performance.

-- 5.Establish a feedback loop: Develop a systematic approach to collect and analyze feedback from various stakeholders.
-- Use this feedback for continous improvement and alignment with business objectives.

-- 6.Regular Monitoring and Adjustments: Adopt a proactive approach to inventory management by regularly monitoring key metrics 
-- and making necessary adjustments to inventory levels, order quantities and safety stocks.