use metro_dwh;
-- Query 1
select a.id, a.store_name, a.Total
from
	(select sum(t.total_sale) as Total, p.store_name, d.year as id
	from transaction_fact t, dim_store p, dim_date d
	where t.store_id=p.store_id and t.date_id=d.date and (d.year='2017')) as a
order by a.Total
LIMIT 10;



-- Query 1

select t.store_id,s.store_name,sum(t.total_sale)
from transaction_fact as t , dim_store as s
where s.store_id = t.store_id
group by s.store_name
order by sum(t.total_sale) desc
limit 3;

-- Query 2

select t.supplier_id,s.supplier_name,sum(t.total_sale)
from transaction_fact as t , dim_supplier as s
where s.supplier_id = t.supplier_id
group by s.supplier_id
order by sum(t.total_sale) desc
limit 10;

-- Query 3

select p.product_name,s.supplier_name,d.quarter,sum(t.total_sale)
from dim_product as p, dim_supplier as s, dim_date as d, transaction_fact as t
where p.product_id=t.product_id and s.supplier_id=t.supplier_id and t.date_id=d.date
group by p.product_id;

-- Query 4

select a.pro, a.sto,a.tot from
(select p.product_name as pro, s.store_name as sto, sum(t.total_sale) as tot
from dim_product as p, dim_store as s, transaction_fact as t
where p.product_id=t.product_id and s.store_id=t.store_id
group by s.store_id) as a
group by a.pro;


-- Query 6
select a.id, a.product_name, a.Total
from
	(select sum(quantity) as Total, product_name, p.product_id as id
	from transaction_fact t, dim_product p, dim_date d
	where t.product_id=p.product_id and t.date_id=d.date and (d.day='Saturday' or d.day='Sunday')
	group by p.product_id
	order by sum(quantity) desc) as a
LIMIT 5;

-- Query 7

select p.product_name,s.store_name,sup.supplier_name, sum(t.total_sale)
from transaction_fact t, dim_product p, dim_store s, dim_supplier sup
where t.product_id = p.product_id and s.store_id = t.store_id and sup.supplier_id = t.supplier_id
group by p.product_id with rollup;




-- Query 8
Select a.pn,a.q as year_half,a.sm from
(Select p.product_name as pn ,d.month as q,sum(t.total_sale) as sm
from dim_product p, transaction_fact t, dim_date as d
where p.product_id=t.product_id and d.date=t.date_id and d.year = '2017'
group by p.product_id) as a
where a.q<7;

-- Query 10

DROP TABLE IF EXISTS `STORE_PRODUCT_ANALYSIS`;
create table STORE_PRODUCT_ANALYSIS as
select s.store_id as STORE_ID, p.product_id as PROD_ID, sum(total_sale) as STORE_TOTAL
from transaction_fact as t, dim_product as p, dim_store as s
where t.product_id=p.product_id and t.store_id=s.store_id
group by s.store_id, p.product_id
order by s.store_id, p.product_id;

select * from STORE_PRODUCT_ANALYSIS; 









