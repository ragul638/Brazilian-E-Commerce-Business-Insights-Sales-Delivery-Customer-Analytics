use ecom

select * from olist_customers_dataset
select * from olist_geolocation_dataset
select * from olist_order_items_dataset
select * from olist_order_payments_dataset
select * from olist_order_reviews_dataset
select * from olist_orders_dataset
select * from olist_products_dataset
select * from olist_sellers_dataset
select * from product_category_name_translation

select * from clean_olist_orders_dataset
select * from clean_olist_products_dataset
select * from clean_olist_order_items_dataset
select * from clean_olist_order_reviews_dataset

select * from join_tables_analysis


--## checkin duplicate values

select * 
from olist_customers_dataset 
group by customer_id,customer_unique_id,customer_zip_code_prefix,customer_city,customer_state 
having count(*) > 1--customers_dataset

select * 
from olist_order_items_dataset 
group by order_id,order_item_id,product_id,seller_id,shipping_limit_date,price,freight_value 
having count(*) > 1--order_items_dataset

select * 
from olist_order_payments_dataset 
group by order_id,payment_sequential,payment_type,payment_installments,payment_value 
having count(*) > 1--order_payments_dataset

select 
    product_id,product_category_name,
    count(*) 
from olist_products_dataset 
group by product_id,product_category_name 
having count(*) > 1--products_dataset

select 
    order_id,customer_id,
    order_status,count(*) 
from olist_orders_dataset 
group by order_id,order_status,customer_id 
having count(*) > 1--orders_dataset



--## checking null values

select 
    count(*) as total_rows, 
    sum(case when customer_id is null then 1 else 0 end) as null_customer_id,
    sum(case when customer_unique_id is null then 1 else 0 end) as null_customer_unique_id,
    sum(case when customer_city is null then 1 else 0 end) as null_customer_city 
from olist_customers_dataset--customers_dataset

select 
    count(*) as total_rows, 
    sum(case when order_id is null then 1 else 0 end) as null_order_id,
    sum(case when order_item_id is null then 1 else 0 end) as null_order_item_id,
    sum(case when product_id is null then 1 else 0 end) as null_product_id,---order_items_dataset
    sum(case when seller_id is null then 1 else 0 end) as null_seller_id 
from olist_order_items_dataset

select 
    count(*) as total_rows, 
    sum(case when order_id is null then 1 else 0 end) as null_order_id,
    sum(case when payment_type is null then 1 else 0 end) as null_Payment_type,
    sum(case when payment_installments is null then 1 else 0 end) as null_payment_installment 
from olist_order_payments_dataset--order_payments_dataset

select 
    count(*) as total_rows,
    sum(case when review_id is null then 1 else 0 end)as null_review_id,
    sum(case when order_id is null then 1 else 0 end)as null_order_id,
    sum(case when review_creation_date is null then 1 else 0 end)as null_review_creation_date 
from olist_order_reviews_dataset--order_reviews_dataset

select 
    count(*) as total_rows, 
    sum(case when seller_id is null then 1 else 0 end)as null_seller_id,--olist_sellers_dataset
    sum(case when seller_city is null then 1 else 0 end)as null_seller_city,
    sum(case when seller_state is null then 1 else 0 end)as null_seller_state 
from olist_sellers_dataset

--## there are some null values in the order_delivered_customer_date(so i am going to create a new table without null)

select 
    count(*) as total_rows, 
    sum(case when order_id is null then 1 else 0 end)as null_order_id,
    sum(case when customer_id is null then 1 else 0 end)as null_customer_id,
    sum(case when order_status is null then 1 else 0 end)as null_order_status,
    sum(case when order_purchase_timestamp is null then 1 else 0 end)as null_order_purch_time,--orders_dataset
    sum(case when order_delivered_customer_date is null then 1 else 0 end)as null_deliver_custo_date 
from olist_orders_dataset where order_status = 'delivered'

--## cleande dataset of olist_orders_dataset (renamed as clean_olist_orders_dataset)

select * 
    into clean_olist_orders_dataset 
from olist_orders_dataset 
where not(order_status = 'delivered' and order_delivered_customer_date is null )--orders_dataset

--## there are some null values in the olist_products_dataset(so i am going to create a new table without null)

select 
    count(*),   
    sum(case when product_id is null then 1 else 0 end)as null_product_id,
    sum(case when product_category_name is null then 1 else 0 end) as null_prod_name 
from olist_products_dataset--products_dataset

--## cleande dataset of olist_orders_dataset (renamed as clean_olist_orders_dataset)

select * 
into clean_olist_products_dataset 
from olist_products_dataset--products_dataset

update clean_olist_products_dataset 
set product_category_name = 'unknown' 
where product_category_name is null--products_dataset



--## fix date format

select 
    order_id,order_item_id,
    product_id,seller_id,
    cast(shipping_limit_date as date) as shipped_date,
    price,freight_value 
into clean_olist_order_items_dataset 
from olist_order_items_dataset--order_items_dataset

select 
    review_id,order_id,
    review_score,review_comment_title,
    review_comment_message,
    cast(review_creation_date as date)as review_creation_date,
    cast(review_answer_timestamp as date)as review_answer_timestamp 
into clean_olist_order_reviews_dataset 
from olist_order_reviews_dataset--order_reviews_dataset

--## created new column in clean_orders_dataset

alter table clean_olist_orders_dataset--orders_dataset
add total_days_to_deliver int

--## updated date column for clean_orders_dataset
update clean_olist_orders_dataset 
set order_purchase_timestamp = convert(date,order_purchase_timestamp),
    order_delivered_customer_date = convert(date,order_delivered_customer_date),
    total_days_to_deliver = DATEDIFF(DAY, order_purchase_timestamp, order_delivered_customer_date)  
where order_status = 'delivered'--orders_dataset


alter table clean_olist_orders_dataset
alter column order_purchase_timestamp date

alter table clean_olist_orders_dataset
alter column order_estimated_delivery_date date

alter table clean_olist_orders_dataset
alter column order_delivered_customer_date date--orders_dataset

DELETE FROM clean_olist_orders_dataset--orders_dataset
WHERE order_status != 'delivered';



--## joining the tables together

select 
    o.order_id,o.order_purchase_timestamp,
    o.order_delivered_customer_date,c.customer_city,
    c.customer_state,oi.product_id,
    o.order_status,pr.product_category_name,
    oi.price,p.payment_value,p.payment_type 
into join_tables_analysis 
from clean_olist_orders_dataset as o 
join olist_customers_dataset as c on o.customer_id = c.customer_id 
join clean_olist_order_items_dataset as oi on o.order_id = oi.order_id 
join olist_order_payments_dataset as p on o.order_id = p.order_id 
join clean_olist_products_dataset as pr on oi.product_id = pr.product_id 


-- 1. Which states generate the highest revenue, and which ones have declining sales trends?

select top 10
  c.customer_state,
  sum(p.payment_value) as total_revenue
from clean_olist_orders_dataset o
join olist_customers_dataset c 
    on o.customer_id = c.customer_id
join olist_order_payments_dataset p 
    on o.order_id = p.order_id
where o.order_status = 'delivered'
group by c.customer_state
order by total_revenue desc

-- 2. What percentage of customers are repeat buyers vs one-time buyers?

select 
    customer_type,count(*) as customer_count,
    round(
        count(*) * 100.0 / sum(count(*)) over(),2
    ) as percentage
from (
    select
        c.customer_unique_id,
        case    
            when count(o.order_id) = 1 then 'one time buyer'
            else 'repeat buyer'
        end as customer_type
    from olist_customers_dataset as c
    join clean_olist_orders_dataset as o
    on c.customer_id = o.customer_id
    group by c.customer_unique_id
)t
group by customer_type

-- 3. Which product categories have the highest cancellation rates?

select 
    p.product_category_name,
    count(distinct o.order_id) as total_orders,
    count(distinct case 
        when o.order_status = 'canceled' then o.order_id 
    end) as canceled_orders,
    round(
        count(distinct case 
            when o.order_status = 'canceled' then o.order_id 
        end) * 1.0 
        / count(distinct o.order_id), 2) as cancellation_rate
from olist_orders_dataset o
join clean_olist_order_items_dataset oi on o.order_id = oi.order_id
join clean_olist_products_dataset p on oi.product_id = p.product_id
group by p.product_category_name
order by cancellation_rate desc

-- 4. What is the average delivery time compared to the estimated delivery date?

select 
    avg(datediff(day,order_purchase_timestamp,order_delivered_customer_date)) as avg_actual_delivery_days,
    avg(datediff(day,order_purchase_timestamp,order_estimated_delivery_date)) as avg_estimated_delivery_days,
    avg(datediff(day,order_estimated_delivery_date,order_delivered_customer_date)) as avg_delay_days
from clean_olist_orders_dataset

-- 5. Which states experience the most late deliveries?

select 
    c.customer_state,
    count(*) as late_delivery
from clean_olist_orders_dataset as o join olist_customers_dataset as c on o.customer_id = c.customer_id
where o.order_delivered_customer_date > o.order_estimated_delivery_date
group by c.customer_state
order by late_delivery desc

-- 6. What percentage of orders are canceled or never delivered?

select 
    count(*) as total_orders,
    sum(case when order_status in ('canceled','unavailable')
    then 1 else 0 end) as failed_orders,
    (sum(case when order_status in ('canceled','unavailable')
    then 1 else 0 end) * 100.0 / count(*)) as failed_percentage
from olist_orders_dataset

-- 7. Which sellers consistently miss shipping deadlines?

select 
    oi.seller_id,
    count (*) as total_orders,
    sum(case 
        when o.order_delivered_carrier_date > oi.shipped_date then 1 
        else 0 
    end) as late_shipments,
    round(
        sum(case 
            when o.order_delivered_carrier_date > oi.shipped_date then 1 
            else 0 
        end) * 100.0 / count(*), 2
    ) as late_percentage
from clean_olist_order_items_dataset oi
join clean_olist_orders_dataset o 
    on oi.order_id = o.order_id
group by oi.seller_id
having count(*) > 50
order by late_percentage desc;

-- 8. What is the average order value (AOV) across different payment types?

select 
    payment_type,
    count(distinct order_id) as total_orders,
    sum(payment_value) as total_revenue,
    sum(payment_value) * 1.0 / count(distinct order_id) as AOV
from olist_order_payments_dataset
group by payment_type
order by AOV desc

-- 9. Which payment methods are most popular, and do they correlate with higher/lower revenue?

select 
    payment_type,
    count(*) as total_transaction,
    sum(payment_value) as total_payment,
    avg(payment_value) as avg_of_payment
from olist_order_payments_dataset 
group by payment_type 
order by total_transaction desc
-- 10. How much revenue is lost due to canceled or undelivered orders?

select 
    o.order_status,
    sum(p.payment_value) as lost_revenue 
from olist_orders_dataset as o 
join olist_order_payments_dataset as p on o.order_id = p.order_id
where order_status <> 'delivered'
group by order_status
order by lost_revenue desc

select 
    sum(case when order_status = 'delivered' then payment_value else 0 end) as actual_revenue,
    sum(case when order_status != 'delivered' then payment_value else 0 end) as lost_revenue
from olist_orders_dataset as o 
join olist_order_payments_dataset as p on o.order_id = p.order_id

-- 11. Which product categories contribute most to revenue vs most to freight costs?

select 
    p.product_category_name,
    sum(oi.price) as total_revenue,
    sum(oi.freight_value) as total_freight_cost,
    sum(oi.freight_value) / sum(oi.price) as freight_ratio
from olist_order_items_dataset oi
join olist_products_dataset p on oi.product_id = p.product_id
group by p.product_category_name
order by freight_ratio desc

-- 12. Which sellers generate the highest revenue, and which have the most complaints/low reviews?

select 
    oi.seller_id,
    sum(oi.price) as total_revenue,
    avg(r.review_score) as avg_rating,
    count(r.review_id) as total_reviews
from olist_order_items_dataset oi
join olist_orders_dataset o on oi.order_id = o.order_id
left join olist_order_reviews_dataset r on o.order_id = r.order_id
where o.order_status = 'delivered'
group by oi.seller_id
order by total_revenue desc

-- 13. Are certain product categories more prone to late deliveries or high freight charges?

select 
    p.product_category_name,
    count(*) as total_orders,
    sum(case 
        when o.order_delivered_customer_date > o.order_estimated_delivery_date 
        then 1 else 0 end) as late_orders,
    round(
        100.0 * sum(case 
            when o.order_delivered_customer_date > o.order_estimated_delivery_date 
            then 1 else 0 end) / count(*), 2
    ) as late_percentage,
    avg(oi.freight_value) as avg_freight_cost
from clean_olist_orders_dataset o
join clean_olist_order_items_dataset oi on o.order_id = oi.order_id
join clean_olist_products_dataset p on oi.product_id = p.product_id
group by p.product_category_name
order by late_percentage desc

-- 14. What is the correlation between delivery time and review score?

select 
    case 
        when datediff(day,o.order_purchase_timestamp,o.order_delivered_customer_date) <= 3 then 'Fast Delivery'
        when datediff(day,o.order_purchase_timestamp,o.order_delivered_customer_date) <= 7 then 'Medium Delivery'
        else 'Slow Delivery'
    end as delivery_speed,
    avg(r.review_score) as avg_review
from clean_olist_orders_dataset as o 
join clean_olist_order_reviews_dataset as r on o.order_id = r.order_id
group by case 
        when datediff(day,o.order_purchase_timestamp,o.order_delivered_customer_date) <= 3 then 'Fast Delivery'
        when datediff(day,o.order_purchase_timestamp,o.order_delivered_customer_date) <= 7 then 'Medium Delivery'
        else 'Slow Delivery' end

-- 15. Which product categories receive the lowest average review scores?

select top 5 
    p.product_category_name,
    avg(r.review_score) as avg_rating
from olist_order_reviews_dataset r
join olist_orders_dataset o on r.order_id = o.order_id
join olist_order_items_dataset oi on o.order_id = oi.order_id
join olist_products_dataset p on oi.product_id = p.product_id
group by p.product_category_name
order by avg_rating asc

--## final data for sharing it into python

select 
    o.order_id,
    c.customer_unique_id,
    c.customer_state,
    c.customer_city,
    
    o.order_status,
    o.order_purchase_timestamp,
    o.order_delivered_customer_date,
    o.order_estimated_delivery_date,
    
    oi.product_id,
    po.product_category_name,
    oi.price,
    oi.freight_value,
    
    p.payment_value,
    p.payment_type,
    r.review_score,
    
    s.seller_id,
    s.seller_state,
    s.seller_city
into final_dataset
from olist_orders_dataset o
join olist_customers_dataset c on o.customer_id = c.customer_id
join olist_order_items_dataset oi on o.order_id = oi.order_id
join olist_products_dataset po on po.product_id = oi.product_id
join (
    select 
        order_id,
        sum(payment_value) as payment_value,
        string_agg(payment_type, ', ') as payment_type
    from olist_order_payments_dataset
    group by order_id
) p on o.order_id = p.order_id

left join olist_order_reviews_dataset r on o.order_id = r.order_id
join olist_sellers_dataset s on oi.seller_id = s.seller_id;

alter table final_dataset
alter column order_purchase_timestamp date

alter table final_dataset
alter column order_delivered_customer_date date

alter table final_dataset
alter column order_estimated_delivery_date date

select * from final_dataset
