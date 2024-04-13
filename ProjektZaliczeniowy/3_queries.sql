select * from (
select distinct top 8 dca.carrier_name, DENSE_RANK() over(partition by dca.carrier_name order by cast(fs.amount as float) desc) as highest_carrier_transactions, fs.amount 
from FactSales fs
join DIMCustomers dcu
	on fs.CustomersKey = dcu.CustomersKey
join DIMCarrier dca
	on fs.CarrierKey = dca.CarrierKey
join DIMDate dd
	on dd.DateKey = fs.DateKey
order by highest_carrier_transactions) a
order by carrier_name, highest_carrier_transactions

select 
	fs.date,
	dca.carrier_name,
	cast(fs.amount as float) as amount,
	round(cast(fs.amount as float)/SUM(cast(fs.amount as float)) over(partition by dca.carrier_name, dd.Year, dd.Month, dd.Day)*100,2) as percent_of_daily_sales,
	round(cast(fs.amount as float)/SUM(cast(fs.amount as float)) over(partition by dca.carrier_name, dd.Year, dd.Month)*100,2) as percent_of_monthly_sales
from FactSales fs
join DIMCustomers dcu
	on fs.CustomersKey = dcu.CustomersKey
join DIMCarrier dca
	on fs.CarrierKey = dca.CarrierKey
join DIMDate dd
	on dd.DateKey = fs.DateKey
order by dd.DateKey

select 
	fs.date,
	dca.carrier_name,
	fs.amount,
	dcu.first_name,
	dcu.last_name,
	count(*) over(partition by dcu.state, dcu.city, dcu.street, dcu.last_name) as transaction_count_by_person,
	dcu.city,
	count(*) over(partition by dcu.state, dcu.city) as transaction_count_by_city,
	dcu.state,
	count(*) over(partition by dcu.state) as transaction_count_by_state
from FactSales fs
join DIMCustomers dcu
	on fs.CustomersKey = dcu.CustomersKey
join DIMCarrier dca
	on fs.CarrierKey = dca.CarrierKey
join DIMDate dd
	on dd.DateKey = fs.DateKey
order by cast(amount as float) desc
