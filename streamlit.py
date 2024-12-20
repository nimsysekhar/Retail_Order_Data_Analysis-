import streamlit as st
import sqlalchemy as sal
import pandas as pd
import psycopg2


#connect into Postgre SQL
engine = sal.create_engine('postgresql+psycopg2://postgres:1234@localhost:5432/retail_order_data')
conn=engine.connect()

def func_1():  
    st.title("Retail Order Analysis")  
    q_1 = "SELECT sub_category as products, sum(profit) as Highest_Revenue FROM df_orders_2 GROUP BY products ORDER BY Highest_Revenue desc LIMIT 10;"
    df_1 = pd.read_sql(q_1,conn)
    df_1
    conn.close()

def func_2():  
    st.title("Retail Order Analysis")  
    q_2 = "SELECT city as cities, sum(profit) as Highest_profit FROM df_orders_1 FULL JOIN df_orders_2 ON df_orders_1.order_id = df_orders_2.order_id_2 GROUP BY cities ORDER BY Highest_profit desc LIMIT 5;"
    df_2 = pd.read_sql(q_2,conn)
    df_2
    conn.close()

def func_3():  
    st.title("Retail Order Analysis")  
    q_3 = "SELECT category, sum(discount) as total_discount FROM df_orders_1 FULL JOIN df_orders_2 ON df_orders_1.order_id = df_orders_2.order_id_2 GROUP BY category ORDER BY total_discount desc ;"
    df_3 = pd.read_sql(q_3,conn)
    df_3
    conn.close()

def func_4():  
    st.title("Retail Order Analysis") 
    q_4 = "SELECT sub_category as product_category, avg(sale_price) as average_sale_price FROM df_orders_2 GROUP BY sub_category ORDER BY average_sale_price desc ;"
    df_4 = pd.read_sql(q_4,conn)
    df_4
    conn.close()    

def func_5():  
    st.title("Retail Order Analysis")  
    q_5 = "SELECT region, avg(sale_price) as average_sale_price FROM df_orders_1 FULL JOIN df_orders_2 ON df_orders_1.order_id = df_orders_2.order_id_2 GROUP BY region ORDER BY average_sale_price desc limit 1;"
    df_5 = pd.read_sql(q_5,conn)
    df_5
    conn.close()

def func_6():  
    st.title("Retail Order Analysis")  
    q_6 = "SELECT category, sum(profit) as total_profit FROM df_orders_1 FULL JOIN df_orders_2 ON df_orders_1.order_id = df_orders_2.order_id_2 GROUP BY category ORDER BY total_profit desc;"
    df_6 = pd.read_sql(q_6,conn)
    df_6
    conn.close()

def func_7():  
    st.title("Retail Order Analysis")  
    q_7 = "SELECT segment, count(order_id) as highest_orders FROM df_orders_1 GROUP BY segment ORDER BY highest_orders desc limit 3;"
    df_7 = pd.read_sql(q_7,conn)
    df_7
    conn.close()

def func_8():  
    st.title("Retail Order Analysis")  
    q_8 = "SELECT region, avg(discount_percent) as average_discount_Percentage FROM df_orders_1 FULL JOIN df_orders_2 ON df_orders_1.order_id = df_orders_2.order_id_2 GROUP BY region ORDER BY average_discount_Percentage desc;"
    df_8 = pd.read_sql(q_8,conn)
    df_8
    conn.close()

def func_9():
    st.title("Retail Order Analysis")  
    q_9 = "SELECT sub_category as product_category, sum(profit) as total_profit FROM df_orders_2 GROUP BY product_category ORDER BY total_profit desc limit 1;"
    df_9 = pd.read_sql(q_9,conn)
    df_9
    conn.close()

def func_10():  
    st.title("Retail Order Analysis")
    q_10 = "SELECT To_char(order_date::DATE,'YYYY') as year, sum(profit) as total_revenue FROM df_orders_1 FULL JOIN df_orders_2 ON df_orders_1.order_id = df_orders_2.order_id_2 GROUP BY year ORDER BY total_revenue desc;"
    df_10 = pd.read_sql(q_10,conn)
    df_10
    conn.close()

def func_11():  
    st.title("Retail Order Analysis")
    q_11 = "with cte as (select region,sub_category,sum(sale_price) as sales from df_orders_1 FULL JOIN df_orders_2 ON df_orders_1.order_id = df_orders_2.order_id_2 group by region,sub_category) select * from (select *, row_number() over(partition by region order by sales desc) as rn from cte) A where rn<=5;"
    df_11 = pd.read_sql(q_11,conn)
    df_11
    conn.close()

def func_12():  
    st.title("Retail Order Analysis")
    q_12 = "with cte as (select extract(year from order_date) as order_year,extract(month from order_date) as order_month, sum(sale_price) as sales from df_orders_1 FULL JOIN df_orders_2 ON df_orders_1.order_id = df_orders_2.order_id_2 group by order_year,order_month) select order_month, sum(case when order_year=2022 then sales else 0 end) as sales_2022 , sum(case when order_year=2023 then sales else 0 end) as sales_2023 from cte group by order_month order by order_month;"
    df_12 = pd.read_sql(q_12,conn)
    df_12
    conn.close()

def func_13():  
    st.title("Retail Order Analysis")
    q_13 = "with cte as (select category,to_char(order_date::DATE,'Mon, YYYY') as order_year_month, sum(sale_price) as sales from df_orders group by category,order_year_month)select * from (select *,row_number() over(partition by category order by sales desc) as rn from cte) a where rn=1;"
    df_13 = pd.read_sql(q_13,conn)
    df_13
    conn.close()

def func_14():  
    st.title("Retail Order Analysis")
    q_14 = "with cte as (select sub_category,extract(year from order_date) as order_year, sum(sale_price) as sales from df_orders group by sub_category,order_year), cte2 as (select sub_category, sum(case when order_year=2022 then sales else 0 end) as sales_2022, sum(case when order_year=2023 then sales else 0 end) as sales_2023 from cte group by sub_category) select * , (sales_2023-sales_2022) as growth from cte2 order by (sales_2023-sales_2022) desc limit 1;"
    df_14 = pd.read_sql(q_14,conn)
    df_14
    conn.close()

def func_15():  
    st.title("Retail Order Analysis")  
    q_15 = "SELECT state , sum(profit) as Highest_profit FROM df_orders_1 FULL JOIN df_orders_2 ON df_orders_1.order_id = df_orders_2.order_id_2 GROUP BY state ORDER BY Highest_profit desc LIMIT 5;"
    df_15 = pd.read_sql(q_15,conn)
    df_15
    conn.close()

def func_16():  
    st.title("Retail Order Analysis")  
    q_16 = "SELECT state , sum(sale_price) as Highest_sales FROM df_orders_1 FULL JOIN df_orders_2 ON df_orders_1.order_id = df_orders_2.order_id_2 GROUP BY state ORDER BY Highest_sales desc LIMIT 10;"
    df_16 = pd.read_sql(q_16,conn)
    df_16
    conn.close()

def func_17():  
    st.title("Retail Order Analysis")  
    q_17 = "SELECT region , sum(order_id) as Highest_orders FROM df_orders_1 FULL JOIN df_orders_2 ON df_orders_1.order_id = df_orders_2.order_id_2 GROUP BY region ORDER BY Highest_orders desc LIMIT 10;"
    df_17 = pd.read_sql(q_17,conn)
    df_17
    conn.close()

def func_18():  
    st.title("Retail Order Analysis")  
    q_18 = "SELECT city , sum(sale_price) as Highest_sales FROM df_orders_1 FULL JOIN df_orders_2 ON df_orders_1.order_id = df_orders_2.order_id_2 GROUP BY city ORDER BY Highest_sales desc LIMIT 10;"
    df_18 = pd.read_sql(q_18,conn)
    df_18
    conn.close()

def func_19():  
    st.title("Retail Order Analysis")  
    q_19 = "SELECT state, avg(profit) as average_profit FROM df_orders_1 FULL JOIN df_orders_2 ON df_orders_1.order_id = df_orders_2.order_id_2 GROUP BY state ORDER BY average_profit desc;"
    df_19 = pd.read_sql(q_19,conn)
    df_19
    conn.close()

def func_20():  
    st.title("Retail Order Analysis")  
    q_20 = "SELECT state, avg(sale_price) as average_sale_price FROM df_orders_1 FULL JOIN df_orders_2 ON df_orders_1.order_id = df_orders_2.order_id_2 GROUP BY state ORDER BY average_sale_price desc;"
    df_20 = pd.read_sql(q_20,conn)
    df_20
    conn.close()

cb_1 = st.sidebar.button('1.Find top 10 highest revenue generating products', on_click=func_1, key='Function_1')
cb_2 = st.sidebar.button('2.Find the top 5 cities with the highest profit margins', on_click=func_2, key='Function_2')
cb_3 = st.sidebar.button('3.Calculate the total discount given for each category', on_click=func_3, key='Function_3')
cb_4 = st.sidebar.button('4.Find the average sale price per product category', on_click=func_4, key='Function_4')
cb_5 = st.sidebar.button('5.Find the region with the highest average sale price', on_click=func_5, key='Function_5')
cb_6 = st.sidebar.button('6.Find the total profit per category', on_click=func_6, key='Function_6')
cb_7 = st.sidebar.button('7.Identify the top 3 segments with the highest quantity of orders', on_click=func_7, key='Function_7')
cb_8 = st.sidebar.button('8.Determine the average discount percentage given per region', on_click=func_8, key='Function_8')
cb_9 = st.sidebar.button('9.Find the product category with the highest total profit', on_click=func_9, key='Function_9')
cb_10 = st.sidebar.button('10.Calculate the total revenue generated per year', on_click=func_10, key='Function_10')
cb_11 = st.sidebar.button('11.Find top 5 highest selling products in each region ', on_click=func_11, key='Function_11')
cb_12 = st.sidebar.button('12.Find month over month growth comparison for 2022 and 2023 sales eg : jan 2022 vs jan 2023', on_click=func_12, key='Function_12')
cb_13 = st.sidebar.button('13.For each category which month had highest sales', on_click=func_13, key='Function_13')
cb_14 = st.sidebar.button('14.which sub category had highest growth by profit in 2023 compare to 2022', on_click=func_14, key='Function_14')
cb_15 = st.sidebar.button('15.Find the top 5 state with the highest profit margins', on_click=func_15, key='Function_15')
cb_16 = st.sidebar.button('16.Find the top states with the highest sales from top to bottom', on_click=func_16, key='Function_16')
cb_17 = st.sidebar.button('17.Identify the regions  with the highest quantity of orders from top to bottom', on_click=func_17, key='Function_17')
cb_18 = st.sidebar.button('18.Find the top cities with the highest sales from top to bottom', on_click=func_18, key='Function_18')
cb_19 = st.sidebar.button('19.Determine the average profit given per states', on_click=func_19, key='Function_19')
cb_20 = st.sidebar.button('20.Determine the average sales given per states', on_click=func_20, key='Function_20')