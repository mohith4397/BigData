--1. Display customers grouped by profession.
SELECT custno,firstname,profession  FROM customer GROUP BY profession,custno,firstname;

--2. Find the total count of customers who do not have any profession.
SELECT custno,firstname FROM customer WHERE profession=null;

--3. Find out the top ten buyers from the sales data along with their personal details
SELECT custno,SUM(amount) AS totalsales FROM txnrecords GROUP BY custno ORDER BY totalsales DESC LIMIT 10;

--4. Find total sales of each type.
SELECT custno,SUM(amount) AS totalsales FROM txnrecords GROUP BY spendby ORDER BY totalsales DESC LIMIT 10;

--5. Find the total sales of each type.(cash/credit)
SELECT spendby,SUM(amount) FROM txnrecords GROUP BY spendby;

		-------<<NYSE>>-------

--7. Find the total stock volume for each stock.
SELECT stock_symbol,COUNT(stock_volume) FROM nyse GROUP BY stock_symbol;

		-------<<MARGIN>>-------
--8. Total net profit.
SELECT productid,COUNT(profit) FROM chinese WHERE profit > 0 GROUP BY productid ;

--10. Total volume of business for those products whose margin is more than 10%.(QUANTITY)
 select productid,quantity from chinese where margin>10;

--11. Find gross profit.
SELECT COUNT(profit) FROM chinese WHERE profit>0;

--12. Find gross loss.
SELECT COUNT(profit) FROM chinese WHERE profit<0;

--13. Find (gross loss/gross profit *100)% should be as less as possible. 
(SELECT COUNT(profit) FROM chinese WHERE profit<0) / (SELECT COUNT(profit) FROM chinese WHERE profit<0) ;
