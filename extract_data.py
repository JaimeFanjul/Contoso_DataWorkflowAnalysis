import pandas as pd
import psycopg2
import time

conn = psycopg2.connect(user = "YOUR USER NAME",
                            password = "YOUR PASSWORD",
                            host = "EXAMPLE: ec2-18-203-128-102.eu-west-1.compute.amazonaws.com",
                            port = "5432",
                            database = "EXAMPLE: gu6uc560t4pkc9")


# truncate records to last 1 year data
str_query = '''
SELECT DateKey,
ProductName,
ProductDescription,
ClassName,
UnitCost::numeric::real,
UnitPrice::numeric::real,
ProductCategory,
ProductSubcategory,
SalesQuantity,
ReturnQuantity,
PromotionName,
DiscountPercent,
ChannelName,
StoreName,
CASE
  WHEN EmployeeCount  < 25  THEN 'Small'
  WHEN EmployeeCount  < 50 THEN 'Medium'
  ELSE 'Big'
END AS StoreSize,
GeographyType,
ContinentName AS Region,
RegionCountryName AS Country
FROM sales s
JOIN product p ON s.ProductKey = p.ProductKey
JOIN product_subcategory ps ON p.ProductSubcategoryKey = ps.ProductSubcategoryKey
JOIN product_category pc ON pc.ProductCategoryKey = ps.ProductCategoryKey
JOIN stores st ON s.StoreKey = st.StoreKey
JOIN channel c ON s.ChannelKey = c.ChannelKey
JOIN promotion pr ON s.PromotionKey = pr.PromotionKey
JOIN geography geo ON geo.GeographyKey = st.GeographyKey
WHERE s.DateKey >= '2013-01-01'
AND st.Status = 'On'
'''
df_data = pd.read_sql(str_query, conn)

df_data.to_csv('data_contoso.csv.gz', index=False, compression='gzip')

# Get the reviews data
str_query = '''
SELECT StoreName,
StoreType,
stars,
text
FROM stores st
JOIN reviews_yelp r ON st.StoreKey = r.StoreKey
WHERE st.Status = 'On'
'''
df_reviews = pd.read_sql(str_query, conn)
