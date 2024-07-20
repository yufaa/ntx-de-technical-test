SELECT * FROM "testDE";

-- Test Case 1: Total revenue each channel grouping for the top 5 countries
-- 5 besar negara dengan revenue tertinggi -> hitung total revenue setiap channel grouping
-- Solusi 
-- Query dibawah menampilkan kolom country  dari tabel "testDE", yang telah dijumlahkan kolom "totalTransactionRevenue"
-- untuk setiap negara dan diurutkan dari yang paling besar total nilainya
WITH TopCountries AS (
    SELECT 
		country
    FROM 
		"testDE"
    WHERE 
		"testDE"."totalTransactionRevenue" IS NOT NULL
    GROUP BY 
		country
    ORDER BY 
		SUM("testDE"."totalTransactionRevenue") DESC
    LIMIT 5
) 

-- setelah didapatkan 5 negara dengan total revenue tertinggi. lakukan query untuk menampilkannya berdasarkan channelGrouping.
SELECT 
    country,
    "testDE"."channelGrouping",
    SUM("testDE"."totalTransactionRevenue") AS total_revenue
FROM 
    "testDE"
WHERE 
    country IN (
    	SELECT 
			country 
		FROM 
			TopCountries
	)   
GROUP BY 
    country, "testDE"."channelGrouping"
ORDER BY 
    country, total_revenue DESC;


-- Test Case 2: Hitung rata-rata timeonsite, pageview, and sessionqualitydim for each visitor. 
-- 				identifikasi siapa yang memiliki rata-rata timeonsite diatas rata-rata tetapi melihat halaman lebih sedikit dari rata-rata

-- jumlahkan timeonsite, pageviews, and sessionsqualitydim untuk setiap usernya untuk perhitungan rata-rata setiap user
WITH UserMetrics AS (
    SELECT
        "testDE"."fullVisitorId" AS fullVisitorId,
        SUM("testDE"."timeOnSite") AS total_timeOnSite,
        SUM("testDE"."pageviews") AS total_pageviews,
        SUM("testDE"."sessionQualityDim") AS total_sessionQualityDim
    FROM
        "testDE"
    GROUP BY
        "testDE"."fullVisitorId"
),


-- setelah didapatkan jumlah 3 kolom(timeonsite, pageviews, and sessionsqualitydim) hitung rata ratanya
-- kemudian tampilkan data pengguna dengan rata-rata timeOnSite diatas rata-rata dan rata-rata pageviews dibawah rata-rata
avg_metrics AS (
    SELECT
        AVG(UserMetrics.total_timeOnSite) AS avg_timeOnSite,
        AVG(UserMetrics.total_pageviews) AS avg_pageviews,
        AVG(UserMetrics.total_sessionQualityDim) AS avg_sessionQualityDim
    FROM
        UserMetrics
)
SELECT
    UserMetrics.fullVisitorId,
    UserMetrics.total_timeOnSite,
    UserMetrics.total_pageviews,
    UserMetrics.total_sessionQualityDim
FROM
    UserMetrics,
    avg_metrics
WHERE
    UserMetrics.total_timeOnSite > avg_metrics.avg_timeOnSite
    AND UserMetrics.total_pageviews < avg_metrics.avg_pageviews;




-- Test Case 3
-- Total Revenue per Product
SELECT
	"testDE"."v2ProductName", 
	SUM("testDE"."totalTransactionRevenue") AS total_revenue
FROM
	"testDE"
GROUP BY 
	"testDE"."v2ProductName"
ORDER BY 
	total_revenue;


-- Total Quantity Sold per Product
SELECT
	"testDE"."v2ProductName", 
	SUM("testDE"."productQuantity") AS total_sold
FROM
	"testDE"
GROUP BY 
	"testDE"."v2ProductName"
ORDER BY 
	total_sold;


-- Total Refund Amount per Product
-- all value null tidak bisa diselesaikan
SELECT
	"testDE"."v2ProductName", 
	SUM("testDE"."productRefundAmount") AS total_refund
FROM
	"testDE"
GROUP BY 
	"testDE"."v2ProductName"
ORDER BY 
	total_refund;

-- dikarenakan tidak ada data refund maka net revenue hanya dihitung dari total revenue perproduct
SELECT
	"testDE"."v2ProductName", 
	SUM("testDE"."totalTransactionRevenue") AS total_revenue
FROM
	"testDE"
GROUP BY 
	"testDE"."v2ProductName"
ORDER BY 
	total_revenue DESC;



