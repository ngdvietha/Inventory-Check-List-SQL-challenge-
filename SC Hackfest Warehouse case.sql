-- Tạo bảng tạm xử lí các bản ghi có carton ids theo dạng khoảng
DROP TABLE IF EXISTS #QC_range
SELECT
container, 
floor_location_id,
CAST(SUBSTRING(carton_ids, 0, CHARINDEX('-',carton_ids)) AS int) min_carton_ids,
CAST(RIGHT(carton_ids, LEN(carton_ids) - CHARINDEX('-',carton_ids)) AS int) max_carton_ids into #QC_range
FROM QC 
WHERE CHARINDEX('-',carton_ids) <> 0

--Tạo bảng tạm xử lí các bản ghi có carton ids bị lỗi theo dạng khoảng
DROP TABLE IF EXISTS #QC_range_error
SELECT
container, 
floor_location_id,
CAST(SUBSTRING(non_conforming_carton_ids, 0, CHARINDEX('-',non_conforming_carton_ids)) AS int) min_carton_ids,
CAST(RIGHT(carton_ids, LEN(non_conforming_carton_ids) - CHARINDEX('-',non_conforming_carton_ids)) AS int) max_carton_ids into #QC_range_error
FROM QC 
WHERE CHARINDEX('-',non_conforming_carton_ids) <> 0


/*
Đệ quy để gen ra một dãy các carton ids từ range đã có UNION ALL với các bản ghi có khai báo carton ids đơn lẻ 
ngăn cách nhau bằng dấu phẩy
*/
DROP TABLE IF EXISTS #Carton_QC
GO
WITH QC_range_final AS(
SELECT 
container,
floor_location_id,
min_carton_ids,
min_carton_ids + 1 next_carton_ids,
max_carton_ids
FROM #QC_range
UNION ALL
SELECT
container,
floor_location_id,
next_carton_ids,
next_carton_ids + 1,
max_carton_ids
FROM QC_range_final
WHERE next_carton_ids <= max_carton_ids)

SELECT * INTO #Carton_QC FROM(
SELECT 
container, 
floor_location_id, 
value carton_ids
FROM QC
CROSS APPLY STRING_SPLIT(carton_ids,',')
WHERE CHARINDEX('-', value) = 0
UNION ALL
SELECT
container,
floor_location_id,
min_carton_ids carton_ids
FROM QC_range_final) a
ORDER BY container, carton_ids

--Tương tự đệ quy để gen ra bảng các carton ids bị lỗi
DROP TABLE IF EXISTS #Carton_error
GO
WITH QC_range_final_error AS(
SELECT 
container,
floor_location_id,
min_carton_ids,
min_carton_ids + 1 next_carton_ids,
max_carton_ids
FROM #QC_range_error
UNION ALL
SELECT
container,
floor_location_id,
next_carton_ids,
next_carton_ids + 1,
max_carton_ids
FROM QC_range_final_error
WHERE next_carton_ids <= max_carton_ids)

SELECT * into #Carton_error
FROM(
SELECT 
container, 
floor_location_id, 
value non_conforming_carton_ids
FROM QC
CROSS APPLY STRING_SPLIT(non_conforming_carton_ids,',')
WHERE CHARINDEX('-', value) = 0
UNION ALL
SELECT
container,
floor_location_id,
min_carton_ids carton_ids
FROM QC_range_final_error
) a
ORDER BY container, non_conforming_carton_ids


--Đệ quy để ra bảng đếm số lượng thực carton đã được biết trước từ nhà cung cấp
DROP TABLE IF EXISTS #Carton_count
GO
WITH carton_qty_range AS(
SELECT
container,
1 min_carton_qty,
1 + 1 next_carton_qty,
total_carton_qty max_carton_qty
FROM Packing
UNION ALL
SELECT 
container,
next_carton_qty,
next_carton_qty + 1,
max_carton_qty
FROM carton_qty_range
WHERE next_carton_qty <= max_carton_qty)

SELECT 
container,
min_carton_qty carton_count into #Carton_count
FROM carton_qty_range ORDER BY container, carton_count

--Join cả 3 bảng tạm cùng nhau để ra kết quả cuối cùng
SELECT 
a.container,
c.floor_location_id,
a.carton_count carton_ids,
CASE
    WHEN b.non_conforming_carton_ids IS NOT NULL THEN 'non-conforming'
    WHEN c.carton_ids IS NULL THEN 'missing'
END note
FROM #Carton_count a
LEFT JOIN #Carton_error b ON a.carton_count = b.non_conforming_carton_ids AND a.container = b.container
LEFT JOIN #Carton_QC c ON a.carton_count = c.carton_ids AND a.container = c.container
ORDER BY a.container, c.floor_location_id, a.carton_count
