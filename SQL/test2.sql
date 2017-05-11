#说明：从最后5天中挑选有过浏览详情、加购物车、关注行为的用户及商品，
#      且当用户对多个商品有上述行为时，挑一个有上述行为最多的商品，
#      并排除了已经删购物车或已经下单的用户，
#      筛选出来的数据仍可能有一个用户对应多个商品，使用excel删除重复简单粗暴
#      mysql没有实现差集（MINUS），所以语句比较麻烦
#      可能会运行较长时间！！

CREATE VIEW all_set AS
SELECT user_id, sku_id, COUNT(*) AS count_actions
FROM actions 
WHERE time >= 2016-04-11 AND cate = 8 AND (type=1 OR type=2 OR type=5)
GROUP BY user_id, sku_id
HAVING count_actions >= 2;

CREATE VIEW all_set1 AS
SELECT a1.user_id, a1.sku_id
FROM all_set AS a1
WHERE a1.count_actions = (SELECT MAX(count_actions)
                          FROM all_set
                          WHERE a1.user_id = user_id);

CREATE VIEW exception AS
SELECT user_id, sku_id
FROM actions
WHERE time >= 2016-04-11 AND cate = 8 AND (type=2 OR type=4)
GROUP BY user_id, sku_id;

SELECT all_set1.user_id, all_set1.sku_id
FROM all_set1 LEFT JOIN exception
ON all_set1.user_id = exception.user_id
WHERE all_set1.sku_id != exception.sku_id
INTO OUTFILE 'C:/JData/data/test2.csv'
FIELDS TERMINATED BY ','
OPTIONALLY ENCLOSED BY ''
LINES TERMINATED BY '\n';

DROP VIEW all_set;
DROP VIEW all_set1;
DROP VIEW exception;