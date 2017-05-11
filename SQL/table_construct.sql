CREATE DATABASE JData;
use JData;


CREATE TABLE user(
    user_id INT NOT NULL,
    age_left TINYINT NOT NULL,
    age_right TINYINT NOT NULL,
    sex ENUM('0', '1', '2'),
    user_lv_cd ENUM('0', '1', '2', '3', '4', '5'),
    user_reg_dt DATE,
    PRIMARY KEY(user_id)
) CHARSET='utf8' ENGINE=INNODB;

load data local infile "D:/02/1-data/user.txt" into table user LINES TERMINATED BY '\r\n' (user_id, age_left, age_right, sex, user_lv_cd, user_reg_dt);



CREATE TABLE comment(
    id INT auto_increment NOT NULL,
    dt DATE,
    sku_id INT NOT NULL,
    comment_num TINYINT,
    has_bad_comment TINYINT,
    bad_comment_rate FLOAT,
    PRIMARY KEY(id,sku_id)
) CHARSET='utf8' ENGINE=INNODB;

load data local infile "D:/02/1-data/comment.txt" into table comment LINES TERMINATED BY '\r\n' (dt, sku_id, comment_num, has_bad_comment, bad_comment_rate);



CREATE TABLE product(
    sku_id INT NOT NULL,
    attr1 TINYINT NOT NULL,
    attr2 TINYINT NOT NULL,
    attr3 TINYINT NOT NULL,
    cate TINYINT NOT NULL,
    brand SMALLINT NOT NULL,
    PRIMARY KEY(sku_id)
) CHARSET='utf8' ENGINE=INNODB;

load data local infile "D:/02/1-data/product.txt" into table product LINES TERMINATED BY '\r\n' (sku_id, attr1, attr2, attr3, cate, brand);



CREATE TABLE actions(
    id INT auto_increment NOT NULL,
    user_id INT NOT NULL,
    sku_id INT NOT NULL,
    time DATETIME,
    model_id VARCHAR(10) NOT NULL DEFAULT '',
    type TINYINT,
    cate TINYINT,
    brand SMALLINT,
    PRIMARY KEY(id)
) CHARSET='utf8' ENGINE=INNODB;

load data local infile "D:/02/1-data/action.txt" into table actions LINES TERMINATED BY '\r\n' (user_id, sku_id, time, model_id, type, cate, brand);



