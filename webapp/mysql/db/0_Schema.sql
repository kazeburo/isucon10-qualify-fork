DROP DATABASE IF EXISTS isuumo;
CREATE DATABASE isuumo;

DROP TABLE IF EXISTS isuumo.estate;
DROP TABLE IF EXISTS isuumo.chair;

CREATE TABLE isuumo.estate
(
    id          INTEGER             NOT NULL PRIMARY KEY,
    name        VARCHAR(64)         NOT NULL,
    description VARCHAR(4096)       NOT NULL,
    thumbnail   VARCHAR(128)        NOT NULL,
    address     VARCHAR(128)        NOT NULL,
    latitude    DOUBLE PRECISION    NOT NULL,
    longitude   DOUBLE PRECISION    NOT NULL,
    rent        INTEGER             NOT NULL,
    door_height INTEGER             NOT NULL,
    door_width  INTEGER             NOT NULL,
    features    VARCHAR(64)         NOT NULL,
    popularity  INTEGER             NOT NULL,
    point       POINT AS (POINT(latitude, longitude)) STORED SRID 0 NOT NULL,
    rent_range        INTEGER AS (IF(rent < 50000, 0, IF(rent < 100000, 1, IF(rent < 150000, 2,3)))) NOT NULL,
    door_height_range INTEGER AS (IF(door_height < 80, 0, IF(door_height < 110, 1, IF(door_height < 150, 2, 3)))) NOT NULL,
    door_width_range  INTEGER AS (IF(door_width < 80, 0, IF(door_width < 110, 1, IF(door_width < 150, 2, 3)))) NOT NULL
) ROW_FORMAT=DYNAMIC;

CREATE TABLE isuumo.chair
(
    id          INTEGER         NOT NULL PRIMARY KEY,
    name        VARCHAR(64)     NOT NULL,
    description VARCHAR(4096)   NOT NULL,
    thumbnail   VARCHAR(128)    NOT NULL,
    price       INTEGER         NOT NULL,
    height      INTEGER         NOT NULL,
    width       INTEGER         NOT NULL,
    depth       INTEGER         NOT NULL,
    color       VARCHAR(64)     NOT NULL,
    features    VARCHAR(64)     NOT NULL,
    kind        VARCHAR(64)     NOT NULL,
    popularity  INTEGER         NOT NULL,
    stock       INTEGER         NOT NULL
);

CREATE TABLE isuumo.chair_stock
(
    id          INTEGER         NOT NULL PRIMARY KEY,
    price       INTEGER         NOT NULL,
    height      INTEGER         NOT NULL,
    width       INTEGER         NOT NULL,
    depth       INTEGER         NOT NULL,
    color       VARCHAR(64)     NOT NULL,
    features    VARCHAR(64)     NOT NULL,
    kind        VARCHAR(64)     NOT NULL,
    popularity  INTEGER         NOT NULL,
    price_range INTEGER AS (IF(price < 3000,0,IF(price < 6000, 1, IF(price < 9000, 2, IF(price < 12000, 3, IF(price < 15000, 4, 5))))))  NOT NULL,
    height_range INTEGER AS (IF(height < 80, 0, IF(height < 110, 1, IF(height < 150, 2, 3))))  NOT NULL,
    width_range INTEGER AS (IF(width < 80, 0, IF(width < 110, 1, IF(width < 150, 2, 3))))  NOT NULL,
    depth_range INTEGER AS (IF(depth < 80, 0, IF(depth < 110, 1, IF(depth < 150, 2, 3))))  NOT NULL,
    kind_range        INTEGER AS (IF(kind = "ゲーミングチェア", 0, IF(kind = "座椅子", 1, IF(kind = "エルゴノミクス", 2, 3)))) NOT NULL,
    color_range       INTEGER AS (IF(color =  "黒", 0, IF(color = "白", 1, IF(color = "赤", 2, IF(color = "青", 3, IF(color = "緑", 4, IF(color = "黄", 5, IF(color = "紫", 6, IF(color = "ピンク", 7, IF(color = "オレンジ", 8, IF(color = "水色", 9, IF(color = "ネイビー", 10, 11)))))))))))) NOT NULL
) ROW_FORMAT=DYNAMIC;

create index idx_pop on isuumo.estate(popularity desc);
create index idx_rent on isuumo.estate(rent asc);
create index idx_rent_range on isuumo.estate(rent_range, popularity desc);
create index idx_door_hei_r on isuumo.estate(door_height_range, rent_range, popularity desc);
create index idx_door_wid_r on isuumo.estate(door_width_range, rent_range, popularity desc);
create index idx_door_widhei_r on isuumo.estate(door_width_range, door_height_range, rent_range, popularity desc);

ALTER TABLE isuumo.estate ADD SPATIAL INDEX idx_point(point);

create index idx_pop on isuumo.chair_stock(popularity desc);
create index idx_price on isuumo.chair_stock(price asc);
create index idx_price_range on isuumo.chair_stock(price_range, popularity desc);
create index idx_kind on isuumo.chair_stock(kind_range, price_range, popularity desc);
create index idx_color on isuumo.chair_stock(color_range, price_range, popularity desc);
create index idx_kind_h on isuumo.chair_stock(kind_range, height_range, popularity desc);
create index idx_color_h on isuumo.chair_stock(color_range, height_range, popularity desc);
create index idx_kolor on isuumo.chair_stock(color_range, kind_range, popularity desc);
create index idx_height on isuumo.chair_stock(height_range, price_range, popularity desc);
create index idx_width on isuumo.chair_stock(width_range, price_range,popularity desc);
create index idx_depth on isuumo.chair_stock(depth_range, price_range, popularity desc);
create index idx_widhei on isuumo.chair_stock(width_range, height_range, popularity desc);

DELIMITER $$
CREATE
    TRIGGER isuumo.insert_chair
    AFTER INSERT
    ON isuumo.chair
    FOR EACH ROW
    BEGIN
    INSERT INTO isuumo.chair_stock(id, price, height, width, depth, color, features, kind, popularity) VALUES (
        NEW.id,
        NEW.price,
        NEW.height,
        NEW.width,
        NEW.depth,
        NEW.color,
        NEW.features,
        NEW.kind,
        NEW.popularity);
    END $$
DELIMITER ;

DELIMITER $$
CREATE
    TRIGGER isuumo.update_chair
    AFTER UPDATE
    ON isuumo.chair
    FOR EACH ROW
    BEGIN
    IF (NEW.stock = 0) THEN
      DELETE FROM isuumo.chair_stock WHERE id=NEW.id;
    END IF;
    END $$
DELIMITER ;
