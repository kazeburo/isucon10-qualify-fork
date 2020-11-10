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
    rent_range        INTEGER AS (IF(rent < 50000, 0, IF(rent < 100000, 1, IF(rent < 150000, 2,3)))) NOT NULL,
    door_height_range INTEGER AS (IF(door_height < 80, 0, IF(door_height < 110, 1, IF(door_height < 150, 2, 3)))) NOT NULL,
    door_width_range  INTEGER AS (IF(door_width < 80, 0, IF(door_width < 110, 1, IF(door_width < 150, 2, 3)))) NOT NULL
) ROW_FORMAT=DYNAMIC;

create index idx_pop on isuumo.estate(popularity desc);
create index idx_rent on isuumo.estate(rent asc);
create index idx_rent_range on isuumo.estate(rent_range, popularity desc);
create index idx_door_hei_r on isuumo.estate(door_height_range, rent_range, popularity desc);
create index idx_door_wid_r on isuumo.estate(door_width_range, rent_range, popularity desc);
create index idx_door_widhei_r on isuumo.estate(door_width_range, door_height_range, rent_range, popularity desc);

CREATE TABLE isuumo.estate_search
(
    id          INTEGER             NOT NULL PRIMARY KEY,
    point       POINT  SRID 0 NOT NULL,
    door_long   INTEGER             NOT NULL,
    door_short  INTEGER             NOT NULL,
    popularity  INTEGER             NOT NULL
) ROW_FORMAT=DYNAMIC;

ALTER TABLE isuumo.estate_search ADD SPATIAL INDEX idx_point(point);
create index idx_pop on isuumo.estate_search(popularity desc);

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
    price_range       INTEGER         NOT NULL,
    height_range      INTEGER         NOT NULL,
    width_range       INTEGER         NOT NULL,
    depth_range       INTEGER         NOT NULL,
    color_range       INTEGER     NOT NULL,
    features    VARCHAR(64)     NOT NULL,
    kind_range        INTEGER     NOT NULL,
    popularity  INTEGER         NOT NULL
) ROW_FORMAT=DYNAMIC;


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
    TRIGGER isuumo.insert_estate
    AFTER INSERT
    ON isuumo.estate
    FOR EACH ROW
    BEGIN
    INSERT INTO isuumo.estate_search(
        id,
        point,
        door_long,
        door_short,
        popularity
    ) VALUES (
        NEW.id,
        POINT(NEW.latitude, NEW.longitude),
        GREATEST(NEW.door_height,NEW.door_width),
        LEAST(NEW.door_height,NEW.door_width),
        NEW.popularity
    );
    END $$
DELIMITER ;

DELIMITER $$
CREATE
    TRIGGER isuumo.insert_chair
    AFTER INSERT
    ON isuumo.chair
    FOR EACH ROW
    BEGIN
    INSERT INTO isuumo.chair_stock(
        id,
        price,
        price_range,
        height_range,
        width_range,
        depth_range,
        color_range,
        features,
        kind_range,
        popularity
    ) VALUES (
        NEW.id,
        NEW.price,
        IF(NEW.price < 3000,0,IF(NEW.price < 6000, 1, IF(NEW.price < 9000, 2, IF(NEW.price < 12000, 3, IF(NEW.price < 15000, 4, 5))))),
        IF(NEW.height < 80, 0, IF(NEW.height < 110, 1, IF(NEW.height < 150, 2, 3))),
        IF(NEW.width < 80, 0, IF(NEW.width < 110, 1, IF(NEW.width < 150, 2, 3))),
        IF(NEW.depth < 80, 0, IF(NEW.depth < 110, 1, IF(NEW.depth < 150, 2, 3))),
        IF(NEW.color =  "黒", 0, IF(NEW.color = "白", 1, IF(NEW.color = "赤", 2, IF(NEW.color = "青", 3, IF(NEW.color = "緑", 4, IF(NEW.color = "黄", 5, IF(NEW.color = "紫", 6, IF(NEW.color = "ピンク", 7, IF(NEW.color = "オレンジ", 8, IF(NEW.color = "水色", 9, IF(NEW.color = "ネイビー", 10, 11))))))))))),
        NEW.features,
        IF(NEW.kind = "ゲーミングチェア", 0, IF(NEW.kind = "座椅子", 1, IF(NEW.kind = "エルゴノミクス", 2, 3))),
        NEW.popularity
    );
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
