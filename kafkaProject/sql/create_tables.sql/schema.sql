DROP TABLE IF EXISTS stations;
CREATE TABLE stations(
     id SERIAL PRIMARY KEY,
     location VARCHAR(255),
     station_id INT
);

DROP TABLE IF EXISTS results;
CREATE TABLE results (
     res_id SERIAL PRIMARY KEY,
     value INT
);

INSERT INTO stations (location, station_id) VALUES ('Lisboa', 1);
INSERT INTO stations (location, station_id) VALUES ('Porto', 1);
INSERT INTO stations (location, station_id) VALUES ('Leiria', 2);
INSERT INTO stations (location, station_id) VALUES ('Coimbra', 2);
INSERT INTO stations (location, station_id) VALUES ('Lisboa', 1);
INSERT INTO stations (location, station_id) VALUES ('Porto', 2);