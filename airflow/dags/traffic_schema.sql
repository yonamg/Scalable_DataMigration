-- CREATE database IF NOT EXISTS pneuma;
-- use pneuma;
CREATE TABLE  IF NOT EXISTS traffic( 
    id INT PRIMARY KEY,
    track_id INT NOT NULL, 
    type varchar(50) NOT NULL, 
    traveled_d DOUBLE DEFAULT NULL,
    avg_speed DOUBLE DEFAULT NULL, 
    lat DOUBLE DEFAULT NULL, 
    lon DOUBLE DEFAULT NULL, 
    speed DOUBLE DEFAULT NULL,    
    lon_acc DOUBLE DEFAULT NULL, 
    lat_acc DOUBLE DEFAULT NULL, 
    time DOUBLE DEFAULT NULL
);