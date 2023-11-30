data = LOAD 'pig-5k.csv' USING PigStorage(',') AS (CRASH_DATE,CRASH_TIME,BOROUGH,ZIP_CODE,LATITUDE,LONGITUDE,LOCATION,ON_STREET_NAME,CROSS_STREET_NAME,OFF_STREET_NAME,PERSONS_INJURED,PERSONS_KILLED,PEDESTRIANS_INJURED,PEDESTRIANS_KILLED,CYCLIST_INJURED,CYCLIST_KILLED,MOTORIST_INJURED,MOTORIST_KILLED,FACTOR_1,FACTOR_2,FACTOR_3,FACTOR_4,FACTOR_5,COLLISION_ID,VEHICLE_1,VEHICLE_2,VEHICLE_3,VEHICLE_4,VEHICLE_5);

------------------- TIME -------------------
time_col = FOREACH data GENERATE CRASH_DATE,CRASH_TIME,LATITUDE,LONGITUDE;
hour_data = FOREACH data GENERATE CRASH_DATE,CRASH_TIME,LATITUDE,LONGITUDE,SUBSTRING(CRASH_TIME, 0, 2) AS HOUR;


-- Each hour
hour_00 = FILTER hour_data BY HOUR == '00';
hour_01 = FILTER hour_data BY HOUR == '01';
hour_02 = FILTER hour_data BY HOUR == '02';
hour_03 = FILTER hour_data BY HOUR == '03';
hour_04 = FILTER hour_data BY HOUR == '04';
hour_05 = FILTER hour_data BY HOUR == '05';
hour_06 = FILTER hour_data BY HOUR == '06';
hour_07 = FILTER hour_data BY HOUR == '07';
hour_08 = FILTER hour_data BY HOUR == '08';
hour_09 = FILTER hour_data BY HOUR == '09';
hour_10 = FILTER hour_data BY HOUR == '10';
hour_11 = FILTER hour_data BY HOUR == '11';
hour_12 = FILTER hour_data BY HOUR == '12';
hour_13 = FILTER hour_data BY HOUR == '13';
hour_14 = FILTER hour_data BY HOUR == '14';
hour_15 = FILTER hour_data BY HOUR == '15';
hour_16 = FILTER hour_data BY HOUR == '16';
hour_17 = FILTER hour_data BY HOUR == '17';
hour_18 = FILTER hour_data BY HOUR == '18';
hour_19 = FILTER hour_data BY HOUR == '19';
hour_20 = FILTER hour_data BY HOUR == '20';
hour_21 = FILTER hour_data BY HOUR == '21';
hour_22 = FILTER hour_data BY HOUR == '22';
hour_23 = FILTER hour_data BY HOUR == '23';

STORE hour_00 INTO 'hour_00' USING PigStorage(',');
STORE hour_01 INTO 'hour_01' USING PigStorage(',');
STORE hour_02 INTO 'hour_02' USING PigStorage(',');
STORE hour_03 INTO 'hour_03' USING PigStorage(',');
STORE hour_04 INTO 'hour_04' USING PigStorage(',');
STORE hour_05 INTO 'hour_05' USING PigStorage(',');
STORE hour_06 INTO 'hour_06' USING PigStorage(',');
STORE hour_07 INTO 'hour_07' USING PigStorage(',');
STORE hour_08 INTO 'hour_08' USING PigStorage(',');
STORE hour_09 INTO 'hour_09' USING PigStorage(',');
STORE hour_10 INTO 'hour_10' USING PigStorage(',');
STORE hour_11 INTO 'hour_11' USING PigStorage(',');
STORE hour_12 INTO 'hour_12' USING PigStorage(',');
STORE hour_13 INTO 'hour_13' USING PigStorage(',');
STORE hour_14 INTO 'hour_14' USING PigStorage(',');
STORE hour_15 INTO 'hour_15' USING PigStorage(',');
STORE hour_16 INTO 'hour_16' USING PigStorage(',');
STORE hour_17 INTO 'hour_17' USING PigStorage(',');
STORE hour_18 INTO 'hour_18' USING PigStorage(',');
STORE hour_19 INTO 'hour_19' USING PigStorage(',');
STORE hour_20 INTO 'hour_20' USING PigStorage(',');
STORE hour_21 INTO 'hour_21' USING PigStorage(',');
STORE hour_22 INTO 'hour_22' USING PigStorage(',');
STORE hour_23 INTO 'hour_23' USING PigStorage(',');

/* Run command below to combine results into one csv file in local drive
hadoop fs -getmerge hour_00 hour00_merged.csv
hadoop fs -getmerge hour_01 hour01_merged.csv
hadoop fs -getmerge hour_02 hour02_merged.csv
hadoop fs -getmerge hour_03 hour03_merged.csv
hadoop fs -getmerge hour_04 hour04_merged.csv
hadoop fs -getmerge hour_05 hour05_merged.csv
hadoop fs -getmerge hour_06 hour06_merged.csv
hadoop fs -getmerge hour_07 hour07_merged.csv
hadoop fs -getmerge hour_08 hour08_merged.csv
hadoop fs -getmerge hour_09 hour09_merged.csv
hadoop fs -getmerge hour_10 hour10_merged.csv
hadoop fs -getmerge hour_11 hour11_merged.csv
hadoop fs -getmerge hour_12 hour12_merged.csv
hadoop fs -getmerge hour_13 hour13_merged.csv
hadoop fs -getmerge hour_14 hour14_merged.csv
hadoop fs -getmerge hour_15 hour15_merged.csv
hadoop fs -getmerge hour_16 hour16_merged.csv
hadoop fs -getmerge hour_17 hour17_merged.csv
hadoop fs -getmerge hour_18 hour18_merged.csv
hadoop fs -getmerge hour_19 hour19_merged.csv
hadoop fs -getmerge hour_20 hour20_merged.csv
hadoop fs -getmerge hour_21 hour21_merged.csv
hadoop fs -getmerge hour_22 hour22_merged.csv
hadoop fs -getmerge hour_23 hour23_merged.csv
*/

-- Every 6 hours
midnight = FILTER hour_data BY HOUR >= '00' and HOUR <= '05';
morning = FILTER hour_data BY HOUR >= '06' and HOUR <= '11';
afternoon = FILTER hour_data BY HOUR >= '12' and HOUR <= '17';
night = FILTER hour_data BY HOUR >= '18' and HOUR <= '23';

STORE midnight INTO 'midnight' USING PigStorage(',');
STORE morning INTO 'morning' USING PigStorage(',');
STORE afternoon INTO 'afternoon' USING PigStorage(',');
STORE night INTO 'night' USING PigStorage(',');

/* Run command below to combine results into one csv file in local drive
hadoop fs -getmerge midnight midnight_merged.csv
hadoop fs -getmerge morning morning_merged.csv
hadoop fs -getmerge afternoon afternoon_merged.csv
hadoop fs -getmerge night night_merged.csv
*/


-- Every 3 hours
hour00_02 = FILTER hour_data BY HOUR >= '00' and HOUR <= '02';
hour03_05 = FILTER hour_data BY HOUR >= '03' and HOUR <= '05';
hour06_08 = FILTER hour_data BY HOUR >= '06' and HOUR <= '08';
hour09_11 = FILTER hour_data BY HOUR >= '09' and HOUR <= '11';
hour12_14 = FILTER hour_data BY HOUR >= '12' and HOUR <= '14';
hour15_17 = FILTER hour_data BY HOUR >= '15' and HOUR <= '17';
hour18_20 = FILTER hour_data BY HOUR >= '18' and HOUR <= '20';
hour21_23 = FILTER hour_data BY HOUR >= '21' and HOUR <= '23';

STORE hour00_02 INTO 'hour00_02' USING PigStorage(',');
STORE hour03_05 INTO 'hour03_05' USING PigStorage(',');
STORE hour06_08 INTO 'hour06_08' USING PigStorage(',');
STORE hour09_11 INTO 'hour09_11' USING PigStorage(',');
STORE hour12_14 INTO 'hour12_14' USING PigStorage(',');
STORE hour15_17 INTO 'hour15_17' USING PigStorage(',');
STORE hour18_20 INTO 'hour18_20' USING PigStorage(',');
STORE hour21_23 INTO 'hour21_23' USING PigStorage(',');

/* Run command below to combine results into one csv file in local drive
hadoop fs -getmerge hour00_02 hour00_02_merged.csv
hadoop fs -getmerge hour03_05 hour03_05_merged.csv
hadoop fs -getmerge hour06_08 hour06_08_merged.csv
hadoop fs -getmerge hour09_11 hour09_11_merged.csv
hadoop fs -getmerge hour12_14 hour12_14_merged.csv
hadoop fs -getmerge hour15_17 hour15_17_merged.csv
hadoop fs -getmerge hour18_20 hour18_20_merged.csv
hadoop fs -getmerge hour21_23 hour21_23_merged.csv
*/


------------------- BOROUGH -------------------
borough_col = FOREACH data GENERATE BOROUGH,LATITUDE,LONGITUDE;
-- Filter the top 5 borough
BROOKLYN = FILTER borough_col BY BOROUGH == 'BROOKLYN';
QUEENS = FILTER borough_col BY BOROUGH == 'QUEENS';
MANHATTAN = FILTER borough_col BY BOROUGH == 'MANHATTAN';
BRONX = FILTER borough_col BY BOROUGH == 'BRONX';
STATEN = FILTER borough_col BY BOROUGH == 'STATEN ISLAND';

STORE BROOKLYN INTO 'BROOKLYN' USING PigStorage(',');
STORE QUEENS INTO 'QUEENS' USING PigStorage(',');
STORE MANHATTAN INTO 'MANHATTAN' USING PigStorage(',');
STORE BRONX INTO 'BRONX' USING PigStorage(',');
STORE STATEN INTO 'STATEN' USING PigStorage(',');

/* Run command below to combine results into one csv file in local drive
hadoop fs -getmerge BROOKLYN BROOKLYN_merged.csv
hadoop fs -getmerge QUEENS QUEENS_merged.csv
hadoop fs -getmerge MANHATTAN MANHATTAN_merged.csv
hadoop fs -getmerge BRONX BRONX_merged.csv
hadoop fs -getmerge STATEN STATEN_merged.csv
*/


------------------- CASUALTIES -------------------
-- ALL Casualties
human_col = FOREACH data GENERATE LATITUDE,LONGITUDE,PERSONS_INJURED,PERSONS_KILLED,PEDESTRIANS_INJURED,PEDESTRIANS_KILLED,CYCLIST_INJURED,CYCLIST_KILLED,MOTORIST_INJURED,MOTORIST_KILLED;
human_data = FILTER human_col BY PERSONS_INJURED > 0 OR PERSONS_KILLED > 0 OR PEDESTRIANS_INJURED > 0 OR PEDESTRIANS_KILLED > 0 OR CYCLIST_INJURED > 0 OR CYCLIST_KILLED > 0 OR MOTORIST_INJURED > 0 OR MOTORIST_KILLED > 0;
STORE human_data INTO 'human' USING PigStorage(',');
-- ALL Killed
kill_col = FOREACH data GENERATE LATITUDE,LONGITUDE,PERSONS_KILLED,PEDESTRIANS_KILLED,CYCLIST_KILLED,MOTORIST_KILLED;
kill_data = FILTER kill_col BY PERSONS_KILLED > 0 OR PEDESTRIANS_KILLED > 0 OR CYCLIST_KILLED > 0 OR MOTORIST_KILLED > 0;
STORE kill_data INTO 'kill' USING PigStorage(',');
-- ALL Injured
injure_col = FOREACH data GENERATE LATITUDE,LONGITUDE,PERSONS_INJURED,PEDESTRIANS_INJURED,CYCLIST_INJURED,MOTORIST_INJURED;
injure_data = FILTER injure_col BY PERSONS_INJURED > 0 OR PEDESTRIANS_INJURED > 0 OR CYCLIST_INJURED > 0 OR MOTORIST_INJURED > 0;
STORE injure_data INTO 'injure' USING PigStorage(',');

/* Run command below to combine results into one csv file in local drive
hadoop fs -getmerge human human_merged.csv
hadoop fs -getmerge kill kill_merged.csv
hadoop fs -getmerge injure injure_merged.csv
*/


------------------- VENICLE TYPE -------------------
type_col = FOREACH data GENERATE LATITUDE,LONGITUDE,VEHICLE_1,VEHICLE_2,VEHICLE_3,VEHICLE_4,VEHICLE_5;

sedan_data = FILTER type_col BY VEHICLE_1 MATCHES '(?i).*\\b(sedan)\\b.*' OR VEHICLE_2 MATCHES '(?i).*\\b(sedan)\\b.*' OR VEHICLE_3 MATCHES '(?i).*\\b(sedan)\\b.*' OR VEHICLE_4 MATCHES '(?i).*\\b(sedan)\\b.*' OR VEHICLE_5 MATCHES '(?i).*\\b(sedan)\\b.*';
station_data = FILTER type_col BY VEHICLE_1 MATCHES '(?i).*station.*' OR VEHICLE_2 MATCHES '(?i).*station.*' OR VEHICLE_3 MATCHES '(?i).*station.*' OR VEHICLE_4 MATCHES '(?i).*station.*' OR VEHICLE_5 MATCHES '(?i).*station.*';
passenger_data = FILTER type_col BY VEHICLE_1 MATCHES '(?i).*passenger.*' OR VEHICLE_2 MATCHES '(?i).*passenger.*' OR VEHICLE_3 MATCHES '(?i).*passenger.*' OR VEHICLE_4 MATCHES '(?i).*passenger.*' OR VEHICLE_5 MATCHES '(?i).*passenger.*';
taxi_data = FILTER type_col BY VEHICLE_1 MATCHES '(?i).*taxi.*' OR VEHICLE_2 MATCHES '(?i).*taxi.*' OR VEHICLE_3 MATCHES '(?i).*taxi.*' OR VEHICLE_4 MATCHES '(?i).*taxi.*' OR VEHICLE_5 MATCHES '(?i).*taxi.*';
dr_sedan_data = FILTER type_col BY VEHICLE_1 MATCHES '(?i).*4 dr sedan.*' OR VEHICLE_2 MATCHES '(?i).*4 dr sedan.*' OR VEHICLE_3 MATCHES '(?i).*4 dr sedan.*' OR VEHICLE_4 MATCHES '(?i).*4 dr sedan.*' OR VEHICLE_5 MATCHES '(?i).*4 dr sedan.*';
pickUp_data = FILTER type_col BY VEHICLE_1 MATCHES '(?i).*pick.*' OR VEHICLE_2 MATCHES '(?i).*pick.*' OR VEHICLE_3 MATCHES '(?i).*pick.*' OR VEHICLE_4 MATCHES '(?i).*pick.*' OR VEHICLE_5 MATCHES '(?i).*pick.*';
bike_data = FILTER type_col BY VEHICLE_1 MATCHES '(?i).*bike.*' OR VEHICLE_2 MATCHES '(?i).*bike.*' OR VEHICLE_3 MATCHES '(?i).*bike.*' OR VEHICLE_4 MATCHES '(?i).*bike.*' OR VEHICLE_5 MATCHES '(?i).*bike.*';
ebike_data = FILTER type_col BY VEHICLE_1 MATCHES '(?i)^e .*' OR VEHICLE_1 MATCHES '(?i)^e-.*' OR VEHICLE_1 MATCHES '(?i).*ebike.*' OR VEHICLE_2 MATCHES '(?i)^e .*' OR VEHICLE_2 MATCHES '(?i)^e-.*' OR VEHICLE_2 MATCHES '(?i).*ebike.*' OR VEHICLE_3 MATCHES '(?i)^e .*' OR VEHICLE_3 MATCHES '(?i)^e-.*' OR VEHICLE_3 MATCHES '(?i).*ebike.*' OR VEHICLE_4 MATCHES '(?i)^e .*' OR VEHICLE_4 MATCHES '(?i)^e-.*' OR VEHICLE_4 MATCHES '(?i).*ebike.*' OR VEHICLE_5 MATCHES '(?i)^e .*' OR VEHICLE_5 MATCHES '(?i)^e-.*' OR VEHICLE_5 MATCHES '(?i).*ebike.*';

STORE sedan_data  INTO 'sedan' USING PigStorage(',');
STORE station_data INTO 'station' USING PigStorage(',');
STORE passenger_data INTO 'passenger' USING PigStorage(',');
STORE taxi_data INTO 'taxi' USING PigStorage(',');
STORE dr_sedan_data INTO 'dr_sedan2' USING PigStorage(',');
STORE pickUp_data INTO 'pickUp' USING PigStorage(',');
STORE bike_data INTO 'bike' USING PigStorage(',');
STORE ebike_data INTO 'ebike' USING PigStorage(',');

/* Run command below to combine results into one csv file in local drive
hadoop fs -getmerge sedan sedan_merged.csv
hadoop fs -getmerge station station_merged.csv
hadoop fs -getmerge passenger passenger_merged.csv
hadoop fs -getmerge taxi taxi_merged.csv
hadoop fs -getmerge dr_sedan dr_sedan_merged.csv
hadoop fs -getmerge bike bike_merged.csv
hadoop fs -getmerge pickUp pickUp_merged.csv
hadoop fs -getmerge ebike ebike_merged.csv
*/
