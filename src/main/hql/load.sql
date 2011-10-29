-- Simple command to load data into the tables, it should be modified to
-- use a specific SERDE object
LOAD DATA LOCAL INPATH '/Users/hadoophive/Desktop/OUT/out*.csv' OVERWRITE INTO TABLE npl_normalized_log;
