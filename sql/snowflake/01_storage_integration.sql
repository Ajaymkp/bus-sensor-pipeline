-- Database & Schema

CREATE DATABASE IF NOT EXISTS bus_censor_db;
USE DATABASE bus_censor_db;

CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS marts;

-- Replace YOUR_AWS_ACCOUNT_ID with your 12-digit AWS account ID
CREATE OR REPLACE STORAGE INTEGRATION s3_bus_censor_integration
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = S3
  ENABLED = TRUE
  STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::YOUR_AWS_ACCOUNT_ID:role/snowflake-s3-role'
  STORAGE_ALLOWED_LOCATIONS = ('s3://mkp-s3-data/bus-censor/');

DESC INTEGRATION s3_bus_censor_integration;