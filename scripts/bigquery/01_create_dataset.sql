-- BigQuery Dataset Creation Script
-- Run this script once to create the PRICING dataset
-- 
-- Usage:
--   bq mk --dataset --location=US pricing-email-bot:PRICING
-- Or run this SQL in BigQuery console:

-- Create the PRICING dataset if it doesn't exist
CREATE SCHEMA IF NOT EXISTS `pricing-email-bot.PRICING`
OPTIONS (
  location = 'asia-south1'
);
