DROP SCHEMA "FINANCE_@@DATASET@@" CASCADE;

CREATE SCHEMA "FINANCE_@@DATASET@@"
  AUTHORIZATION postgres;

CREATE TABLE IF NOT EXISTS "FINANCE_@@DATASET@@".ASKS (
  T         float(53), 
  ID        int, 
  BROKER_ID int, 
  VOLUME    float(53), 
  PRICE     float(53)
);

CREATE TABLE IF NOT EXISTS "FINANCE_@@DATASET@@".BIDS (
  T         float(53), 
  ID        int, 
  BROKER_ID int, 
  VOLUME    float(53), 
  PRICE     float(53)
);


DELETE FROM "FINANCE_@@DATASET@@".ASKS;
DELETE FROM "FINANCE_@@DATASET@@".BIDS;

COPY "FINANCE_@@DATASET@@".ASKS FROM '@@DBT_DIR@@/../../experiments/data/finance/@@DATASET@@/asks_export.csv' WITH DELIMITER AS ','; 
COPY "FINANCE_@@DATASET@@".BIDS FROM '@@DBT_DIR@@/../../experiments/data/finance/@@DATASET@@/bids_export.csv' WITH DELIMITER AS ','; 

