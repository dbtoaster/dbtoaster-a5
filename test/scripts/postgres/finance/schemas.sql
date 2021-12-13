DROP SCHEMA "FINANCE_@@DATASET@@" CASCADE;

CREATE SCHEMA "FINANCE_@@DATASET@@"
  AUTHORIZATION postgres;

CREATE TABLE IF NOT EXISTS "FINANCE_@@DATASET@@".ASKS (
  T         float, 
  ID        int, 
  BROKER_ID int, 
  VOLUME    float, 
  PRICE     float
);

CREATE TABLE IF NOT EXISTS "FINANCE_@@DATASET@@".BIDS (
  T         float, 
  ID        int, 
  BROKER_ID int, 
  VOLUME    float,
  PRICE     float
);


DELETE FROM "FINANCE_@@DATASET@@".ASKS;
DELETE FROM "FINANCE_@@DATASET@@".BIDS;

COPY "FINANCE_@@DATASET@@".ASKS FROM '@@DBT_DIR@@/../dbtoaster-experiments-data/finance/@@DATASET@@/asks_export.csv' WITH DELIMITER AS ','; 
COPY "FINANCE_@@DATASET@@".BIDS FROM '@@DBT_DIR@@/../dbtoaster-experiments-data/finance/@@DATASET@@/bids_export.csv' WITH DELIMITER AS ','; 

