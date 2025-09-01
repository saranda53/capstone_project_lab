-- Capstone Project Governance setup
CREATE CATALOG IF NOT EXISTS capstone;
CREATE SCHEMA  IF NOT EXISTS capstone.bronze;
CREATE SCHEMA  IF NOT EXISTS capstone.silver;
CREATE SCHEMA  IF NOT EXISTS capstone.gold;
CREATE VOLUME  IF NOT EXISTS capstone.bronze.raw;
