-- Mirrors just enough of the production schema for the sync query to run.
-- Column names and types follow what index.js selects; anything the service
-- never reads is left out on purpose.

CREATE DATABASE IF NOT EXISTS analytics;

CREATE TABLE IF NOT EXISTS analytics.UserHistory
(
    UserID           UInt64,
    Login            String,
    Name             String,
    LastName         String,
    Email            String,
    Phone            String,
    PhoneVerified    UInt8,
    Gender           String,
    Language         String,
    CountryID        UInt32,
    City             String,
    Timezone         String,
    LastCreditDate   DateTime,
    RegistrationDate DateTime,
    LastLoginDate    DateTime,
    PEP              UInt8,
    Status           UInt8,
    -- argMax(..., RecordTime) picks the newest revision of every field
    RecordTime       DateTime,
    -- the table is partitioned on this in production, which is what lets the
    -- sync window skip partitions instead of reading everything
    RecordDate       Date,
    -- the sync window filters on this
    LastUpdated      DateTime,
    -- two generations of the same field, as in production: the Date32 column is
    -- authoritative but stores 1900-01-01 for "unknown", and the older string
    -- column still holds a real value for some users
    DateOfBirth      Nullable(String),
    DateOfBirthNew   Date32
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(RecordDate)
ORDER BY (UserID, RecordDate);

CREATE TABLE IF NOT EXISTS analytics.CountriesNew
(
    ID   UInt32,
    Name String
)
ENGINE = MergeTree
ORDER BY ID;

CREATE TABLE IF NOT EXISTS analytics.Turnovers
(
    UserID   UInt64,
    -- stored in cents, the query divides by 100
    Deposit  Int64,
    Withdraw Int64
)
ENGINE = MergeTree
ORDER BY UserID;
