-- Fake data shaped to exercise the real code paths:
--   * 3 revisions per user, so argMax(..., RecordTime) has something to choose from
--     (older revisions carry STALE-* values -- if any of them reach NetHunt, argMax is wrong)
--   * 180 users inside the default 60-minute window and 70 outside it,
--     so the LastUpdated filter is visibly doing work
--   * 180 matching users against BATCH_SIZE=100 means pagination runs twice and then stops

INSERT INTO analytics.CountriesNew (ID, Name) VALUES
    (1, 'Poland'), (2, 'Ukraine'), (3, 'Germany'), (4, 'Canada'), (5, 'Brazil');

INSERT INTO analytics.UserHistory
SELECT
    intDiv(number, 3) + 1                                   AS UserID,
    concat('user', toString(UserID))                        AS Login,
    if(number % 3 = 2, concat('First', toString(UserID)), 'STALE-First')    AS Name,
    if(number % 3 = 2, concat('Last', toString(UserID)), 'STALE-Last')      AS LastName,
    concat('user', toString(UserID), '@example.test')       AS Email,
    concat('+4850000', leftPad(toString(UserID), 4, '0'))   AS Phone,
    toUInt8(UserID % 2)                                     AS PhoneVerified,
    if(UserID % 2 = 0, 'Male', 'Female')                    AS Gender,
    arrayElement(['en', 'pl', 'uk', 'de'], toInt32(UserID % 4) + 1)  AS Language,
    -- UserID 7 points at a country that does not exist in CountriesNew, the
    -- same shape as the 102 production users an inner join silently dropped
    if(UserID = 7, 9999, toUInt32(UserID % 5) + 1)          AS CountryID,
    if(number % 3 = 2, arrayElement(['Warsaw', 'Kyiv', 'Berlin', 'Toronto'], toInt32(UserID % 4) + 1), 'STALE-City') AS City,
    'Europe/Warsaw'                                         AS Timezone,
    now() - INTERVAL toUInt32(UserID % 48) HOUR             AS LastCreditDate,
    now() - INTERVAL toUInt32(UserID % 365) DAY             AS RegistrationDate,
    now() - INTERVAL toUInt32(UserID % 72) HOUR             AS LastLoginDate,
    toUInt8(if(UserID % 50 = 0, 1, 0))                      AS PEP,
    toUInt8(if(UserID % 7 = 0, 0, 1))                       AS Status,
    now() - INTERVAL (2 - toUInt32(number % 3)) DAY         AS RecordTime,
    toDate(RecordTime)                                      AS RecordDate,
    -- users 1..180 are "recently changed", the rest must be filtered out
    if(UserID <= 180, now() - INTERVAL toUInt32(UserID % 30) MINUTE, now() - INTERVAL 5 DAY) AS LastUpdated,
    -- every branch of the DateOfBirth expression gets a case here:
    --   UserID % 4 != 0            -> Date32 carries the real value
    --   UserID % 8 = 0             -> Date32 is the placeholder, string has the value
    --   UserID % 4 = 0 (not % 8)   -> both empty
    if(UserID % 8 = 0, concat('19', toString(70 + UserID % 30), '-03-21'), NULL) AS DateOfBirth,
    if(UserID % 4 = 0,
       toDate32('1900-01-01'),
       toDate32(concat('19', toString(60 + UserID % 40), '-0', toString(1 + UserID % 9), '-1', toString(UserID % 10)))) AS DateOfBirthNew
FROM numbers(750);

-- Two turnover rows per user: the query sums them, so the totals prove the GROUP BY
INSERT INTO analytics.Turnovers
SELECT
    intDiv(number, 2) + 1        AS UserID,
    toInt64(UserID * 1000 + 500) AS Deposit,
    toInt64(UserID * 250)        AS Withdraw
FROM numbers(500);
