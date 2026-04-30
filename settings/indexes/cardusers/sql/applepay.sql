-- @range
SELECT
    MIN(AP.DPAN_ID) AS MIN_ID,
    MAX(AP.DPAN_ID) AS MAX_ID
FROM GAMER.APPLEPAY_DPAN AP

-- @batch
SELECT
    CASE AP.WALLET_TYPE
        WHEN 1 THEN 'APPLE_PAY'  || TO_CHAR(AP.DPAN_ID)
        WHEN 2 THEN 'GOOGLE_PAY' || TO_CHAR(AP.DPAN_ID)
    END                                                         AS "id",
    AP.WALLET_TYPE                                              AS "WALLET_TYPE",
    AP.DPAN_ID                                                  AS "DPAN_ID",
    MIN(ADU.CREATE_DATE)                                        AS "CREATE_DATE",
    AP.DISPLAY_NAME                                             AS "DISPLAY_NAME",
    AP.EXPIRATION_DATE                                          AS "EXPIRATION_DATE",
    AP.IN_BLACKLIST                                             AS "IN_BLACKLIST",
    AP.VERIFIED                                                 AS "VERIFIED",
    CAST(AP.NETWORK AS VARCHAR2(200))                           AS "NETWORK",
    CAST(AP.TYPE    AS VARCHAR2(200))                           AS "TYPE",
    LISTAGG(CAST(U.EXTERNALUSERID AS VARCHAR2(100)), ',')
        WITHIN GROUP (ORDER BY U.EXTERNALUSERID)                AS "EXTERNALUSERIDS",
    LISTAGG(U.USERID, ',') WITHIN GROUP (ORDER BY U.USERID)     AS "USERIDS"
FROM GAMER.APPLEPAY_DPAN AP
LEFT JOIN GAMER.APPLEPAY_DPAN_USERS ADU
    ON ADU.DPAN_ID = AP.DPAN_ID
LEFT JOIN GAMER.USERDETAILS2 U
    ON U.USERID = ADU.USER_ID
WHERE AP.DPAN_ID >= :from_id
  AND AP.DPAN_ID <  :to_id
GROUP BY
    AP.WALLET_TYPE,
    AP.DPAN_ID,
    AP.DISPLAY_NAME,
    AP.EXPIRATION_DATE,
    AP.IN_BLACKLIST,
    AP.VERIFIED,
    AP.NETWORK,
    AP.TYPE
ORDER BY AP.DPAN_ID
