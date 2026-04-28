SELECT
    LO.USERID                         AS "userId",
    UD.EXTERNALUSERID                 AS "externalUserId",
    LO.LOGINID                        AS "loginId",
    NVL(UD.USER_PARENT_ID, 0)         AS "parentId",
    NVL(UP.EXTERNAL_PARENT_ID, -1)    AS "externalParentId",
    CU.SKINID                         AS "skinId",
    SK.SKIN                           AS "skin",
    SGS.GROUPID                       AS "skinGroupId",
    SK.SKINORIGIN                     AS "skinOriginId",
    LO.LOGINDATE                      AS "loginDate",
    LO.LOGOUTDATE                     AS "logoutDate",
    LO.IP                             AS "ip",
    LO.LOGOUTCODE                     AS "logoutCode",
    LO.COUNTRYID                      AS "countryId",
    LO.REALBALANCE                    AS "realBalance",
    LO.BONUSBALANCE                   AS "bonusBalance",
    LO.LOGINTYPEID                    AS "loginTypeId",
    LO.CHANNELID                      AS "channelId",
    ISU.INTERNALACCOUNT               AS "internalAccount",
    LO.ISWEB                          AS "isWeb",
    LSI.NATIVE                        AS "nativeApp",
    LSI.OS                            AS "os",
    LSI.BROWSER_NAME                  AS "browserName",
    LSI.APP_BUILD_NUMBER              AS "appBuildNumber",
    LSI.IP_CITY                       AS "ipCity",
    SG.GROUPNAME                      AS "skinGroup",
    GO.ORIGIN                         AS "skinOrigin",
    GO.ORIGIN                         AS "skinOriginName"
FROM GAMER.LOGINS LO
LEFT JOIN GAMER.USERDETAILS2        UD  ON UD.USERID         = LO.USERID
LEFT JOIN GAMER.USER_PARENT         UP  ON UP.USER_PARENT_ID = UD.USER_PARENT_ID
LEFT JOIN GAMER.IR_SYS_USERACCOUNTS ISU ON ISU.USERID        = LO.USERID
LEFT JOIN GAMER.LOGINS_SOURCE_INFO  LSI ON LSI.LOGINID       = LO.LOGINID
LEFT JOIN CASINO.USERS              CU  ON CU.USERID         = LO.USERID
LEFT JOIN GAMER.SKINS               SK  ON SK.SKINID         = CU.SKINID
LEFT JOIN GAMER.SKINGROUPSKINS      SGS ON SGS.SKINID        = CU.SKINID
LEFT JOIN GAMER.SKINGROUPS          SG  ON SG.GROUPID        = SGS.GROUPID
LEFT JOIN GAMER.SKINORIGIN          GO  ON GO.ORIGINID       = SK.SKINORIGIN
