add file /Users/hadoophive/IdeaProjects/normalizeFiles/src/main/groovy/identity.groovy;
add file /Users/hadoophive/IdeaProjects/normalizeFiles/src/main/groovy/condenser.groovy;
INSERT OVERWRITE TABLE NPL_STAT
SELECT delivery_id, provider, flowtype, delivery, status, count(distinct publisher_doc_id), min(xp) as min_xp,max(xp) as max_xp
FROM npl_normalized_log
GROUP BY delivery_id, status, delivery, provider, flowtype
ORDER BY provider,flowtype,delivery;
FROM (
    FROM  NPL_STAT
    MAP DELIVERY_ID, PROVIDER, FLOWTYPE, DELIVERY, STATUS, STATUS_COUNT, MIN_XP, MAX_XP
    USING '/usr/share/java/groovy/bin/groovy ./identity.groovy'
    AS DELIVERY_ID, PROVIDER, FLOWTYPE, DELIVERY, STATUS, STATUS_COUNT, MIN_XP, MAX_XP
    CLUSTER BY DELIVERY_ID) MAP_OUTPUT
    INSERT OVERWRITE TABLE NPL_STAT_CONDENSED
    REDUCE DELIVERY_ID, PROVIDER, FLOWTYPE, DELIVERY, STATUS, STATUS_COUNT, MIN_XP, MAX_XP
    USING '/usr/share/java/groovy/bin/groovy ./condenser.groovy'
    AS DELIVERY_ID, PROVIDER, FLOWTYPE, DELIVERY, INN, IGNORED, BUSINNESSREJECT, UNKNOWNERROR, NOPDF, UNKNOWNCONTENTTYPE, OK, PRODUCED, MIN_XP, MAX_XP
;



