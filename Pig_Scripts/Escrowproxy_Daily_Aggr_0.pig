%default TODAYS_DATE `date  +%Y-%m-%d`
%default ONE_DAY_AGO `date -d "$TODAYS_DATE - 1 day" +%Y-%m-%d`

SET mapred.compress.map.output true;
SET mapred.output.compression.codec org.apache.hadoop.io.compress.SnappyCodec;
SET output.compression.enabled true;
SET output.compression.codec org.apache.hadoop.io.compress.SnappyCodec;
SET io.compression.codecs org.apache.hadoop.io.compress.SnappyCodec;
SET mapred.output.compress true;
SET job.name "Escrowproxy_Daily_Aggr_0";
SET default_parallel 60;
A = load '$in_dir1' using PigStorage('\t') as (PRS_ID:long,LABEL:chararray,EVENT_TIME:chararray,EVENT_TYPE:chararray,RESPONSE:chararray,COMMAND:chararray,CLIENT_INFO:chararray,CLUBH:chararray,ERROR_CODE:chararray,USER_AGENT:chararray,SLMT:chararray,MAX_EVENT_TIME:chararray,AGGR_COUNT:long);

B1 = FILTER A by PRS_ID is not null;

B = FOREACH B1 GENERATE PRS_ID, LABEL, '$ONE_DAY_AGO' AS EVENT_TIME, EVENT_TYPE, RESPONSE, COMMAND, CLIENT_INFO, CLUBH, ERROR_CODE, USER_AGENT,SLMT,MAX_EVENT_TIME,AGGR_COUNT;

C = group B by (PRS_ID,LABEL,EVENT_TIME,EVENT_TYPE,RESPONSE,COMMAND,CLIENT_INFO,CLUBH,ERROR_CODE,USER_AGENT,SLMT);
--D = FOREACH C GENERATE FLATTEN(group), MAX(B.MAX_EVENT_TIME), SUM(B.AGGR_COUNT);

D1 = FOREACH C GENERATE FLATTEN(group), MAX(B.MAX_EVENT_TIME), SUM(B.AGGR_COUNT);
D = FOREACH D1 GENERATE $0, $1, $2, $3, $4, $5, $6, $7, $8, $9, $11, $12, $10;

STORE D INTO '$out_dir' using PigStorage('\t');
