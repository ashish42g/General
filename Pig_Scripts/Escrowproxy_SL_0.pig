REGISTER /home/hadoop/reports/jobs/HistoryDataToAvro/jars/piggybank.jar;

SET mapred.compress.map.output true;
SET mapred.output.compression.codec org.apache.hadoop.io.compress.SnappyCodec;
SET output.compression.enabled true;
SET output.compression.codec org.apache.hadoop.io.compress.SnappyCodec;
SET io.compression.codecs org.apache.hadoop.io.compress.SnappyCodec;
SET mapred.output.compress true;
SET job.name "Escrowproxy_Daily_SL_0";
SET default_parallel 300;

ESCROW_DAILY = load '$in_dir1' using PigStorage('\t') as (PRS_ID:long,LABEL:chararray,EVENT_TIME:chararray,EVENT_TYPE:chararray,RESPONSE:chararray,COMMAND:chararray,CLIENT_INFO:chararray,CLUBH:chararray,ERROR_CODE:chararray,USER_AGENT:chararray,EPOCH_TIME:long,AGGR_COUNT:long,SLMT:chararray);
--- ADDED CONCAT METHOD in ST proc16 host TO AVOID ERROR 2000: Error processing rule ColumnMapKeyPrune

ESCROW_DAILY_USERS = FOREACH ESCROW_DAILY GENERATE PRS_ID, LABEL,EVENT_TYPE,RESPONSE,COMMAND,CLIENT_INFO,CLUBH,EPOCH_TIME,CONCAT(EVENT_TIME,'')  as INITIAL_DATE,CONCAT(EVENT_TIME,'') as RECENT_DATE,ERROR_CODE,USER_AGENT,SLMT;

ESCROW_SL = LOAD '$in_dir2' using PigStorage('\t') as (PRS_ID:long,LABEL:chararray,EVENT_TYPE:chararray,RESPONSE:chararray,COMMAND:chararray,CLIENT_INFO:chararray,CLUBH:chararray,EPOCH_TIME:long,INITIAL_DATE:chararray,RECENT_DATE:chararray,ERROR_CODE:chararray,USER_AGENT:chararray,SLMT:chararray);

COMBINED = UNION ESCROW_DAILY_USERS,ESCROW_SL;

COMBINED_GRPD = GROUP COMBINED BY (PRS_ID,LABEL,EVENT_TYPE,RESPONSE,COMMAND,CLIENT_INFO,CLUBH,ERROR_CODE,USER_AGENT,SLMT);

COMBINED_SL = FOREACH COMBINED_GRPD GENERATE FLATTEN(group) AS (PRS_ID:long,LABEL:chararray,EVENT_TYPE:chararray,RESPONSE:chararray,COMMAND:chararray,CLIENT_INFO:chararray,CLUBH:chararray,ERROR_CODE:chararray,USER_AGENT:chararray,SLMT:chararray), MAX(COMBINED.EPOCH_TIME) AS EPOCH_TIME, MIN(COMBINED.INITIAL_DATE) AS INITIAL_DATE, MAX(COMBINED.RECENT_DATE) AS RECENT_DATE;

GEN_COMBINED_SL = FOREACH COMBINED_SL GENERATE PRS_ID, LABEL,EVENT_TYPE,RESPONSE,COMMAND,CLIENT_INFO,CLUBH,EPOCH_TIME,INITIAL_DATE,RECENT_DATE,ERROR_CODE,USER_AGENT,SLMT;

STORE GEN_COMBINED_SL INTO '$out_dir' using PigStorage('\t');

