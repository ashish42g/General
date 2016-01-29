SET mapred.compress.map.output true;
SET mapred.output.compression.codec org.apache.hadoop.io.compress.SnappyCodec;
SET output.compression.enabled true;
SET output.compression.codec org.apache.hadoop.io.compress.SnappyCodec;
SET io.compression.codecs org.apache.hadoop.io.compress.SnappyCodec;
SET mapred.output.compress true;
SET job.name "Escrowproxy_Hourly_Agg_SBD";

REGISTER /home/hadoop/reports/jobs/cdh4-client-jars/lib/udf/hdpudfs.jar;

A = load '$in_dir' using PigStorage('\t') as (PRS_ID:long,LABEL:chararray,WEB:chararray,EVENT_TYPE:chararray,EVENT_TIME:chararray,RESPONSE:chararray,SLMT:chararray,COMMAND:chararray,CLIENT_INFO:chararray, CLUBH:chararray,ERROR_CODE:chararray,USER_AGENT:chararray);

-- Changed for iCloud 15F release as EVENT_TIME is coming null
-- B = FILTER A by PRS_ID is not null AND EVENT_TIME is not null and (LABEL is null or not LABEL matches 'com.apple.securebackup.record') ;

B = FILTER A by PRS_ID is not null  and (LABEL is null or not LABEL matches 'com.apple.protectedcloudstorage.record') ;

--C = group B by (PRS_ID,LABEL,hdpudfs.TimeConverter(EVENT_TIME),EVENT_TYPE,RESPONSE,COMMAND,CLIENT_INFO,CLUBH,ERROR_CODE,USER_AGENT);

C = group B by (PRS_ID,LABEL,hdpudfs.TimeConverter(EVENT_TIME),EVENT_TYPE,RESPONSE,COMMAND,CLIENT_INFO,CLUBH,ERROR_CODE,USER_AGENT,SLMT);

D = FOREACH C GENERATE FLATTEN(group), MAX(B.EVENT_TIME), COUNT(B);

STORE D INTO '$out_dir_0' using PigStorage('\t');
