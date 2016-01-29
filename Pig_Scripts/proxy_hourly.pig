A = load '/Users/ashish/IdeaProjects/General/data/sample_proxy_Data.csv'
using PigStorage(',')
as (PRS_ID:long,LABEL:chararray,WEB:chararray,EVENT_TYPE:chararray,EVENT_TIME:chararray,RESPONSE:chararray,SLMT:chararray,COMMAND:chararray,CLIENT_INFO:chararray, CLUBH:chararray,ERROR_CODE:chararray,USER_AGENT:chararray);

B = filter A by PRS_ID is not null and (LABEL is null or not LABEL matches 'com.apple.com.apple.securebackup.record.record');

C = group B by (PRS_ID,LABEL,hdpudfs.TimeConverter(EVENT_TIME),EVENT_TYPE,RESPONSE,COMMAND,CLIENT_INFO,CLUBH,ERROR_CODE,USER_AGENT,SLMT);


