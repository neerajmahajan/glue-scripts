-- pre-req https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/PostgreSQL-Lambda.html
-- install aws_lambda extension in PG DB, but before that update rds.allowed_extensions to allow aws_lambda
-- Change the value of the rds.custom_dns_resolution parameter from its default of 0 to 1. If there is an vpc endpoint for lambda, but private dns is disabled then this will not work.
-- setup the IAM role and permission for RDS to call lambda
DECLARE
	required_event_list character varying[];
	event_list_waiting_to_process character varying[];
	event_ids bigint[];
	run_comparision boolean;
	lambda_status_code bigint;	
	request_body json;

BEGIN
 
 select ('{"process_to_run": '||'"' || NEW.process ||'"' || ',"event_date": ' ||'"' || NEW.business_date::text ||'"' ||'}')::json into request_body;
 select array_agg(event_type) into required_event_list from  process_event_type where  process = NEW.process;
 select array_agg(id) ,  array_agg(event_type) into event_ids,event_list_waiting_to_process
     from   event_watch  where  process = NEW.process and  status  = 'NEW' and event_date = NEW.event_date;
	 
 run_comparision = required_event_list <@ event_list_waiting_to_process;
 RAISE NOTICE 'EVENTS REQUIRED %',required_event_list;
 RAISE NOTICE 'EVENTS OCCURED %',event_list_waiting_to_process; 
 RAISE NOTICE 'EVENTS IDs  %',event_ids;
 RAISE NOTICE 'REQUEST BODY  %',request_body;
 
 IF run_comparision THEN  
  SELECT status_code into lambda_status_code from aws_lambda.invoke('<lambda_arn_to_be_called>', request_body);
  
  RAISE NOTICE 'lambda status code  %',lambda_status_code;
  
  IF lambda_status_code = 200 THEN
  	UPDATE ach_event_watch SET status = 'COMPLETED' WHERE id = any (event_ids);
  END IF;
 END IF;
 
 
 RETURN NEW;
END
