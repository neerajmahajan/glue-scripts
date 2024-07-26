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
