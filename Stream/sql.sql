SELECT * FROM public.transactions
ORDER BY transaction_id ASC 

ALTER TABLE transactions ADD COLUMN modified_by TEXT

ALTER TABLE transactions ADD COLUMN modified_at TIMEsTAMP


CREATE OR REPLACE FUNCTION record_change_user()
RETURNS TRIGGER AS $$ 
BEGIN 
NEW.modified_by := CURRENT_USER;
NEW.modified_at := CURRENT_TIMESTAMP;
RETURN NEW;
END;
$$ LANGUAGE plpgsql;


CREATE TRIGGER trigger_record_user_update 
BEFORE UPDATE ON transactions 
FOR EACH ROW EXECUTE FUNCTION record_change_user()

drop trigger trigger_record_user_update on transactions


update transactions set amount = amount + 100 where transaction_id = 'fb9b60f8-f3fd-48e0-9304-34ec7ecd5b33'

-- capture the changes to specific columns into json object
CREATE OR REPLACE FUNCTION record_changed_columns()
RETURNS TRIGGER AS $$
DECLARE 
	change_details JSONB;

BEGIN
	change_details := '{}'::JSONB;  -- Initialize an empty JSONB object
	IF NEW.amount IS DISTINCT FROM OLD.amount THEN 
		change_details := jsonb_insert(change_details, '{amount}', jsonb_build_object('old', OLD.amount, 'new', NEW.amount));
	END IF;
	
	-- adding the user and timestamp
	change_details := cahnge_details || jsonb_build_object('modified_by', CURRENT_USER, 'modified_at', NOW());
	
	-- update the change info column
	NEW.change_info := change_details;
	RETURN NEW;
END;
$$ LANGUAGE plpgsql;



ALTER TABLE transactions ADD COLUMN chage_info JSONB;




CREATE TRIGGER trigger_record_change_info
BEFORE UPDATE ON transactions 
FOR EACH ROW EXECUTE FUNCTION record_change_user()

