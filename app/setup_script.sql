CREATE APPLICATION ROLE IF NOT EXISTS app_public;


CREATE OR ALTER VERSIONED SCHEMA core;
GRANT USAGE ON SCHEMA core TO APPLICATION ROLE app_public;

CREATE or replace STREAMLIT core.ui
     FROM '/'
     MAIN_FILE = 'Dashboard.py';

GRANT USAGE ON STREAMLIT core.ui TO APPLICATION ROLE app_public;

GRANT ALL ON SCHEMA core TO APPLICATION ROLE app_public;



create or replace procedure core.register_reference(ref_name string, operation string, ref_or_alias string)
returns string
language sql
as $$
begin
case (operation)
  when 'ADD' then 
    select system$set_reference(:ref_name, :ref_or_alias);
  when 'REMOVE' then 
    select system$remove_reference(:ref_name, :ref_or_alias);
  when 'CLEAR' then 
    select system$remove_all_references(:ref_name);
  else
    return 'Unknown operation: ' || operation;
  end case;
return 'Success';
end;
$$;

GRANT USAGE ON PROCEDURE core.register_reference( string, string,  string) TO APPLICATION ROLE app_public;
