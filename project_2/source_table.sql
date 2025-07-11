--- drop table
drop table employees;
drop table emp_cdc;

truncate table employees;
truncate table emp_cdc;

--drop trigger
DROP FUNCTION log_action_after();

DROP TRIGGER trg_after_insert ON employees;
DROP TRIGGER trg_after_update ON employees;
DROP TRIGGER trg_after_delete ON employees;


--- sources table
CREATE TABLE employees( 
emp_id SERIAL, 
first_name VARCHAR(100), 
last_name VARCHAR(100), 
dob DATE, 
city VARCHAR(100), 
salary INT );

--- keep track of DML command
CREATE TABLE emp_cdc( 
cdc_id SERIAL primary key,
emp_id INT, 
first_name VARCHAR(100), 
last_name VARCHAR(100), 
dob DATE, 
city VARCHAR(100), 
salary INT, 
action VARCHAR(100) );


--- trigger function
create or replace function log_action_after()
returns trigger as $$
begin
	IF TG_OP = 'INSERT' then
		insert into emp_cdc(
		emp_id,first_name,last_name,dob,city,salary,action)
		values (
			new.emp_id, new.first_name, new.last_name, new.dob, new.city, new.salary,'INSERT');
	
	elsif TG_OP = 'UPDATE' then
		insert into emp_cdc(
		emp_id,first_name,last_name,dob,city,salary,action) 
		values(
			new.emp_id, new.first_name, new.last_name, new.dob, new.city, new.salary,'UPDATE');
	elsif TG_OP = 'DELETE' then 
		insert into emp_cdc(
		emp_id,first_name,last_name,dob,city,salary,action) 
		values(
			old.emp_id, old.first_name, old.last_name, old.dob, old.city, old.salary,'DELETE');
	end if;
	return Null;
end;
$$ language plpgsql;


--- create trigger
create trigger trg_after_insert
after insert on employees
for each row 
execute function log_action_after();

create trigger trg_after_update 
after update on employees
for each row
execute function log_action_after();

create trigger trg_after_delete
after delete on employees
for each row
execute function log_action_after();


--- insert:
insert into employees(first_name,last_name,dob,city, salary)
values('Penny', 'Long', '2006-8-9', 'texas', 150000);
insert into employees(first_name,last_name,dob,city, salary)
values('Jeffery', 'Liu', '2005-1-9', 'texas', 100000);
insert into employees(first_name,last_name,dob,city, salary)
values('FirstName', 'LastName', '2000-1-9', 'texas', 8000);

select * from employees e ;
select * from emp_cdc;

--- update:
update employees e set first_name = 'Lucky' where emp_id = 1;
--- delete:
delete from employees where emp_id = 1;

--- add unknow action
INSERT INTO emp_cdc (first_name, last_name, dob, city, salary, action)
VALUES ('Test', 'Test', '2001-1-9', 'place', 100, 'UNKNOWN');


