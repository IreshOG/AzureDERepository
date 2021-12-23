Azure Data Factory
 - Data Flows : This is meant to perform data transformation. Here you can write the required transformation
                logic even without writing the code.

				Data Flows internally for transformation using Apache Spark Clusters !!!


Data Flow

Employee ----
				Join --------- result (Synapse) ----> Spark -----> Analytics
Dept --------


create table employee
(eid int,
esal int,
deptid int,
ename text);

create table dept
(deptid int,
deptname text)


insert into employee values (1,5000,101,'Prashant');
insert into employee values (2,3000,101,'Arun');
insert into employee values (3,2000,102,'Akhil');


insert into dept values (101,'Operations');
insert into dept values (102,'HR');

select * from employee;
select * from dept;

select * from employee join dept on employee.deptid=dept.deptid

CREATE VIEW result AS
SELECT e.eid, e.esal, e.ename, d.deptid, d.deptnameFROM employee as eLEFT JOIN dept as dON e.deptid=d.deptid;select * from resultCreate Destination table in SynapseCREATE TABLE [resultset]
(eid int,
esal int,
ename [varchar](200),
deptid int,
deptname [varchar](200));

select * from resultset