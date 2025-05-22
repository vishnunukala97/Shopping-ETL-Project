use ShoppingDB
go

create table Items (
		item_id varchar(10) primary key,
		item_name varchar(200) not null,
		price float ,
		is_active bit );


create table Transactions (
		transaction_id UNIQUEIDENTIFIER  primary key,
		item_id varchar(10) foreign key references Items(item_id),
		date_of_transaction date,
		qusntity int,
		timezone varchar(10) 
		);

EXEC sp_rename 'Transactions.qusntity', 'quantity', 'COLUMN';



select @@SERVERNAME

select * from Items;

SELECT name FROM sys.tables;

SELECT COLUMN_NAME
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_NAME = 'Transactions';

select * from Items;

select * from Transactions;


delete from Transactions;
delete from Items;


