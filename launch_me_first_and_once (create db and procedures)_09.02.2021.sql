/* ��������� �����. ��������, 2020 */
/* 
��� ����, � ������� ������������ (�� �� ����������) ���������. 
����������: ������������ � ���������� ��������� �������� ���� ������.
���� ���� ����� ��������� ������, ������ �� �����.
*/


/*	
CREATE_DATABASE_PRICE_FORMATION ���������
������� ���� ������ PRICE_FORMATION 
������ COLLATE � RECOVERY 
*/
DROP PROCEDURE IF EXISTS  CREATE_DATABASE_PRICE_FORMATION 
GO
CREATE PROCEDURE CREATE_DATABASE_PRICE_FORMATION 
	AS
	BEGIN
		DROP DATABASE IF EXISTS PRICE_FORMATION
		CREATE DATABASE PRICE_FORMATION  
			COLLATE Cyrillic_General_CI_AS
			
		ALTER DATABASE PRICE_FORMATION SET RECOVERY SIMPLE
	END
GO 

EXEC CREATE_DATABASE_PRICE_FORMATION
GO
/*
-----ALTER DATABASE PRICE_FORMATION SET OFFLINE WITH ROLLBACK IMMEDIATE
exec sp_who2
--kill --RUNNABLE
DROP DATABASE IF EXISTS PRICE_FORMATION
GO

CREATE DATABASE PRICE_FORMATION  
	COLLATE Cyrillic_General_CI_AS
ALTER DATABASE PRICE_FORMATION SET RECOVERY SIMPLE
GO
*/
USE PRICE_FORMATION 
GO


/*	
LOAD_FROM_FILES ���������
1. ������� ������� sales, products, stores. 
2. ��������� � ��� ������ �� ������ (� ������������� �������� � ����������� �����). ���������� � ������������ ������ ���������� � ��������� �����  � ����������.
3. ������� ��������� ������ � ���� nvarchar, ����� ����������� � date, nvarchar, float.
4. ������ ��������� � ������� ����� ��� ������.
*/
DROP PROCEDURE IF EXISTS LOAD_FROM_FILES
GO
CREATE PROCEDURE LOAD_FROM_FILES (@DIRECTORY		varchar(300) = 'C:\Users\elena.khotlyannik\Documents\2020AAPrice\sales01.04.2016-01.04.2018\',
								  @SALES_FILE1		varchar(100) = '�������� ������ � 01.04.2016 - 31.12.2016.csv',								  
								  @SALES_FILE2		varchar(100) = '�������� ������ � 01.01.2017 - 01.10.2017.csv',
								  @SALES_FILE3		varchar(100) = '�������� ������ � 02.10.2017 - 01.04.2018.csv',
								  @PRODUCTS_FILE	varchar(100) = '���������� �������(�� ������� ���� ������� � 01.04.2016-01.04.2018).csv',
								  @STORES_FILE		varchar(100) = '��������.csv'								  
								  )
	AS 
	BEGIN
	/*1.1.������������� ������� ������*/
		DROP TABLE IF EXISTS sales 
		CREATE TABLE sales	(	[s_date]		nvarchar(50) NOT NULL,
								[store_id]		nvarchar(50) NOT NULL,
								[product_id]	nvarchar(50) NOT NULL,
								[s_amount]		nvarchar(50),
								[s_count]		nvarchar(50)
							)
		
		/*1.2.�������� ������ � ������� ������ �� 3-� ������*/
		DECLARE @BULK_INSERT nvarchar(MAX);
		SET @BULK_INSERT = '
		BULK INSERT sales  
		FROM ''' + @DIRECTORY  + @SALES_FILE1 + '''  
		WITH	(	FIRSTROW=2,
					FIELDTERMINATOR = '';'', 
					ROWTERMINATOR = ''\n''
				)';
		EXEC(@BULK_INSERT)

		SET @BULK_INSERT = '
		BULK INSERT sales 
		FROM ''' + @DIRECTORY  + @SALES_FILE2 + '''
		WITH	(	FIRSTROW=2,
					FIELDTERMINATOR = '';'', 
					ROWTERMINATOR = ''\n''
				)'
		EXEC(@BULK_INSERT)
		
		SET @BULK_INSERT = '
		BULK INSERT sales 
		FROM ''' + @DIRECTORY  + @SALES_FILE3 + '''
		WITH	(  FIRSTROW=2,
					FIELDTERMINATOR = '';'', 
					ROWTERMINATOR = ''\n''
				)'
		EXEC(@BULK_INSERT)
		
		/* �������� ���������� ������� � ������� ������
		select count( *) from  sales
		*/

		/*1.3.�������������� ����� ������ � ������� ������ */
		ALTER TABLE sales ALTER COLUMN 	s_date		date	NOT NULL
		ALTER TABLE sales ALTER COLUMN 	store_id	bigint	NOT NULL
		ALTER TABLE sales ALTER COLUMN 	product_id	bigint	NOT NULL
		
		UPDATE sales set s_amount=TRY_CONVERT(float, REPLACE(s_amount,',','.'))
		ALTER TABLE sales ALTER COLUMN s_amount float
		
		UPDATE sales set s_count=TRY_CONVERT(float, REPLACE(s_count,',','.'))
		ALTER TABLE sales ALTER COLUMN s_count float
		
		/* 1.4 ���������� ����� */
		ALTER table sales
		ADD CONSTRAINT PK_sales PRIMARY KEY CLUSTERED (s_date, store_id, product_id)
		
		
		/*2.1.������������� ����������� ������� */
		DROP TABLE IF EXISTS [products]
		CREATE TABLE [products] (	[product_id] nvarchar(50) NOT NULL,
									[product_name] nvarchar(150)
								)

		/*2.2.�������� ������ � ���������� ������� */
		SET @BULK_INSERT = '
		BULK INSERT products 
		FROM ''' + @DIRECTORY  + @PRODUCTS_FILE + '''
		WITH	(	FIRSTROW=2,
					FIELDTERMINATOR = '';'', 
					ROWTERMINATOR = ''0x0a'',
					CODEPAGE = ''1251''
				)'
		EXEC(@BULK_INSERT)
		
		/*2.3.�������������� ���� ������ � ���������� ����� � ������� ������� */
		ALTER TABLE products ALTER COLUMN 	product_id bigint NOT NULL

		ALTER table products
		ADD CONSTRAINT PK_products PRIMARY KEY CLUSTERED (product_id)

		
		/*3.1.������������� ����������� ��������� */
		DROP TABLE IF EXISTS stores ;
		CREATE TABLE stores	(	[store_id] nvarchar(50) NOT NULL,
								[store_name] nvarchar(150)
							)
		
		/*3.2.�������� ������ � ���������� ��������� */
		SET @BULK_INSERT = '
		BULK INSERT stores 
		FROM ''' + @DIRECTORY  + @STORES_FILE + '''
		WITH (
		  FIRSTROW=2,
		  FIELDTERMINATOR = '';'', 
		  ROWTERMINATOR = ''0x0a'',
		  CODEPAGE = ''1251''
		  )'
		EXEC(@BULK_INSERT)
		
		/*3.3.�������������� ����� � ������� ��������� */
		ALTER TABLE stores ALTER COLUMN store_id bigint NOT NULL

		/*3.4.���������� ����� � ������� ��������� */
		ALTER table stores
		ADD CONSTRAINT PK_stores PRIMARY KEY CLUSTERED (store_id)
		

		/* 4.4 ���������� ������� ������ � ������� ������ */
		ALTER table sales
		ADD CONSTRAINT FK_sales_store_id	FOREIGN KEY (store_id)	REFERENCES stores	(store_id)
		ON DELETE CASCADE
		ON UPDATE CASCADE
		
		ALTER table sales
		ADD CONSTRAINT FK_sales_product_id FOREIGN KEY (product_id) REFERENCES products (product_id)
		ON DELETE CASCADE
		ON UPDATE CASCADE
	END
GO


/* 
ANONIMISING ��������� 
1. �������� ������������� �������� , ��������������� ��������
2. �������� ������������� ������, ��������� � ������� ������� ������
3. ���������� �������� �� 5, ���� �� 1.3
*/
DROP PROCEDURE IF EXISTS ANONIMISING
GO
CREATE PROCEDURE ANONIMISING
	AS
	BEGIN
	/* ��������� �������������� �������� */
		UPDATE [stores] 
		SET store_id= (	SELECT new_id 
							FROM (SELECT	ROW_NUMBER() OVER(ORDER BY [store_name]) new_id,
											store_id
								  FROM [stores]  )  
							AS tab
							WHERE tab.store_id=[stores].store_id)


		/* ��������� �������������� ������ */
		UPDATE [products]  
		SET product_id= (	SELECT new_id 
							FROM (SELECT	ROW_NUMBER() OVER(ORDER BY [product_name]) new_id,
											product_id
								  FROM [products]   )  
							AS tab
							WHERE tab.product_id=[products] .product_id)

		/* �������������� ��������� */
		UPDATE [stores] 
		SET store_name = '������� �' WHERE store_id=1

		UPDATE [stores] 
		SET store_name = '������� �' WHERE store_id=2

		/* ������� ������� ��� ������� */
		UPDATE [products] 
		SET product_name = UPPER(product_name)

		
		UPDATE [sales]
		SET s_amount = 0,
			s_count= 0
		WHERE s_count <= 0  or  s_amount <= 0

		UPDATE [sales]
		SET s_amount = (s_count*5)*(s_amount/s_count*1.3),
			s_count= s_count*5
		WHERE s_count > 0 and s_amount > 0
	END
GO


/*
FILL_DATES_PRODUCTS
��������� ��� �������� ������������ ���������� ���� �� ������� �������� 
*/
DROP PROCEDURE IF EXISTS FILL_DATES_PRODUCTS 
GO
CREATE PROCEDURE FILL_DATES_PRODUCTS
	AS
	BEGIN
		DECLARE @dateStart DATE
		SET @dateStart =	(select min(s_date)  from sales)
		DECLARE @dateEnd DATE
		SET @dateEnd =	(select max(s_date)  from sales)

		  WHILE @dateStart <= @dateEnd 
			BEGIN 
			INSERT INTO time_product_series (s_date,	product_id,				store_id) 
			SELECT							@dateStart, products.product_id, stores.store_id
			from products, stores

				SET @dateStart = dateadd(DAY, 1 , @dateStart)
		  END
	END
GO


DROP PROCEDURE IF EXISTS ALTER_SALES
GO
CREATE PROCEDURE ALTER_SALES
	AS 
	BEGIN
		/*  ������� ���� � sales */
		ALTER TABLE [sales] ADD 
				price1					float NULL,		-- ��� ����������� �������, �������� ����� �������� �� ���� ������� (����� �������) 
				price2					float NULL,		-- ��� ����������� �������, �������� ����� �������� �� ���������� ��������� ���� 
				price3					float NULL,		-- ��� ����������� �������, �������� �����  �������� ����� ����� ���������� ����������  	
				auto_sign				integer NULL,	-- 
														-- , ����� 1 ��� ����������� ������� c �������������� ���������� ���� � ����������
														-- , ����� 0 ��� ����������� ������� c ���������������� ���������� ���� ��� ����������
														-- , ����� 5 ��� ����������� ������� (������ ��������� ��� ���������� ���������� ����)
				prev_notnull_date		date,
				prev_notnull_price		float,
				next_notnull_date		date,	
				next_notnull_price		float,
				avg_product_store_price float,
				time_series_flag		int	,			/* "������� ���������� ���������" =1, ���� ������ ������ ���������� ���� ��� ���������� ���� �������+������� */
				min_date				date,
				max_date				date;

	END
GO


/*	TIME_SERIES_AND_PRICES
��������� ��������� ��������� ���
������������ ��� �������� ����
����������� �������, ����������� ��������� ������ ��� ��������� � ������ ���������� ���������� ����
����������� �������, ���� ������ ������ ���������� ��������� ��� ��������� ���� �������+�������
*/
DROP PROCEDURE IF EXISTS TIME_SERIES_AND_PRICES
GO
CREATE PROCEDURE TIME_SERIES_AND_PRICES
	AS
	BEGIN		
		UPDATE [sales]
		SET 	price1 = 0,
				price2 = 0,
				price3 = 0,
				auto_sign = 0
		WHERE s_count <= 0  or  s_amount <= 0

		UPDATE [sales]
		SET 
			price1 = s_amount/s_count,
			price2 = s_amount/s_count,
			price3 = s_amount/s_count,
			auto_sign = 1
		WHERE s_count > 0 and s_amount > 0


		drop table IF EXISTS time_product_series
		create table time_product_series
		([s_date] date NOT NULL,
		[product_id] bigint NOT NULL,
		store_id bigint NOT NULL,
		min_date date,
		max_date date)

		EXEC FILL_DATES_PRODUCTS	
		
		INSERT INTO  sales  --~5 min
		SELECT t2.s_date , t2.store_id, t2.product_id, t1.s_amount, t1.s_count, t1.price1, t1.price2, t1.price3, 5,
			t1.prev_notnull_date, t1.prev_notnull_price, t1.next_notnull_date, t1.next_notnull_price,
			t1.avg_product_store_price, t1.time_series_flag, NULL, NULL
		FROM sales t1 
		FULL JOIN time_product_series t2
		ON t1.product_id=t2.product_id and t1.s_date=t2.s_date and t1.store_id=t2.store_id
		WHERE t1.s_amount is NULL		

		UPDATE sales --~2-3min
		set avg_product_store_price = (		SELECT mean_product_price
												FROM 
													(SELECT AVG(price1) as mean_product_price, product_id, store_id
														FROM sales
														WHERE s_amount > 0 	AND s_count > 0
														GROUP BY product_id, store_id) as t2   
												WHERE	t2.product_id=sales.product_id
														AND t2.store_id=sales.store_id
											)
		--where s_amount is NULL or s_amount=0

		drop table IF EXISTS prep1
		select * 
		into prep1
		from sales
		where s_amount>0

		update sales
		set prev_notnull_date =(SELECT max(it.s_date) 
								FROM prep1 it
								WHERE	it.s_date < sales.s_date
									AND it.product_id=sales.product_id
									AND it.store_id=sales.store_id
								),
		next_notnull_date = (SELECT min(it.s_date) 
								FROM prep1 it
								WHERE	it.s_date > sales.s_date
									AND it.product_id=sales.product_id
									AND it.store_id=sales.store_id
								)
		--where s_amount is NULL or s_amount=0

		UPDATE sales
		set time_series_flag =  1 
		WHERE 
			NOT (s_amount is  NULL and prev_notnull_date is  null) 
			and 
			not (s_amount is  NULL and next_notnull_date is  null)

		UPDATE sales
		set prev_notnull_price = (SELECT it.price1
									FROM sales it
									WHERE	it.s_date = sales.prev_notnull_date
										AND it.product_id=sales.product_id
										AND it.store_id=sales.store_id
								),
			next_notnull_price = (SELECT it.price1
									FROM sales it
									WHERE	it.s_date = sales.next_notnull_date
										AND it.product_id=sales.product_id
										AND it.store_id=sales.store_id
								)
		WHERE (s_amount is NULL or s_amount=0) and time_series_flag =  1 



		UPDATE sales
		set price1 =  (avg_product_store_price), 
			price2 =  (coalesce(prev_notnull_price,avg_product_store_price)),
			price3 =  ((coalesce(prev_notnull_price,avg_product_store_price) + coalesce(next_notnull_price,avg_product_store_price))/2)
		WHERE (s_amount is NULL or s_amount=0) and time_series_flag =  1 

		UPDATE sales
		set min_date = (select min(s2.s_date) from sales s2 where	s2.time_series_flag=1 
															and		s2.store_id=sales.store_id 
															and		s2.product_id=sales.product_id
															group by s2.store_id,s2.product_id),
			max_date = (select max(s2.s_date) from sales s2 where	s2.time_series_flag=1 
															and		s2.store_id=sales.store_id 
															and		s2.product_id=sales.product_id
															group by s2.store_id,s2.product_id)
		where time_series_flag = 1 


		UPDATE sales
		set s_count=0
		where time_series_flag = 1  and auto_sign = 5

	END
GO


/*
REPLACE_MEASURE_SIMBOLS

��������� �������� ������ @variant1, ������� ������� ����� ����� ����� ��� ����� ������,
 �� ������ @measure_simbol.
��������, ��� �����: @variant1 = ��, @measure_simbol=�.

��� ���� �������� ��������, ��� ��������������� ����� ������� @variant1 �� ����� ����� (����� �� �������� , ��������, ��������� '3 �������' �� '3 ������' )
*/
DROP PROCEDURE IF EXISTS REPLACE_MEASURE_SIMBOLS 
GO
CREATE PROCEDURE REPLACE_MEASURE_SIMBOLS  (@measure_simbol nvarchar(20),
											--  @remove_measure int =1,
											@variant1  nvarchar(20) = '',
											@variant2  nvarchar(20) = '')
	AS
	BEGIN
		/* �������� ������� �� � ����� (,),/  */	
		UPDATE step0
		SET product_name = replace(replace(replace(product_name,'(',' ( '),')',' ) '),'/',' / ')

		/* ������ ������ ������� � ������� � ����� ������*/	
		UPDATE step0
		SET product_name = rtrim(replace(product_name,'  ',' '))

		/* ������ ������� ������������ ������� �� ��������*/
		UPDATE step0
		SET product_name = replace(product_name, @variant1 , @measure_simbol)
		WHERE	(product_name like concat('%[0-9]',@variant1,'%')
			or product_name like concat('%[0-9] ',@variant1,'%'))
			and product_name not like concat('%[0-9]',@variant1,'[�-�]%')
			and product_name not like concat('%[0-9] ',@variant1,'[�-�]%')

		/*������ ������� ����� ������ � �������� ��������� (��������, ������� [0-9] � �� [0-9]�)*/
		UPDATE step0 
		SET product_name = replace(product_name, concat(' ',@measure_simbol), @measure_simbol)  
		WHERE product_name like   concat('%[0-9] ',@measure_simbol,'%')

	END
GO


/*
EXTRACT_MEASURE
��������� ��������� ��������� ����� � ��������. ��������, 100�. 
��������� ��� ��������� � ���� temp_param, � ������� ��� ��������� �� ���� product_name (��� �������� @remove_measure = 1 ).
�������� �� temp_param ����� ���������� ��������� ����� ��������� � ���������� ������ �������
��������, �������� �� ��� � ������ ���������
���������
	@measure_simbol - ������, ��������� � ������� ���� �����
	(���� �� ����������) @remove_measure - �������, ������� �� ��������� ��������� �� product_name (���� �� ����������)
	*/
DROP PROCEDURE	IF EXISTS EXTRACT_MEASURE 
GO
CREATE PROCEDURE EXTRACT_MEASURE  (@measure_simbol nvarchar(20))
	AS
	BEGIN
		/* �������� ������� �� � ����� (,),/  */	
		UPDATE step0
		SET product_name = replace(replace(replace(product_name,'(',' ( '),')',' ) '),'/',' / ')

		/* ������ ������ ������� � ������� � ����� ������*/	
		UPDATE step0
		SET product_name = rtrim(replace(product_name,'  ',' '))
		
		/*������ ������� ����� ������ � �������� ��������� (��������, ������� [0-9] � �� [0-9]�)*/
		UPDATE step0 
		SET product_name = replace(product_name, concat(' ',@measure_simbol), @measure_simbol)  
		WHERE product_name like   concat('%[0-9] ',@measure_simbol,'%')
			and product_name not like concat('%[0-9]',@measure_simbol,'[�-�]%')

		/* ���������� �����-�� ��������� [0-9]�*/
		DROP TABLE IF EXISTS step01
		SELECT 
			product_id,
			product_name,
			CASE 
				WHEN charindex(concat('0',@measure_simbol),product_name) !=0 THEN charindex(concat('0',@measure_simbol),product_name)
				WHEN charindex(concat('1',@measure_simbol),product_name) !=0 THEN charindex(concat('1',@measure_simbol),product_name)
				WHEN charindex(concat('2',@measure_simbol),product_name) !=0 THEN charindex(concat('2',@measure_simbol),product_name)
				WHEN charindex(concat('3',@measure_simbol),product_name) !=0 THEN charindex(concat('3',@measure_simbol),product_name)
				WHEN charindex(concat('4',@measure_simbol),product_name) !=0 THEN charindex(concat('4',@measure_simbol),product_name)
				WHEN charindex(concat('5',@measure_simbol),product_name) !=0 THEN charindex(concat('5',@measure_simbol),product_name)
				WHEN charindex(concat('6',@measure_simbol),product_name) !=0 THEN charindex(concat('6',@measure_simbol),product_name)
				WHEN charindex(concat('7',@measure_simbol),product_name) !=0 THEN charindex(concat('7',@measure_simbol),product_name)
				WHEN charindex(concat('8',@measure_simbol),product_name) !=0 THEN charindex(concat('8',@measure_simbol),product_name)
				WHEN charindex(concat('9',@measure_simbol),product_name) !=0 THEN charindex(concat('9',@measure_simbol),product_name)
			END as last_num_index
		INTO step01
		FROM STEP0
		WHERE	product_name like concat('%[0-9]',@measure_simbol,'%')
			and product_name not like concat('%[0-9]',@measure_simbol,'[�-�]%')

		/* ��������� ������� ���������� ����� ������� */
		DROP TABLE IF EXISTS step02
		SELECT	*,
				--substring(product_name, 1,last_num_index) as sub1,
				--reverse(substring(product_name, 1,last_num_index)) as sub2,
				--charindex(' ',reverse(substring(product_name, 1,last_num_index))) as charindex_space_rev,
				len(substring(product_name, 1,last_num_index)) - charindex(' ',reverse(substring(product_name, 1,last_num_index)))+1 as charindex_space
		INTO step02 	
		FROM step01
	
		/* ��������� ... �� ������� � ������� */
		DROP TABLE IF EXISTS step03
		SELECT	product_id,
				product_name,
				last_num_index,
				charindex_space,
				(last_num_index-charindex_space)+len(@measure_simbol) as count_simb,	--���������� ��������, ������� ������ � ��������
				last_num_index+len(@measure_simbol)+1 as charindex_after,				--������ ������� ������� ����� @measure_simbol
				charindex_space-1 as charindex_before						-- ������ ������� ����� ��������
		INTO step03
		FROM step02

		/* ��������� �������� � ����� ������ */
		DROP TABLE IF EXISTS step04
		SELECT	product_id,
				product_name,
				--charindex_space,
				--count_simb,
				--charindex_after,
				--charindex_before,
				substring(product_name,charindex_space+1,count_simb ) as new_param,
				--substring(product_name, 1,charindex_before ) as string_before,
				--substring(product_name, charindex_after,len(product_name) ) as string_after,
				concat(substring(product_name, 1,charindex_before ) ,
				substring(product_name, charindex_after,len(product_name) ) ) as new_product_name
		INTO step04
		FROM step03

		UPDATE step0 
		SET temp_param='                                '

		UPDATE step0
		SET		product_name= (select new_product_name from step04 as t1 where t1.product_id=step0.product_id),
				temp_param	= (select new_param from step04 as t1 where t1.product_id=step0.product_id),
				parse_status = parse_status+1
		WHERE	product_name like concat('%[0-9]',@measure_simbol,'%')
			and product_name not like concat('%[0-9]',@measure_simbol,'[�-�]%')


	
	END
GO

/* �������� ������� ������ ��� ������������� �������� */
DROP PROCEDURE IF EXISTS  CREATE_product_category_rules
GO
CREATE PROCEDURE CREATE_product_category_rules 
	AS
	BEGIN
		DROP TABLE IF EXISTS product_category_rules;
		CREATE TABLE product_category_rules 
			(rule_id				int			not null PRIMARY KEY IDENTITY(1,1),
			step_priority			int			not null,

			product_name_included	varchar(50) not null,
			product_name_excluded	varchar(50) not null,

			product_type			varchar(32) not null,
			product_subtype			varchar(32) not null,
			product_category		varchar(32) not null,
			product_subcategory		varchar(32) not null,
			producer				varchar(60) not null
			)
	
		CREATE UNIQUE INDEX uq_product_category_rules
		 ON product_category_rules (step_priority, product_name_included, product_name_excluded, product_type, product_subtype, product_category, product_subcategory, producer)
	END
GO


/* ���������� ������� ������ ��� ������������� �������� */ 
/* ��� ������� �������, � ��� ������, ��� ������ ����������� ������ ������� �������������, �� ��� ���������� "�����������", � ���� �� ��������� �� � ��� ������� ������ */
DROP PROCEDURE IF EXISTS  FILL_product_category_rules
GO
CREATE PROCEDURE FILL_product_category_rules 
	AS
	BEGIN
			INSERT INTO product_category_rules 
			(step_priority,		product_name_included,	product_name_excluded,	product_type,	product_subtype,	product_category,		product_subcategory,		producer)
			VALUES
			(1,					'%����%�������%',		'',					'��������',		'����',					'���� �����������',			'',					''),
			(2,					'%����%',				'',					'��������',		'����',					'���� �������',				'',					''),
			(11,				'%������%��������%',	'',					'',				'',						'',							'',					'�� ����'),
			(1,					'���� %',				'',					'��������',		'����/������������',	'����',						'',					''),
			(1,					'�������� %',			'',					'��������',		'����/������������',	'��������',					'',					''),
			(1,					'���� ����� %',			'',					'��������',		'����/������������',	'���� �����',				'',					''),
			(1,					'������� %',			'',					'��������',		'����/������������',	'�������',					'',					''),
			(1,					'�����%',				'',					'��������',		'����/������������',	'�����',					'',					''),
			(1,					'%��������%',			'',					'��������',		'����/������������',	'��������',					'',					''),
			(1,					'%������%',				'',					'��������',		'����/������������',	'������',					'',					''),
			(1,					'������%������%',		'',					'��������',		'����/������������',	'������ ������',			'',					''),
			(1,					'������%������%',		'',					'��������',		'����/������������',	'������ ������',			'',					''),
			(1,					'%�����%',				'%������%',			'��������',		'����/������������',	'�����',					'',					''),
			(1,					'�������%',				'%�����%',			'��������',		'����/������������',	'�������',					'',					''),
			(1,					'�������� �������%',	'%�����%',			'��������',		'����/������������',	'�������� �������',			'',					''),
			(1,					'�������� �������%',	'%�����%',			'��������',		'����/������������',	'�������� �������',			'',					''),
			(2,					'���� %',				'',					'��������',		'����/������������',	'����',						'',					''),

			(1,					'%����%����%',			'',				'��������',		'���������/�������������',	'������� ����',				'',					''),
			(1,					'%����%��������%',		'',				'��������',		'���������/�������������',	'������� ����',				'',					''),
			(1,					'%����%�����%',			'',				'��������',		'���������/�������������',	'������� ����',				'',					''),

			(1,					'%������%����%�����%',	'',				'��������',		'���������/�������������',	'������',					'���� �����',			''),

			(1,					'���� ��������%',		'',					'��������',		'����/�����',			'��������',					'���� ��������',		''),
			(2,					'���� �����%',			'',					'��������',		'����/�����',			'���� �����',				'���� �����',			''),
			(2,					'%�������%',			'',					'��������',		'����/�����',			'��������',					'���� �����',			''),
			(1,					'��������%',			'',					'��������',		'����/�����',			'��������',					'',					''),
			(1,					'������ %',				'',					'��������',		'����/�����',			'������',					'',					''),
			(1,					'������� �������%',		'',					'��������',		'����/�����',			'������',		'',					''),
			(1,					'%����� �������%',		'',					'��������',		'����/�����',			'������',		'',					''),
			(1,					'%����%�������%',		'',					'��������',		'����/�����',			'������',		'',					''),
			(1,					'����� ��� �������%',		'',					'��������',		'����/�����',		'����� ��� �������',		'',					''),

			(1,					'%��� ���������%',		'',					'��������',		'����',					'��� ���������',			'',					''),
			(1,					'%��� ����������%',		'',					'��������',		'����',					'��� ����������',			'',					''),
			(1,					'%������ �������%',		'',					'��������',		'����',					'������ �������',			'',					''),
			(1,					'��� %',				'',					'��������',		'����',					'��� %',					'',					''),

			(1,					'������ %',					'',					'��������',		'�������� ��������',	'������',					'',					''),
			(1,					'%������ %',				'',					'��������',		'�������� ��������',	'������',					'',					''),
			(1,					'%������� %',				'',					'��������',		'�������� ��������',	'�����',					'',					''),
			(1,					'%������� %',				'',					'��������',		'�������� ��������',	'�������',					'',					''),
			(1,					'%������ %',				'',					'��������',		'�������� ��������',	'������',					'',					''),
			(1,					'%��������� %',				'',					'��������',		'�������� ��������',	'������',					'',					''),
			(1,					'������� ���������%',		'',					'��������',		'�������� ��������',	'������',					'',					''),
			(1,					'%��������� %',				'',					'��������',		'�������� ��������',	'������',					'',					''),
			(1,					'%����������%',				'',					'��������',		'�������� ��������',	'������',					'',					''),
			(1,					'%������ %',				'',					'��������',		'�������� ��������',	'������',					'',					''),
			(1,					'%����� %',					'',					'��������',		'�������� ��������',	'��������� �������',					'',			''),
			(1,					'������ ������%',			'',					'��������',		'�������� ��������',	'��������� �������',					'',					''),
			(1,					'%��������� �������%',		'',					'��������',		'�������� ��������',	'��������� �������',		'',					''),
			(1,					'%������� ���������%',		'',					'��������',		'�������� ��������',	'��������� �������',		'',					''),
			(1,					'������%',					'',					'��������',		'�������� ��������',	'������',					'',					''),
			(1,					'%���������������� ������%','',					'��������',		'�������� ��������',	'���������������� ������',	'',					''),
			(1,					'%����������%',				'',					'��������',		'�������� ��������',	'������',				'',					''),
			(1,					'��������%',				'',					'��������',		'�������� ��������',	'������',				'',					''),
			(1,					'��������� �����%',			'',					'��������',		'�������� ��������',	'������',				'',					''),
			(1,					'%�����%',					'',					'��������',		'�������� ��������',	'�����',					'',					''),
			(1,					'%����� �������������%',	'',					'��������',		'�������� ��������',	'����� �������������',		'',					''),
			(1,					'���%����%',				'',					'��������',		'�������� ��������',	'�������� ������',				'',					''),
			(1,					'��������%',				'',					'��������',		'�������� ��������',	'�������� ������',				'',					''),
			(1,					'����� ����%',				'',					'��������',		'�������� ��������',	'����� ����',					'',					''),
			(1,					'�������%',					'',					'��������',		'�������� ��������',	'�������',					'',					''),
			(1,					'�������� �%',				'',					'��������',		'�������� ��������',	'�������� �������',			'',					''),
			(1,					'�������� ��������%',		'',					'��������',		'�������� ��������',	'�������� ��������',			'',					''),
			(1,					'�������� ��������%',		'',					'��������',		'�������� ��������',	'�������� ��������',			'',					''),
			(1,					'�������� ����%',			'',					'��������',		'�������� ��������',	'�������� ��������',			'',					''),
			(1,					'�������%',					'',					'��������',		'�������� ��������',	'������',			'',					''),
			(1,					'������%',					'',					'��������',		'�������� ��������',	'������',			'',					''),

			
			(1,					'���%����%',				'',					'��������',		'������� �������',	'�������� ������� ��� �����',		'',				''),
			(1,					'����%Ҩ��%',				'',					'��������',		'������� �������',	'���� ��� �����',		'',				''),
			(1,					'%����%����%',				'',					'��������',		'������� �������',	'���� ��� �����',		'',				''),
			
			(1,					'%�����%',					'',					'��������',		'�����������/�����',	'�����',					'',					''),
			(1,					'%�������%',				'%�������[�-�]%',	'��������',		'�����������/�����',	'�������',					'',					''),
			(1,					'���������� ����%',			'',					'��������',		'�����������/�����',	'�������',					'',					''),
			(1,					'����%',					'',					'��������',		'�����������/�����',	'����',						'',					''),
			(1,					'������ %',					'',					'��������',		'�����������/�����',	'������',					'',					''),
			(1,					'������%����%',				'',					'��������',		'�����������/�����',	'������ �������',			'',					''),
			(1,					'%�������%�������%',		'',					'��������',		'�����������/�����',	'������� �������',			'',					''),
			(2,					'%������%',					'��������%',		'��������',		'�����������/�����',	'������� ��������',			'',					''),
			(1,					'%�������� �������%',		'',					'��������',		'�����������/�����',	'�����',					'',					''),
			(1,					'%������%',					'%�������%',		'��������',		'�����������/�����',	'������',					'',					''),
			(1,					'%�������� �������%',		'',					'��������',		'�����������/�����',	'�������� �������',			'',					''),
			(1,					'%�������� �����%',			'',					'��������',		'�����������/�����',	'�������� �����',			'',					''),
			(1,					'������%� / �%',			'',					'��������',		'�����������/�����',	'������ � � / �',			'',					''),
			(1,					'������%�/�%',			'',					'��������',		'�����������/�����',	'������ � � / �',			'',					''),
			(1,					'�������%',					'',					'��������',		'�����������/�����',	'�������%',					'',					''),

			(1,					'�������� %',				'',					'��������',		'������/�����',		'��������',						'',					''),
			(1,					'��������%',				'',					'��������',		'������/�����',		'��������',						'',					''),
			(1,					'���������%',				'',					'��������',		'������/�����',		'���������',					'',					''),
			(1,					'������%',					'',					'��������',		'������/�����',		'������',						'',					''),
			(1,					'�������%',					'',					'��������',		'������/�����',		'�������',						'',					''),
			(1,					'�����%',					'',					'��������',		'������/�����',		'�����',						'',					''),
			(1,					'������%',					'',					'��������',		'������/�����',		'������',						'',					''),
			(1,					'�����%',					'',					'��������',		'������/�����',		'������',						'',					''),
			(1,					'"�����%',					'',					'��������',		'������/�����',		'������',						'',					''),
			(1,					'%������ %',				'',					'��������',		'������/�����',		'������',						'',					''),
			(1,					'������ %',					'',					'��������',		'������/�����',		'������',						'',					''),
			(1,					'���� %',					'',					'��������',		'������/�����',		'����',							'',					''),
			(1,					'���������%',				'',					'��������',		'������/�����',		'���������',					'',					''),
			(1,					'%���������%',				'',					'��������',		'������/�����',		'���������',					'',					''),
			(1,					'�������� %',				'',					'��������',		'������/�����',		'���������',					'',					''),
			(1,					'�������%',					'',					'��������',		'������/�����',		'�������',						'',					''),
			(1,					'��������� %',				'',					'��������',		'������/�����',		'���������',					'',					''),
			(1,					'������ %',					'',					'��������',		'������/�����',		'������',						'',					''),
			(1,					'�����%',					'',					'��������',		'������/�����',		'�����',						'',					''),
			(1,					'��������%',				'',					'��������',		'������/�����',		'��������',						'',					''),
			(1,					'��������� %',				'',					'��������',		'������/�����',		'���������',					'',					''),
			(1,					'������%',					'',					'��������',		'������/�����',		'������',						'',					''),
			(1,					'�����%',					'',					'��������',		'������/�����',		'�����',						'',					''),
			(1,					'������%',					'������[�-�]%',		'��������',		'������/�����',		'������',						'',					''),
			(1,					'����%',					'',					'��������',		'������/�����',		'����',							'',					''),
			(1,					'%����� ��������%',			'',					'��������',		'������/�����',		'��������',						'',					''),
			(1,					'������%',					'',					'��������',		'������/�����',		'������',						'',					''),
			(1,					'�����%',					'',					'��������',		'������/�����',		'�����',						'',					''),
			(1,					'�����%',					'',					'��������',		'������/�����',		'�����',						'',					''),
			(1,					'�������,%',				'',					'��������',		'������/�����',		'�������',						'',					''),
			
			(1,					'����� %',					'',					'��������',		'�����/�����',		'�����',						'',					''),
			(1,					'���������� %',				'',					'��������',		'�����/�����',		'����������',					'',					''),
			(1,					'������ %',					'',					'��������',		'�����/�����',		'������',						'',					''),
			(1,					'������ %',					'',					'��������',		'�����/�����',		'������',						'',					''),
			(1,					'������� %',				'',					'��������',		'�����/�����',		'�������',						'',					''),
			(1,					'������� %',				'',					'��������',		'�����/�����',		'������� ����',					'',					''),
			(1,					'������ %',					'',					'��������',		'�����/�����',		'������� ����',					'',					''),
			(1,					'����� %',					'',					'��������',		'�����/�����',		'�����',						'',					''),
			(1,					'��������� ������� %',		'',					'��������',		'�����/�����',		'��������� �������',			'',					''),
			(1,					'������� ����%',			'',					'��������',		'�����/�����',		'������� ����',					'',					''),
			(1,					'�������� �����%',			'',					'��������',		'�����/�����',		'�������� �����',				'',					''),

			(1,					'%����%��� ����%',			'',					'��������',		'����',				'���� ��� ����',				'',					''),
			(1,					'%����%�����%',				'',					'��������',		'����',				'���� ������������',			'',					''),
						
			(1,					'%�������%',				'',					'��������',		'�������',			'�������',						'',					''),
			(1,					'%����%',					'',					'��������',		'�������',			'����',							'',					''),
			(1,					'%������� ����������%',		'',					'��������',		'�������',			'������� ����������',			'',					''),
			(1,					'%����%',	'%�������%�������%',				'��������',		'�������',			'����',							'',					''),
			(1,					'��� %',					'',					'��������',		'�������',			'���',							'',					''),
			(1,					'������ %',					'',					'��������',		'�������',			'������',						'',					''),
			(1,					'������ %',					'',					'��������',		'�������',			'������',						'',					''),
			(2,					'%����%',		'%[�-�]����%',					'��������',		'�������',			'����',							'',					''),

			(1,					'%����%',			'%�����%',					'��������',		'������������� �������/�������',	'����',			'',					''),
			(1,					'%����� %',				   '',					'��������',		'������������� �������/�������',	'�����',		'',					''),
			(1,					'%�����%',				   '',					'��������',		'������������� �������/�������',	'�����',		'',					''),
			(1,					'����� %',				   '',					'��������',		'������������� �������/�������',	'�����',		'',					''),
			(1,					'�������%',				   '',					'��������',		'������������� �������/�������',	'�������',		'',					''),
			(1,					'�����%',				   '',					'��������',		'������������� �������/�������',	'�������',		'',					''),
			(1,					'%�������%',		       '',					'��������',		'������������� �������/�������',	'�������',		'',					''),
			(1,					'%�����%',			'%��������%',				'��������',		'������������� �������/�������',	'����',			'',					''),
						
			(1,					'������� ���%',				'',					'��������',		'������� ��� ��������',		'������� ��� ��������',	'',					''),	


			(1,					'�������� %',				 '',					'��������',		'�����/������',	'��������',						'',					''),
			(1,					'������� ������� %',		 '',					'��������',		'�����/������',	'������� �������',				'',					''),
			(1,					'%������� �������%',		 '',					'��������',		'�����/������',	'������� �������',				'',					''),
			(1,					'������%',		 '������%����%',					'��������',		'�����/������',	'������',						'',					''),
			(1,					'%�������%',				 '',					'��������',		'�����/������',	'�������',						'',					''),
			(1,					'�������%',					 '',					'��������',		'�����/������',	'��������',						'',					''),
			(1,					'�������%',					 '',					'��������',		'�����/������',	'�������',						'',					''),
			(1,					'�����%',				'%�����%',					'��������',		'�����/������',	'�����',						'',					''),
			(1,					'%�������%',				 '',					'��������',		'�����/������',	'�������',						'',					''),
			(2,					'��������� %',				 '',					'��������',		'�����/������',	'��������',						'',					''),
			(1,					'���%',						 '',					'��������',		'�����/������',	'���',						'',					''),
			(1,					'���������%',				 '',					'��������',		'�����/������',	'���������',						'',					''),
			(1,					'�����%',					'',						'��������',		'�����/������',	'�����',						'',					''),
			(1,					'������%',					 '',					'��������',		'�����/������',	'������',						'',					''),
			(1,					'������ %',					 '',					'��������',		'�����/������',	'������',						'',					''),
			(1,					'������%',					'',						'��������',		'�����/������',	'������',						'',					''),
			(1,					'����� ������%',			 '',					'��������',		'�����/������',	'����� ������',					'',					''),
			(1,					'�����%�%��������%',		'',						'��������',		'�����/������',	'�����',						'',					''),
			(1,					'�����%�������%',			'',						'��������',		'�����/������',	'�����',						'',					''),
			(1,					'�����%�������%',			'',						'��������',		'�����/������',	'�����',						'',					''),
			(1,					'�����%����%',				'',						'��������',		'�����/������',	'�����',						'',					''),
			(1,					'������%',					'',						'��������',		'�����/������',	'������',						'',					''),
			(1,					'�����%',					'',						'��������',		'�����/������',	'�����',						'',					''),
			(1,					'��������%',				'',						'��������',		'�����/������',	'��������',						'',					''),
			(1,					'������%',					'',						'��������',		'�����/������',	'������',						'',					''),
						
			(2,					'�����%',				 '%��������%',				'��������',		'���������/�������������',			'�����',		'',					''),
			(1,					'%������%',					 '',					'��������',		'���������/�������������',			'�������',		'',					''),
			(1,					'�������%',					 '',					'��������',		'���������/�������������',			'�������',		'',					''),
			(1,					'%���������%',				 '',					'��������',		'���������/�������������',			'���������',	'',					''),
			(1,					'������ %',					 '',					'��������',		'���������/�������������',			'������',		'',					''),
			(1,					'%������%',					 '',					'��������',		'���������/�������������',			'������',		'',					''),
			(1,					'�������� %',				 '',					'��������',		'���������/�������������',			'��������',		'',					''),
			(1,					'�������� %',				 '',					'��������',		'���������/�������������',			'��������',		'',					''),
			(1,					'�������� %',				 '',					'��������',		'���������/�������������',			'��������',		'',					''),
			(1,					'������� %',				 '',					'��������',		'���������/�������������',			'�������',		'',					''),
			(1,					'����� %',					 '',					'��������',		'���������/�������������',			'�����',		'',					''),
			(1,					'%�����%',					 '',					'��������',		'���������/�������������',			'�����',		'',					''),
			(1,					'����� %',					 '',					'��������',		'���������/�������������',			'�����',		'',					''),
			(1,					'��������� %',				 '',					'��������',		'���������/�������������',			'���������',		'',					''),
			(1,					'%������%',					 '',					'��������',		'���������/�������������',			'������',		'',					''),
			(1,					'%������%',					 '',					'��������',		'���������/�������������',			'�������',		'',					''),
			(1,					'%�������%�������%',		 '',					'��������',		'���������/�������������',			'������� �������','',				''),
			(1,					'����� %',					 '',					'��������',		'���������/�������������',			'�����',		'',					''),
			(1,					'%��������%',				 '',					'��������',		'���������/�������������',			'��������',		'',					''),
			(1,					'%������%',					 '',					'��������',		'���������/�������������',			'������� �����',		'',					''),
			(2,					'%��������%',				 '',					'��������',		'���������/�������������',			'��������',		'',					''),
			(1,					'%������%',					 '',					'��������',		'���������/�������������',			'�������',		'',					''),
			(1,					'������%',					 '',					'��������',		'���������/�������������',			'�������',		'',					''),
			(1,					'�������%',					 '',					'��������',		'���������/�������������',			'�������',		'',					''),
			(1,					'%������%',					 '',					'��������',		'���������/�������������',			'�������',		'',					''),
			(2,					'%���%',					 '',					'��������',		'���������/�������������',			'���',			'',					''),
			(1,					'%�������%',				 '',					'��������',		'���������/�������������',			'������� �����','',					''),
			(1,					'%���������%',				 '',					'��������',		'���������/�������������',			'���������',	'',					''),
			(1,					'%��������%',				 '',					'��������',		'���������/�������������',			'��������',		'',					''),
			(1,					'%����%',					 '',					'��������',		'���������/�������������',			'����',			'',					''),
			(1,					'%�������%��%�����%',		 '',					'��������',		'���������/�������������',			'������� ��-��������',	'',			''),
			(1,					'%�� %',					 '',					'��������',		'���������/�������������',			'��',			'',					''),
			(1,					'%������%',					 '',					'��������',		'���������/�������������',			'������� �����','',					''),
			(1,					'%������%',					 '',					'��������',		'���������/�������������',			'������� �����','',					''),
			(1,					'%������%',					 '',					'��������',		'���������/�������������',			'������� �����','',					''),
			(1,					'%�������%',				 '',					'��������',		'���������/�������������',			'������� �����','',					''),
			(1,					'%������%',					 '',					'��������',		'���������/�������������',			'������� �����','',					''),
			(1,					'%������%',					 '',					'��������',		'���������/�������������',			'������� �����','',					''),
			(1,					'%������%',					 '',					'��������',		'���������/�������������',			'������� �����','',					''),
			(1,					'%����%',					 '',					'��������',		'���������/�������������',			'����',			'',					''),
			(1,					'%��������%',				 '',					'��������',		'���������/�������������',			'��������',		'',					''),
			(1,					'�����%',					 '',					'��������',		'���������/�������������',			'�����',		'',					''),
			(1,					'%��-���������%',			 '',					'��������',		'���������/�������������',			'������� �����','',					''),
			(1,					'�������%',					 '',					'��������',		'���������/�������������',			'�������',		'',					''),
			(1,					'���%���%',					 '',					'��������',		'���������/�������������',			'���-���',		'',					''),
			(1,					'����%',					 '',					'��������',		'���������/�������������',			'����',			'',					''),
			(1,					'%�����%�������%',			'',						'��������',		'���������/�������������',			'�����',		'',					''),
			(1,					'������%',					'',						'��������',		'���������/�������������',			'������',		'',					''),
			(1,					'%�������%�����%',			'',						'��������',		'���������/�������������',			'�����',		'',					''),
			
			(1,					'%��������%',				'',						'��������',		'������������ �������/��������',	'��������',		'',					''),
			(1,					'%�������%',				'',						'��������',		'������������ �������/��������',	'�������',		'',					''),
			(1,					'%�������%',				'',						'��������',		'������������ �������/��������',	'�������',		'',					''),
			(1,					'������� %',				'',						'��������',		'������������ �������/��������',	'�������',		'',					''),
						
			(1,					'����� %',					'',						'��������',		'������������ �������/��������',	'�����',		'',					''),
			(1,					'���� %',					'',						'��������',		'������������ �������/��������',	'����',			'',					''),
			(2,					'������ �������%',				'',						'��������',		'������������ �������/��������',	'������',		'',					''),
			(2,					'%� ������%',				'',						'��������',		'������������ �������/��������',	'������',		'',					''),
			(2,					'%� �����%',				'',						'��������',		'������������ �������/��������',	'������',		'',					''),
			(1,					'�������� %',				'',						'��������',		'������������ �������/��������',	'��������',		'',					''),
			(1,					'����� %',					'',						'��������',		'������������ �������/��������',	'�������',		'',					''),
			(1,					'����� %',					'',						'��������',		'������������ �������/��������',	'�����',		'',					''),
			(1,					'������ %',					'',						'��������',		'������������ �������/��������',	'������',		'',					''),
			(1,					'������� %',				'',						'��������',		'������������ �������/��������',	'�������',		'',					''),
			(1,					'�������� %',				'',						'��������',		'������������ �������/��������',	'�����',		'',					''),
			(1,					'%��������%',				'',						'��������',		'������������ �������/��������',	'��������',		'',					''),
			(1,					'%���������%',				'',						'��������',		'������������ �������/��������',	'���������',	'',					''),
			(1,					'%�������%',				'',						'��������',		'������������ �������/��������',	'��������',		'',					''),
			(1,					'%���.�������%',			'',						'��������',		'������������ �������/��������',	'�������',		'',					''),
			(1,					'%�����%������%',			'',						'��������',		'������������ �������/��������',	'�������',		'',					''),
			(1,					'%�����%',					'%���������%',			'��������',		'������������ �������/��������',	'�����',		'',					''),
			(1,					'%�����%',					'%���������%',			'��������',		'������������ �������/��������',	'�����',		'',					''),
			(1,					'������ %',					'',						'��������',		'������������ �������/��������',	'������',		'',					''),
			(1,					'������� %',				'',						'��������',		'������������ �������/��������',	'�������',		'',					''),
			(1,					'%���-���%',				'',						'��������',		'������������ �������/��������',	'�������',		'',					''),
			(1,					'�������%�������%',			'',						'��������',		'������������ �������/��������',	'�������',		'',					''),
			(1,					'���������� %',				'',						'��������',		'������������ �������/��������',	'�������',		'',					''),
			(1,					'%������%',					'',						'��������',		'������������ �������/��������',	'������',		'',					''),
			(1,					'%������%',					'',						'��������',		'������������ �������/��������',	'�������',		'',					''),
			(1,					'%����-����%',				'',						'��������',		'������������ �������/��������',	'��������',		'',					''),
			(1,					'%������%',					'',						'��������',		'������������ �������/��������',	'������',		'',					''),
			(1,					'%����%���%',				'',						'��������',		'������������ �������/��������',	'�������',		'',					''),
			(1,					'�����%',					'',						'��������',		'������������ �������/��������',	'�����',		'',					''),
			(1,					'�����%������%',			'',						'��������',		'������������ �������/��������',	'�����',		'',					'������'),
			(1,					'%�������%',				'',						'��������',		'������������ �������/��������',	'�������',		'',					''),
			(1,					'����%',					'',						'��������',		'������������ �������/��������',	'����',			'',					''),
			(1,					'%����� ���������%',		'',						'��������',		'������������ �������/��������',	'������',		'',					''),
			(2,					'%����%',			'%����� �%' ,					'��������',		'������������ �������/��������',	'����',			'',					''),

			(1,					'%��������%',				'',						'��������',		'�������',							'��������',		'',					''),
			(1,					'�����%',			'���������%',					'��������',		'�������',							'�����',		'',					''),
			(1,					'�������%',					'',					'��������',		'�������',								'�������',		'',					''),
			(1,					'%����%',					'',						'��������',		'�������',							'����',			'',					''),
			(1,					'%������%',					'',						'��������',		'�������',							'������',		'',					''),
			(1,					'%�����%',					'',						'��������',		'�������',							'�����',		'',					''),
			(1,					'��� %',					'',						'��������',		'�������',							'���',			'',					''),
			(1,					'%�������%',				'',						'��������',		'�������',							'��������',		'',					''),
			(1,					'���� %',					'',					'��������',		'�������',							'����',			'',					''),
			(1,					'�������%',					'',						'��������',		'�������',							'�������',		'',					''),
			(1,					'�����%�����%',				'',						'��������',		'�������',							'����� �������','',					''),
			(1,					'�����%',					'����� ����%',			'��������',		'�������',							'�����',		'',					''),
			(1,					'���. �������%',			'',						'��������',		'�������',							'������',		'',					''),
			(1,					'�������%',					'',						'��������',		'�������',							'�������',		'',					''),
			(1,					'����� �����%',	'',						'��������',		'�������',		'����� ��� �������� �������������',	'',					''),
			(1,					'������������ ���� �/�%',	'',						'��������',		'�������',		'����� ��� �������� �������������',	'',					''),
			(1,					'���� �/�%',				'',						'��������',		'�������',		'����� ��� �������� �������������',	'',					''),
			(1,					'����� �/�%',				'',						'��������',		'�������',		'����� ��� �������� �������������',	'',					''),
			(1,					'��� �/�%',					'',						'��������',		'�������',		'����� ��� �������� �������������',	'',					''),
			(1,					'������%',					'',						'��������',		'�������',							'������',		'',					''),
			(1,					'���� %',					'',						'��������',		'�������',							'����',			'',					''),
			(1,					'�����.�������%',			'',						'��������',		'�������',							'������',		'',					''),
			(1,					'%�������%',				'',						'��������',		'�������',							'�������',		'',					''),
			(1,					'����%',					'',						'��������',		'�������',							'����',			'',					''),
			(1,					'%�����%�����%',			'',						'��������',		'�������',							'����� ��� �������� �������������',	'',	''),
			(1,					'�����%',					'',						'��������',		'�������',							'�����',			'',					''),
			(1,					'��������%',				'',						'��������',		'�������',							'��������',			'',					''),
			(1,					'������%',					'',						'��������',		'�������',							'������',			'',					''),
			(1,					'%�����%',					'',						'��������',		'�������',							'�����',			'',					''),
			(1,					'���%����%',				'',						'��������',		'�������',							'��� ����',			'',					''),

			(2,					'����� %',					 '%�������� ����%',		'��������',		'������� ����',						'�����',			'',					''),			
			(1,					'�������� %',				'',						'��������',		'������� ����',						'��������',			'',					''),			
			(1,					'�����%',					'',						'��������',		'������� ����',						'�����',			'',					''),
			
			(1,					'%���������%',				'',						'������',		'������ ��� ���� � ����',			'���������',		'',					''),
			(1,					'%������%',					'',						'������',		'������ ��� ���� � ����',			'������',			'',					''),
			(1,					'%�����%',					'',						'������',		'������ ��� ���� � ����',			'�����',			'',					''),
			(1,					'%������%',				'%����� ��� �����%',		'������',		'������ ��� ���� � ����',			'������',			'',					''),
			(1,					'%�����%���������%',		'',						'������',		'������ ��� ���� � ����',			'����� ��� ���������',	'',				''),
			(1,					'%���%��������%',			'',						'������',		'������ ��� ���� � ����',			'��� ��������',		'',					''),
			(1,					'%����%',					'',						'������',		'������ ��� ���� � ����',			'��������',			'',					''),
			(1,					'%�%���������%',			'',						'������',		'������ ��� ���� � ����',			'��������',		'',					''),
			(1,					'%��������%',				'',						'������',		'������ ��� ���� � ����',			'������� �������',	'',					''),
			(1,					'%����� ��� ���������������%',	'',					'������',		'������ ��� ���� � ����',			'����� ��� ���������������',	'',					''),
			(1,					'%�%�����%',				'',						'������',		'������ ��� ���� � ����',			'��� ����� ������',	'',					''),
			(1,					'%����%��%',				'',						'������',		'������ ��� ���� � ����',			'������� �����',	'�������� ��������',					''),
			(1,					'%�%������%',				'',						'������',		'������ ��� ���� � ����',			'��� ������',	'',					''),
			(1,					'%����%�����%',				'',						'������',		'������ ��� ���� � ����',			'��� ������',	'',					''),
			(1,					'%������������%',			'',						'������',		'������ ��� ���� � ����',			'������� �����',	'������������',					''),
			(1,					'%�����%',					'',						'������',		'������ ��� ���� � ����',			'�����',	'',						''),
			(1,					'%��������%',				'',						'������',		'������ ��� ���� � ����',			'��������',	'',						''),
			(1,					'%����%',					'',						'������',		'������ ��� ���� � ����',			'����',	'',							''),
			(1,					'%�����%',					'',						'������',		'������ ��� ���� � ����',			'�����',	'',						''),
			(1,					'%�������%�����%',			'',						'������',		'������ ��� ���� � ����',			'������',	'',						''),
			(1,					'%������� �������%',		'',						'������',		'������ ��� ���� � ����',			'���������',	'',						''),
			(1,					'%����%',					'',						'������',		'������ ��� ���� � ����',			'���������',	'',						''),
			(1,					'%���������%',				'',						'������',		'������ ��� ���� � ����',			'������',	'',						''),
			(1,					'%�����%�����%',			'',						'������',		'������ ��� ���� � ����',			'�����',	'',						''),
			(1,					'%�����%������%',			'',						'������',		'������ ��� ���� � ����',			'�����',	'',						''),
			(1,					'%�����%�%���%',			'',						'������',		'������ ��� ���� � ����',			'�����',	'',						''),
			(1,					'%������%�%',				'',						'������',		'������ ��� ���� � ����',			'�����',	'',						''),
			(1,					'%������� ���%',			'',						'������',		'������ ��� ���� � ����',			'������',	'',						''),
			(1,					'%��������%���%',			'',						'������',		'������ ��� ���� � ����',			'��������',	'',						''),
			(1,					'�����%',					'',						'������',		'������ ��� ���� � ����',			'�����',	'',						''),
			(1,					'�����%',					'',						'������',		'������ ��� ���� � ����',			'�����',	'',						''),
			(1,					'������%',					'',						'������',		'������ ��� ���� � ����',			'������',	'',						''),
			(1,					'%����� �%',				'',						'������',		'������ ��� ���� � ����',			'�������� ���������',	'',			''),
			(1,					'%����� ���%',				'',						'������',		'������ ��� ���� � ����',			'��������',	'',					''),
			(1,					'%����� ���%',				'',						'������',		'������ ��� ���� � ����',			'��������',	'',					''),
			(1,					'%���������%',				'',						'������',		'������ ��� ���� � ����',			'��������',	'',					''),
			(1,					'%٨���%',					'',						'������',		'������ ��� ���� � ����',			'٨���',	'',					''),
			(1,					'%�����%',					'',						'������',		'������ ��� ���� � ����',			'�����',	'',					''),
			(1,					'%����������%',				'',						'������',		'������ ��� ���� � ����',			'������� �����',	'����������',					''),
			(1,					'%�������� ���������%',		'',						'������',		'������ ��� ���� � ����',			'��������',	'',					''),
			(1,					'�����%',					'',						'������',		'������ ��� ���� � ����',			'�����',	'',					''),
			(1,					'%�����%',					'',						'������',		'������ ��� ���� � ����',			'�����',	'',					''),
			(1,					'�����%',					'',						'������',		'������ ��� ���� � ����',			'�����',	'',					''),
			(1,					'������%',					'',						'������',		'������ ��� ���� � ����',			'������',	'',					''),
			(1,					'�������%',					'',						'������',		'������ ��� ���� � ����',			'�������',	'',					''),
			(1,					'%����������%',				'',						'������',		'������ ��� ���� � ����',			'������� �����','',					''),

			(1,					'%����%',					'',						'������',		'���������/�������',				'����',			'',					''),
			(1,					'%�������%',				'',						'������',		'���������/�������',				'�������','',					''),	
			(1,					'%�������%',				'',						'������',		'���������/�������',				'�������','',					''),	
			(1,					'%����%',					'',						'������',		'���������/�������',				'����','',							''),
			(1,					'����%�/�%',				'',						'������',		'���������/�������',				'���� ��� ���','',							''),		
			(1,					'%���������%',				'',						'������',		'���������/�������',				'���������','',					''),	
			(1,					'%������� ������%',			'',						'������',		'���������/�������',				'������� ������','',		''),	
			(1,					'%������%',					'',						'������',		'���������/�������',				'������','',						''),	
			(1,					'%�������%',				'',						'������',		'���������/�������',				'����������','',				''),	
			(1,					'%����%���%',				'',						'������',		'���������/�������',				'��������� ������','',			''),	
			(1,					'%����� %',				'%������%',					'������',		'���������/�������',				'�����','',					''),	
			(1,					'%�����%',					'',						'������',		'���������/�������',				'������������� ��������','',		''),	
			(1,					'%��������%',				'',						'������',		'���������/�������',				'������������� ��������','',	''),	
			(1,					'����� %�%������%',			'',						'������',		'���������/�������',				'������������� ��������','',''),	
			(1,					'��-�� �%',					'',						'������',		'���������/�������',				'������������� ��������','',		''),	
			(1,					'%��������%',				'',						'������',		'���������/�������',				'��������','',					''),	
			(1,					'� / � %',					'',						'������',		'���������/�������',				'������ �����','',					''),
			(1,					'�/� %',					'',						'������',		'���������/�������',				'������ �����','',					''),	
			(1,					'� / � %',					'',						'������',		'���������/�������',				'������ �����','',					''),
			(1,					'�/� %',					'',						'������',		'���������/�������',				'������ �����','',					''),


			(1,					'%��������%',				'',						'������',		'����������',						'��������','',					''),
			(1,					'%����� ��� �������%',		'',						'������',		'����������',						'����� ��� �������','',					''),
			(1,					'%�������%',				'',						'������',		'����������',						'�������','',					''),
			(1,					'%�������%',				'',						'������',		'����������',						'�������','',					''),
			(1,					'%�����%',					'',						'������',		'����������',						'�����','',					''),
			(1,					'%���������%',				'',						'������',		'����������',						'��� ���������','',					''),
			(1,					'%������%',					'',						'������',		'����������',						'������','',					''),
			(1,					'%������%',					'',						'������',		'����������',						'������','',					''),
			(1,					'%����%',					'',						'������',		'����������',						'����','',					''),
			(1,					'%��������%',				'',						'������',		'����������',						'��������','',					''),
			(1,					'%�������%',				'',						'������',		'����������',						'�������','',					''),
			(1,					'%��������%',				'',						'������',		'����������',						'��������','',					''),
			(1,					'%������%',					'%[�_�]������%',		'������',		'����������',						'������','',					''),
			(1,					'%������%',					'',						'������',		'����������',						'������','',					''),
			(1,					'%������%',					'',						'������',		'����������',						'������','',					''),
			(1,					'%�������%',				'',						'������',		'����������',						'�������','',					''),
			(1,					'%��������%',				'',						'������',		'����������',						'��������','',					''),
			
			(1,					'%������%',					'',						'������',		'�����/�������',					'������','',					''),
			(1,					'%������%',					'',						'������',		'�����/�������',					'������','',					''),
			(1,					'%�����%',					'',						'������',		'�����/�������',					'�����','',					''),
			(1,					'%��������%',				'',						'������',		'�����/�������',					'��������','',					''),
			
			(1,					'%����� ��� �����%',		'',						'������',		'������ ��� �����',					'�������','',					''),
			(1,					'%���%������%',				'',						'������',		'������ ��� �����',					'�����\��������','',					''),
			(1,					'%�������%',				'',						'������',		'������ ��� �����',					'�������','',					''),
			(1,					'���� �������%',			'',						'������',		'������ ��� �����',					'���� �� ������� �����','',					''),
			(1,					'%�������� ����%',			'',						'������',		'������ ��� �����',					'���� �� ������� �����','',					''),
			(1,					'%����������%',				'',						'������',		'������ ��� �����',					'����������','',				''),	
			
			(1,					'%�����������%',			'',						'������',		'������ ��� ��������',				'�����������','',					''),
			
			(1,					'%���������� �����%',		'',						'������',		'�������',							'���������� �����','',					''),
			(1,					'%%����� �����%',			'',						'������',		'�������',							'���������� �����','',					''),
			(1,					'%�����%�����%',			'',						'������',		'�������',							'���������� �����','',					''),
			(1,					'%����� ��� ��������%',		'',						'������',		'�������',							'���������� ������','',					''),
			(1,					'%���������� �������%',		'',						'������',		'�������',							'���������� �������','',					''),
			(1,					'%�������� �����%',			'',						'������',		'�������',							'�������� �����','',					''),
			(1,					'%���������� �����%',		'',						'������',		'�������',							'���������� �����','',					''),
			(1,					'%����� �������%',			'',						'������',		'�������',							'���������� �����','',					''),

			
			(1,					'%�����%',					'',						'������',		'������ ������',					'�����','',					''),
			(1,					'%����������%',				'',						'������',		'������ ������',					'����������','',					''),
			(1,					'%��������%',				'',						'������',		'������ ������',					'���������','',					''),
			(1,					'%���������%',				'',						'������',		'������ ������',					'���������','',					''),
			(1,					'%��������%',				'',						'������',		'������ ������',					'������������','',					''),
			(1,					'%���%�����%',				'',						'������',		'������ ������',					'����������� �������','',			''),
			(1,					'%�������%',				'',						'������',		'������ ������',					'��������','',					'')	


		END
GO


/*
EXTRACT_SUBSTRING
��������� ������� ��������� @SUBSTRING �� product_name � ���������� �� � temp_param
�������� �� temp_param ����� ���������� ��������� ����� ��������� � ���������� ������ �������
���������
	@SUBSTRING - ���������, ������� ���� �����
	(���� �� ����������) @remove_measure - �������, ������� �� ��������� ��������� �� product_name (���� �� ����������)
*/
DROP PROCEDURE IF EXISTS EXTRACT_SUBSTRING 
GO
CREATE PROCEDURE EXTRACT_SUBSTRING  (@SUBSTRING nvarchar(50))
	AS
	BEGIN
		UPDATE step0 SET temp_param=''
	
		UPDATE step0
		SET temp_param=@SUBSTRING,
			product_name = replace(product_name, @SUBSTRING, ''),
			parse_status = parse_status+10
		WHERE	product_name like concat('%',@SUBSTRING,'%')		

		/* ������ ������ ������� � ������� � ����� ������*/	  
		UPDATE step0
		SET product_name = rtrim(replace(product_name,'  ',' '))
	
	END
GO

/*
MULTIPLE_UPDATE
��������� ������� ���������, ��������������� ������ ���������� , �� product_name (�� ������� like @SUBSTRING)
� ���������� �������� ���������� � ��������������� �������
�������� �� temp_param ����� ���������� ��������� ����� ��������� � ���������� ������ �������

*/
DROP PROCEDURE IF EXISTS MULTIPLE_UPDATE 
GO
CREATE PROCEDURE MULTIPLE_UPDATE (	@SUBSTRING nvarchar(50),
									@PRODUCER	nvarchar(50),
									@PROD_TYPE nvarchar(50),
									@PROD_SUBTYPE nvarchar(50),
									@PROD_CATEGORY nvarchar(50),
									@PROD_SUBCATEGORY nvarchar(50))
	AS
	BEGIN
		UPDATE step0 SET temp_param=''
	
		UPDATE step0
		SET temp_param			= @SUBSTRING,
			product_name		= replace(replace(replace(replace(replace(replace(product_name, @SUBSTRING, ''), @PRODUCER, ''), @PROD_TYPE, ''), @PROD_SUBTYPE, ''), @PROD_CATEGORY, ''), @PROD_SUBCATEGORY, ''),
			param15_producer	= CASE WHEN @PRODUCER !=''			and param15_producer=''		THEN @PRODUCER			ELSE param15_producer		END,
			param16_type		= CASE WHEN @PROD_TYPE !=''			and param16_type=''			THEN @PROD_TYPE			ELSE param16_type			END,
			param17_subtype		= CASE WHEN @PROD_SUBTYPE !=''		and param17_subtype=''		THEN @PROD_SUBTYPE		ELSE param17_subtype		END,
			param18_category	= CASE WHEN @PROD_CATEGORY !=''		and param18_category=''		THEN @PROD_CATEGORY		ELSE param18_category		END,
			param19_subcategory = CASE WHEN @PROD_SUBCATEGORY !=''	and param19_subcategory=''	THEN @PROD_SUBCATEGORY	ELSE param19_subcategory	END,
			parse_status = parse_status+1000
		WHERE	product_name like concat('%',@SUBSTRING,'%')		

		/* ������ ������ ������� � ������� � ����� ������*/	  
		UPDATE step0
		SET product_name = rtrim(replace(product_name,'  ',' '))
	
	END
GO


/*
MULTIPLE_UPDATE_PRODSUBTYPE
��������� ������� ���������, ��������������� ������ ���������� , �� product_name  (�� ������� like @SUBSTRING and like @PROD_TYPE)
� ���������� �������� ���������� � ��������������� �������
�������� �� temp_param ����� ���������� ��������� ����� ��������� � ���������� ������ �������

*/
DROP PROCEDURE IF EXISTS MULTIPLE_UPDATE_PRODSUBTYPE 
GO
CREATE PROCEDURE MULTIPLE_UPDATE_PRODSUBTYPE (	@SUBSTRING nvarchar(50),
												@PRODUCER	nvarchar(50),
												@PROD_TYPE nvarchar(50),
												@PROD_SUBTYPE nvarchar(50),
												@PROD_CATEGORY nvarchar(50),
												@PROD_SUBCATEGORY nvarchar(50))
	AS
	BEGIN
		UPDATE step0 SET temp_param=''
	
		UPDATE step0
		SET temp_param			= @SUBSTRING,
			product_name		= replace(replace(replace(replace(replace(replace(product_name, @SUBSTRING, ''), @PRODUCER, ''), @PROD_TYPE, ''), @PROD_SUBTYPE, ''), @PROD_CATEGORY, ''), @PROD_SUBCATEGORY, ''),
			param15_producer	= CASE WHEN @PRODUCER !=''			and param15_producer=''		THEN @PRODUCER				ELSE param15_producer		END,
			param16_type		= CASE WHEN @PROD_TYPE !=''			and param16_type=''			THEN @PROD_TYPE				ELSE param16_type			END,
			param17_subtype		= CASE WHEN @PROD_SUBTYPE !=''		and param17_subtype=''		THEN RTRIM(@PROD_SUBTYPE)	ELSE param17_subtype		END,
			param18_category	= CASE WHEN @PROD_CATEGORY !=''		and param18_category=''		THEN @PROD_CATEGORY			ELSE param18_category		END,
			param19_subcategory = CASE WHEN @PROD_SUBCATEGORY !=''	and param19_subcategory=''	THEN @PROD_SUBCATEGORY		ELSE param19_subcategory	END,
			parse_status = parse_status+100
		WHERE	product_name like concat('%',@SUBSTRING,'%')		
			and product_name like concat('%',@PROD_SUBTYPE,'%')		

		/* ������ ������ ������� � ������� � ����� ������*/	  
		UPDATE step0
		SET product_name = rtrim(replace(product_name,'  ',' '))
	
	END
GO


/*
MULTIPLE_UPDATE_PRODCATEGORY
��������� ������� ���������, ��������������� ������ ���������� , �� product_name  (�� ������� like @SUBSTRING and like @PROD_CATEGORY)
� ���������� �������� ���������� � ��������������� �������
�������� �� temp_param ����� ���������� ��������� ����� ��������� � ���������� ������ �������

*/
DROP PROCEDURE IF EXISTS  MULTIPLE_UPDATE_PRODCATEGORY 
GO
CREATE PROCEDURE MULTIPLE_UPDATE_PRODCATEGORY (	@SUBSTRING nvarchar(50),
											  @PRODUCER	nvarchar(50),
											  @PROD_TYPE nvarchar(50),
											  @PROD_SUBTYPE nvarchar(50),
											  @PROD_CATEGORY nvarchar(50),
											  @PROD_SUBCATEGORY nvarchar(50))
	AS
	BEGIN
		UPDATE step0 SET temp_param=''
	
		UPDATE step0
		SET temp_param			= @SUBSTRING,
			product_name		= replace(replace(replace(replace(replace(replace(product_name, @SUBSTRING, ''), @PRODUCER, ''), @PROD_CATEGORY, ''), @PROD_TYPE, ''), @PROD_SUBTYPE, ''), @PROD_SUBCATEGORY, ''),
			param15_producer	= CASE WHEN @PRODUCER !=''			and param15_producer=''		THEN @PRODUCER				ELSE param15_producer		END,
			param16_type		= CASE WHEN @PROD_TYPE !=''			and param16_type=''			THEN @PROD_TYPE				ELSE param16_type			END,
			param17_subtype		= CASE WHEN @PROD_SUBTYPE !=''		and param17_subtype=''		THEN @PROD_SUBTYPE			ELSE param17_subtype		END,
			param18_category	= CASE WHEN @PROD_CATEGORY !=''		and param18_category=''		THEN RTRIM(@PROD_CATEGORY)	ELSE param18_category		END,
			param19_subcategory = CASE WHEN @PROD_SUBCATEGORY !=''	and param19_subcategory=''	THEN @PROD_SUBCATEGORY		ELSE param19_subcategory	END,
			parse_status = parse_status+100
		WHERE	product_name like concat('%',@SUBSTRING,'%')		
			and product_name like concat('%',@PROD_CATEGORY,'%')		

		/* ������ ������ ������� � ������� � ����� ������*/	  
		UPDATE step0
		SET product_name = rtrim(replace(product_name,'  ',' '))
	
	END
GO


/* 
PRODUCT_NAME_PARCING
��������� ��������� ������������ �������� � ��������� ���� � ����������� ��������
*/
DROP PROCEDURE IF EXISTS PRODUCT_NAME_PARCING
GO
CREATE PROCEDURE PRODUCT_NAME_PARCING
	AS
	BEGIN
	--STEP0: ������ ������� � ������ ��� ����������
		DROP TABLE IF EXISTS step0 
		SELECT	product_id,
				RTRIM(substring(product_name, 1, len(product_name)-1)) as product_name,  --������� ��������� ���������� ������ � ����� ������ � ������� � ����� ������

				0 as parse_status,
				'                                                 ' as temp_param,
				'                                ' as param1_pricefor,
				'                                ' as param1_ves,

				'                                ' as param3_kg,
				'                                ' as param3_gramm,		
				'                                ' as param3_special,
			
				'                                ' as param4_litr,
				'                                ' as param4_ml,
				'                                ' as param4_special,

				'                                ' as param5_items,
				'                                ' as param5_special,

				'                                ' as param6_tara,

				'                                ' as param7_percent,

				'                                ' as param8_sm,
				'                                ' as param8_m,
				'                                ' as param8_special,  

				'                                ' as param10_num3,
				'                                ' as param10_num2,
				'                                ' as param10_num1,

				'                                ' as param11_CCC,
		
				'                                ' as param12,
				'                                ' as param14_pch_part,

				'                                                                   ' as param15_producer,
				'                                ' as param16_type,
				'                                ' as param17_subtype,
				'                                ' as param18_category,		
				'                                                  ' as param19_subcategory,
				product_name as product_name_origin
		INTO step0
		from products
		

		/* ������� ���������� */
		update step0  set product_name = replace(product_name, '����������-��������������-����������75��+����-�������75��/12', '����������-��������������-����������+����-������� 75��+75�� / 12')
		where ( product_name like '%����������-��������������-����������75��+����-�������75��/12%' )

		-------------------------------param15_producer-------------------------------------------
		UPDATE step0
		SET product_name=replace(replace(replace(replace(replace(product_name
												,'�� ����','����')
												,'��� �� ""������""','������')
												,'"������ ���"','������')
												,'"�� ���� ����.��������"','"�� ���� ����"')
												,'������"','������.')

		/* ��� ������� �����, ������� ���� �� ��������� � ������� ������ product_category_rules. �.�. ���� ������� ����������� ����� - ������ ������� */
	    /* ������ ��������������� ������� */
		EXEC MULTIPLE_UPDATE				@SUBSTRING = '����'				,@PRODUCER	= '�� ����'					,@PROD_TYPE ='��������'	,@PROD_SUBTYPE=''					,@PROD_CATEGORY=''					,@PROD_SUBCATEGORY=''
		EXEC MULTIPLE_UPDATE				@SUBSTRING = '������'			,@PRODUCER	= '��� �� ""������""'		,@PROD_TYPE ='��������'	,@PROD_SUBTYPE='����/������������'	,@PROD_CATEGORY='����'				,@PROD_SUBCATEGORY='����'
		EXEC MULTIPLE_UPDATE				@SUBSTRING = '������� ������������'	,@PRODUCER	= '������� ������������',@PROD_TYPE ='��������'	,@PROD_SUBTYPE='����'				,@PROD_CATEGORY=''					,@PROD_SUBCATEGORY=''
		EXEC MULTIPLE_UPDATE				@SUBSTRING = '���'				,@PRODUCER	= '���'						,@PROD_TYPE ='��������'	,@PROD_SUBTYPE='����/�����'			,@PROD_CATEGORY='������'			,@PROD_SUBCATEGORY=''
		EXEC MULTIPLE_UPDATE				@SUBSTRING = '"������ ���"'		,@PRODUCER	= '"������  ���"'			,@PROD_TYPE ='��������'	,@PROD_SUBTYPE='������'				,@PROD_CATEGORY='������(��������)'	,@PROD_SUBCATEGORY=''
		EXEC MULTIPLE_UPDATE				@SUBSTRING = '"�� ���� ����"'	,@PRODUCER	= '"�� ���� ����.��������"' ,@PROD_TYPE ='��������'	,@PROD_SUBTYPE=''					,@PROD_CATEGORY='������'			,@PROD_SUBCATEGORY=''  -- �� ������ ����, ����� ��������� ��� ���� ����
		EXEC MULTIPLE_UPDATE				@SUBSTRING = '"����.�������"'	,@PRODUCER	= '"����.�������"'          ,@PROD_TYPE ='��������'	,@PROD_SUBTYPE='����'				,@PROD_CATEGORY='������'			,@PROD_SUBCATEGORY='��������'
		EXEC MULTIPLE_UPDATE				@SUBSTRING = '��������� ��'		,@PRODUCER	= '��������� ��'			,@PROD_TYPE ='��������'	,@PROD_SUBTYPE='������������ �������/��������',@PROD_CATEGORY='�������'	,@PROD_SUBCATEGORY='�����'
		EXEC MULTIPLE_UPDATE				@SUBSTRING = '��������'			,@PRODUCER	= '��������'				,@PROD_TYPE ='��������'	,@PROD_SUBTYPE='����/�����'			,@PROD_CATEGORY=''					,@PROD_SUBCATEGORY=''
		EXEC MULTIPLE_UPDATE				@SUBSTRING = '������'			,@PRODUCER	= '������'					,@PROD_TYPE ='��������'	,@PROD_SUBTYPE='����/�����'			,@PROD_CATEGORY=''					,@PROD_SUBCATEGORY=''
		EXEC MULTIPLE_UPDATE				@SUBSTRING = '�������'			,@PRODUCER	= '�������'					,@PROD_TYPE ='��������'	,@PROD_SUBTYPE=''					,@PROD_CATEGORY=''					,@PROD_SUBCATEGORY=''
		EXEC MULTIPLE_UPDATE				@SUBSTRING = '�����'			,@PRODUCER	= '�����'					,@PROD_TYPE ='��������'	,@PROD_SUBTYPE='����/������������'	,@PROD_CATEGORY=''					,@PROD_SUBCATEGORY=''
		EXEC MULTIPLE_UPDATE				@SUBSTRING = '��������������'	,@PRODUCER	= '��������������'			,@PROD_TYPE ='��������'	,@PROD_SUBTYPE='����/������������'	,@PROD_CATEGORY='����'				,@PROD_SUBCATEGORY='�����'
		EXEC MULTIPLE_UPDATE				@SUBSTRING = '����������'		,@PRODUCER	= '����������'				,@PROD_TYPE ='��������'	,@PROD_SUBTYPE='����/������������'	,@PROD_CATEGORY=''					,@PROD_SUBCATEGORY=''
		EXEC MULTIPLE_UPDATE				@SUBSTRING = '��������'			,@PRODUCER	= '��������'				,@PROD_TYPE ='��������'	,@PROD_SUBTYPE='����/������������'	,@PROD_CATEGORY=''					,@PROD_SUBCATEGORY=''
		EXEC MULTIPLE_UPDATE				@SUBSTRING = '���'				,@PRODUCER	= '���'						,@PROD_TYPE ='��������'	,@PROD_SUBTYPE='����/�����'			,@PROD_CATEGORY=''					,@PROD_SUBCATEGORY=''
		EXEC MULTIPLE_UPDATE_PRODCATEGORY	@SUBSTRING = '�������������'	,@PRODUCER	= '�������������'			,@PROD_TYPE ='��������'	,@PROD_SUBTYPE='������� ����'		,@PROD_CATEGORY='����� ���������'	,@PROD_SUBCATEGORY=''
		EXEC MULTIPLE_UPDATE				@SUBSTRING = '�������������'	,@PRODUCER	= '�������������'			,@PROD_TYPE ='��������'	,@PROD_SUBTYPE='�������� ��������'	,@PROD_CATEGORY=''					,@PROD_SUBCATEGORY=''
		EXEC MULTIPLE_UPDATE				@SUBSTRING = '�����'			,@PRODUCER	= '�����'					,@PROD_TYPE ='��������'	,@PROD_SUBTYPE='�������'			,@PROD_CATEGORY='���'				,@PROD_SUBCATEGORY=''
		EXEC MULTIPLE_UPDATE				@SUBSTRING = '����� �������'	,@PRODUCER	= '��� ���� ����������'	,@PROD_TYPE ='��������'	,@PROD_SUBTYPE='������ ���.'			,@PROD_CATEGORY='������'			,@PROD_SUBCATEGORY=''	-- �� ������ ������ ���. ���., ����� ��������� ��� ���� ����
		EXEC MULTIPLE_UPDATE				@SUBSTRING = '��������� ������'	,@PRODUCER	= '��������� ������'		,@PROD_TYPE ='��������'	,@PROD_SUBTYPE='���������/�������������'	,@PROD_CATEGORY=''			,@PROD_SUBCATEGORY=''
		EXEC MULTIPLE_UPDATE				@SUBSTRING = '������'			,@PRODUCER	= '������'					,@PROD_TYPE ='��������'	,@PROD_SUBTYPE='�������� ��������'	,@PROD_CATEGORY=''					,@PROD_SUBCATEGORY=''
		EXEC MULTIPLE_UPDATE				@SUBSTRING = '���'				,@PRODUCER	= '��� ���'					,@PROD_TYPE ='��������'	,@PROD_SUBTYPE='�������� ��������'	,@PROD_CATEGORY=''					,@PROD_SUBCATEGORY=''
		EXEC MULTIPLE_UPDATE				@SUBSTRING = '������.'			,@PRODUCER	= '������'					,@PROD_TYPE ='��������'	,@PROD_SUBTYPE=''					,@PROD_CATEGORY='������'			,@PROD_SUBCATEGORY=''  -- �� ������ ����, ����� ��������� ��� ���� ����
		EXEC MULTIPLE_UPDATE_PRODCATEGORY	@SUBSTRING = '����'				,@PRODUCER	= '����'					,@PROD_TYPE ='��������'	,@PROD_SUBTYPE='����/������������'	,@PROD_CATEGORY='���� ������ ��������'	,@PROD_SUBCATEGORY=''
		EXEC MULTIPLE_UPDATE_PRODCATEGORY	@SUBSTRING = '����'				,@PRODUCER	= '����'					,@PROD_TYPE ='��������'	,@PROD_SUBTYPE='����/������������'	,@PROD_CATEGORY='���� ������� ��������'	,@PROD_SUBCATEGORY=''
		EXEC MULTIPLE_UPDATE_PRODCATEGORY	@SUBSTRING = '����'				,@PRODUCER	= '����'					,@PROD_TYPE ='��������'	,@PROD_SUBTYPE='����/������������'	,@PROD_CATEGORY='����� �������'			,@PROD_SUBCATEGORY=''
		EXEC MULTIPLE_UPDATE_PRODCATEGORY	@SUBSTRING = '����'				,@PRODUCER	= '����'					,@PROD_TYPE ='��������'	,@PROD_SUBTYPE='����/������������'	,@PROD_CATEGORY='����� � ����'			,@PROD_SUBCATEGORY=''
		EXEC MULTIPLE_UPDATE_PRODCATEGORY	@SUBSTRING = '������� ��������'	,@PRODUCER	= '������� ��������'		,@PROD_TYPE ='��������'	,@PROD_SUBTYPE='����/������������'	,@PROD_CATEGORY='����� � ����'			,@PROD_SUBCATEGORY=''
		EXEC MULTIPLE_UPDATE_PRODCATEGORY	@SUBSTRING = '������� ��������'	,@PRODUCER	= '������� ��������'		,@PROD_TYPE ='��������'	,@PROD_SUBTYPE='����/������������'	,@PROD_CATEGORY='���� ������'			,@PROD_SUBCATEGORY=''
		EXEC MULTIPLE_UPDATE_PRODCATEGORY	@SUBSTRING = '������� ��������'	,@PRODUCER	= '������� ��������'		,@PROD_TYPE ='��������'	,@PROD_SUBTYPE='����/������������'	,@PROD_CATEGORY='����'					,@PROD_SUBCATEGORY=''-- ���������� �� �������� ���������
		EXEC MULTIPLE_UPDATE				@SUBSTRING = '������� ��������'	,@PRODUCER	= '������� ��������'		,@PROD_TYPE ='��������'	,@PROD_SUBTYPE='����/������������'	,@PROD_CATEGORY='������������'			,@PROD_SUBCATEGORY='' -- ������� ����� ���������, ��������
		EXEC MULTIPLE_UPDATE_PRODCATEGORY	@SUBSTRING = '������� ���������',@PRODUCER	= '������� ���������'		,@PROD_TYPE ='��������'	,@PROD_SUBTYPE='����/������������'	,@PROD_CATEGORY='���� ������'			,@PROD_SUBCATEGORY=''
		EXEC MULTIPLE_UPDATE_PRODCATEGORY	@SUBSTRING = '������� ���������',@PRODUCER	= '������� ���������'		,@PROD_TYPE ='��������'	,@PROD_SUBTYPE='����/������������'	,@PROD_CATEGORY='����'					,@PROD_SUBCATEGORY=''-- ���������� �� �������� ����������
		EXEC MULTIPLE_UPDATE				@SUBSTRING = '������� ���������',@PRODUCER	= '������� ���������'		,@PROD_TYPE ='��������'	,@PROD_SUBTYPE='����/������������'	,@PROD_CATEGORY=''						,@PROD_SUBCATEGORY=''
		EXEC MULTIPLE_UPDATE_PRODCATEGORY	@SUBSTRING = '������'			,@PRODUCER	= '������'					,@PROD_TYPE ='��������'	,@PROD_SUBTYPE='����/������������'	,@PROD_CATEGORY='��������'				,@PROD_SUBCATEGORY=''
		EXEC MULTIPLE_UPDATE_PRODCATEGORY	@SUBSTRING = '������'			,@PRODUCER	= '������'					,@PROD_TYPE ='��������'	,@PROD_SUBTYPE='����/������������'	,@PROD_CATEGORY='����'					,@PROD_SUBCATEGORY='' --��������� ��������\ ������� \ ...?
		EXEC MULTIPLE_UPDATE				@SUBSTRING = '������'			,@PRODUCER	= '������'					,@PROD_TYPE ='��������'	,@PROD_SUBTYPE='����/������������'	,@PROD_CATEGORY=''						,@PROD_SUBCATEGORY='' ---���������
		EXEC MULTIPLE_UPDATE				@SUBSTRING = '������ ��'		,@PRODUCER	= '������ ��'				,@PROD_TYPE ='��������'	,@PROD_SUBTYPE='���������/�������������',@PROD_CATEGORY=''						,@PROD_SUBCATEGORY=''   --��������� �����-�� ��� , ������, ����
		EXEC MULTIPLE_UPDATE				@SUBSTRING = '���������'		,@PRODUCER	= '���������'				,@PROD_TYPE ='��������'	,@PROD_SUBTYPE='�����������/�����'		,@PROD_CATEGORY=''						,@PROD_SUBCATEGORY=''   --  �������, �������, ��������
		EXEC MULTIPLE_UPDATE				@SUBSTRING = '�������������'	,@PRODUCER	= '�������������'			,@PROD_TYPE ='��������'	,@PROD_SUBTYPE='�����������/�����'	,@PROD_CATEGORY=''						,@PROD_SUBCATEGORY=''   
		EXEC MULTIPLE_UPDATE				@SUBSTRING = '������� �������'  ,@PRODUCER	= '���������'				,@PROD_TYPE ='��������'	,@PROD_SUBTYPE='�����������/�����'	,@PROD_CATEGORY=''						,@PROD_SUBCATEGORY=''   --  ��������, ����, �������, �����
		EXEC MULTIPLE_UPDATE_PRODCATEGORY	@SUBSTRING = '���������'		,@PRODUCER	= '���������'				,@PROD_TYPE ='��������'	,@PROD_SUBTYPE='���������/�������������'	,@PROD_CATEGORY='�����'			,@PROD_SUBCATEGORY=''   --  ��������, ����, �������, �����
		EXEC MULTIPLE_UPDATE				@SUBSTRING = '���������'		,@PRODUCER	= '���������'				,@PROD_TYPE ='��������'	,@PROD_SUBTYPE='����/������������'	,@PROD_CATEGORY=''						,@PROD_SUBCATEGORY=''   --  ��������, ����, �������, �����
		EXEC MULTIPLE_UPDATE_PRODCATEGORY	@SUBSTRING = '���������� �����'	,@PRODUCER	= '���������� �����'		,@PROD_TYPE ='��������'	,@PROD_SUBTYPE='����/������������'	,@PROD_CATEGORY='��������'				,@PROD_SUBCATEGORY='' 
		EXEC MULTIPLE_UPDATE_PRODCATEGORY	@SUBSTRING = '���������� �����'	,@PRODUCER	= '���������� �����'		,@PROD_TYPE ='��������'	,@PROD_SUBTYPE='����/������������'	,@PROD_CATEGORY='����'					,@PROD_SUBCATEGORY='' 
		EXEC MULTIPLE_UPDATE_PRODCATEGORY	@SUBSTRING = '���������� �����'	,@PRODUCER	= '���������� �����'		,@PROD_TYPE ='��������'	,@PROD_SUBTYPE='����/������������'	,@PROD_CATEGORY='����� ���������'		,@PROD_SUBCATEGORY='' 
		EXEC MULTIPLE_UPDATE_PRODCATEGORY	@SUBSTRING = '���������� �����'	,@PRODUCER	= '���������� �����'		,@PROD_TYPE ='��������'	,@PROD_SUBTYPE='����/������������'	,@PROD_CATEGORY='����� ����������'		,@PROD_SUBCATEGORY='' 
		EXEC MULTIPLE_UPDATE_PRODCATEGORY	@SUBSTRING = '���������� �����'	,@PRODUCER	= '���������� �����'		,@PROD_TYPE ='��������'	,@PROD_SUBTYPE='����/������������'	,@PROD_CATEGORY='������ ������'			,@PROD_SUBCATEGORY='' 
		EXEC MULTIPLE_UPDATE_PRODCATEGORY	@SUBSTRING = '���������� �����'	,@PRODUCER	= '���������� �����'		,@PROD_TYPE ='��������'	,@PROD_SUBTYPE='����/������������'	,@PROD_CATEGORY='����'					,@PROD_SUBCATEGORY='' 
		EXEC MULTIPLE_UPDATE				@SUBSTRING = '���������� �����'	,@PRODUCER	= '���������� �����'		,@PROD_TYPE ='��������'	,@PROD_SUBTYPE='����/������������'	,@PROD_CATEGORY='����� �� ������� �������'	,@PROD_SUBCATEGORY='' 
		EXEC MULTIPLE_UPDATE_PRODCATEGORY	@SUBSTRING = '��������'			,@PRODUCER	= '��������'				,@PROD_TYPE ='��������'	,@PROD_SUBTYPE='����/������������'	,@PROD_CATEGORY='��������'				,@PROD_SUBCATEGORY='' 
		EXEC MULTIPLE_UPDATE_PRODCATEGORY	@SUBSTRING = '��������'			,@PRODUCER	= '��������'				,@PROD_TYPE ='��������'	,@PROD_SUBTYPE='����/������������'	,@PROD_CATEGORY='����� �� ������� �������'	,@PROD_SUBCATEGORY='' 
		EXEC MULTIPLE_UPDATE_PRODCATEGORY	@SUBSTRING = '��������'			,@PRODUCER	= '��������'				,@PROD_TYPE ='��������'	,@PROD_SUBTYPE='����/������������'	,@PROD_CATEGORY='�������� �������'		,@PROD_SUBCATEGORY='' 
		EXEC MULTIPLE_UPDATE_PRODCATEGORY	@SUBSTRING = '��������'			,@PRODUCER	= '��������'				,@PROD_TYPE ='��������'	,@PROD_SUBTYPE='����/������������'	,@PROD_CATEGORY='����'					,@PROD_SUBCATEGORY='' -- ���������� �� ��������
		EXEC MULTIPLE_UPDATE				@SUBSTRING = '��������'			,@PRODUCER	= '��������'				,@PROD_TYPE ='��������'	,@PROD_SUBTYPE='����/������������'	,@PROD_CATEGORY='����'					,@PROD_SUBCATEGORY='' 
		EXEC MULTIPLE_UPDATE_PRODCATEGORY	@SUBSTRING = '���������'		,@PRODUCER	= '���������'				,@PROD_TYPE ='��������'	,@PROD_SUBTYPE='����/������������'	,@PROD_CATEGORY='��������'				,@PROD_SUBCATEGORY='' 
		EXEC MULTIPLE_UPDATE_PRODCATEGORY	@SUBSTRING = '���������'		,@PRODUCER	= '���������'				,@PROD_TYPE ='��������'	,@PROD_SUBTYPE='����/������������'	,@PROD_CATEGORY='����'					,@PROD_SUBCATEGORY='' 
		EXEC MULTIPLE_UPDATE_PRODCATEGORY	@SUBSTRING = '�������'			,@PRODUCER	= '�������'					,@PROD_TYPE ='��������'	,@PROD_SUBTYPE='����/������������'	,@PROD_CATEGORY='��������'				,@PROD_SUBCATEGORY='' 
		EXEC MULTIPLE_UPDATE_PRODCATEGORY	@SUBSTRING = '�������'			,@PRODUCER	= '�������'					,@PROD_TYPE ='��������'	,@PROD_SUBTYPE='����/������������'	,@PROD_CATEGORY='����'					,@PROD_SUBCATEGORY='' 
		EXEC MULTIPLE_UPDATE_PRODCATEGORY	@SUBSTRING = '�� ����'			,@PRODUCER	= '�� ����'					,@PROD_TYPE ='��������'	,@PROD_SUBTYPE='����/������������'	,@PROD_CATEGORY='��������'				,@PROD_SUBCATEGORY='' 
		EXEC MULTIPLE_UPDATE_PRODCATEGORY	@SUBSTRING = '�� ����'			,@PRODUCER	= '�� ����'					,@PROD_TYPE ='��������'	,@PROD_SUBTYPE='����/������������'	,@PROD_CATEGORY='����� � ����'			,@PROD_SUBCATEGORY='' 
		EXEC MULTIPLE_UPDATE_PRODCATEGORY	@SUBSTRING = '�� ����'			,@PRODUCER	= '�� ����'					,@PROD_TYPE ='��������'	,@PROD_SUBTYPE='����/������������'	,@PROD_CATEGORY='����'					,@PROD_SUBCATEGORY='' 
		EXEC MULTIPLE_UPDATE_PRODCATEGORY	@SUBSTRING = '������� �����'	,@PRODUCER	= '������� �����'			,@PROD_TYPE ='��������'	,@PROD_SUBTYPE='����/������������'	,@PROD_CATEGORY='��������'				,@PROD_SUBCATEGORY='' 
		EXEC MULTIPLE_UPDATE_PRODCATEGORY	@SUBSTRING = '������� �����'	,@PRODUCER	= '������� �����'			,@PROD_TYPE ='��������'	,@PROD_SUBTYPE='����/������������'	,@PROD_CATEGORY='����'					,@PROD_SUBCATEGORY='' 
		EXEC MULTIPLE_UPDATE_PRODCATEGORY	@SUBSTRING = '����������'		,@PRODUCER	= '����������'				,@PROD_TYPE ='��������'	,@PROD_SUBTYPE='����/������������'	,@PROD_CATEGORY='��������'				,@PROD_SUBCATEGORY='' 
		EXEC MULTIPLE_UPDATE				@SUBSTRING = '����������'		,@PRODUCER	= '����������'				,@PROD_TYPE ='��������'	,@PROD_SUBTYPE='���������/�������������',@PROD_CATEGORY=''					,@PROD_SUBCATEGORY='����� �������� �� ������� ������������' 
		EXEC MULTIPLE_UPDATE				@SUBSTRING = '���'				,@PRODUCER	= '���'						,@PROD_TYPE ='��������'	,@PROD_SUBTYPE='�������� ��������'	,@PROD_CATEGORY=''						,@PROD_SUBCATEGORY=''
		EXEC MULTIPLE_UPDATE				@SUBSTRING = '������� ����'		,@PRODUCER	= '������� ����'			,@PROD_TYPE ='��������'	,@PROD_SUBTYPE='���������/�������������',@PROD_CATEGORY=''					,@PROD_SUBCATEGORY='' --�����, �����, �������, ���������, ������, ��������, ����  -- ���������?
		EXEC MULTIPLE_UPDATE				@SUBSTRING = '�������'			,@PRODUCER	= '�������'					,@PROD_TYPE ='��������'	,@PROD_SUBTYPE='�������� ��������'	,@PROD_CATEGORY='���������'				,@PROD_SUBCATEGORY=''
		EXEC MULTIPLE_UPDATE				@SUBSTRING = '������� �������'	,@PRODUCER	= '������� �������'			,@PROD_TYPE ='��������'	,@PROD_SUBTYPE='�������� ��������'	,@PROD_CATEGORY='���������'				,@PROD_SUBCATEGORY=''
		EXEC MULTIPLE_UPDATE				@SUBSTRING = '���������'		,@PRODUCER	= '�������'					,@PROD_TYPE ='��������'	,@PROD_SUBTYPE='�������� ��������'	,@PROD_CATEGORY='���������'				,@PROD_SUBCATEGORY=''
		EXEC MULTIPLE_UPDATE				@SUBSTRING = '�������'			,@PRODUCER	= '�������'					,@PROD_TYPE ='��������'	,@PROD_SUBTYPE='�������� ��������'	,@PROD_CATEGORY='���������'				,@PROD_SUBCATEGORY=''
		EXEC MULTIPLE_UPDATE_PRODCATEGORY	@SUBSTRING = '�������'			,@PRODUCER	= '�������'					,@PROD_TYPE ='��������'	,@PROD_SUBTYPE='�������'			,@PROD_CATEGORY='������� ���.'			,@PROD_SUBCATEGORY=''
		EXEC MULTIPLE_UPDATE_PRODCATEGORY	@SUBSTRING = '�������'			,@PRODUCER	= '�������'					,@PROD_TYPE ='��������'	,@PROD_SUBTYPE='����'				,@PROD_CATEGORY='���� �������.'			,@PROD_SUBCATEGORY=''
		EXEC MULTIPLE_UPDATE_PRODCATEGORY	@SUBSTRING = '���� TV'			,@PRODUCER	= '���� TV'					,@PROD_TYPE ='��������'	,@PROD_SUBTYPE='������������ �������/��������',@PROD_CATEGORY=''			,@PROD_SUBCATEGORY=''  -- �������, �����, �����, ������, ���
		EXEC MULTIPLE_UPDATE_PRODCATEGORY	@SUBSTRING = 'TV'				,@PRODUCER	= 'TV'						,@PROD_TYPE ='��������'	,@PROD_SUBTYPE='������������ �������/��������',@PROD_CATEGORY=''			,@PROD_SUBCATEGORY=''  -- �������, �����, �����, ������, ���
		EXEC MULTIPLE_UPDATE_PRODCATEGORY	@SUBSTRING = '�������� ����'	,@PRODUCER	= '�������� ����'			,@PROD_TYPE ='��������'	,@PROD_SUBTYPE='�����/�����'		,@PROD_CATEGORY='�����'					,@PROD_SUBCATEGORY='���������'  
		EXEC MULTIPLE_UPDATE_PRODCATEGORY	@SUBSTRING = '�������� ����'	,@PRODUCER	= '�������� ����'			,@PROD_TYPE ='��������'	,@PROD_SUBTYPE='�����/�����'		,@PROD_CATEGORY='����� �������'			,@PROD_SUBCATEGORY=''     
		EXEC MULTIPLE_UPDATE				@SUBSTRING = '�������� ����'	,@PRODUCER	= '�������� ����'			,@PROD_TYPE ='��������'	,@PROD_SUBTYPE='������/�����'		,@PROD_CATEGORY='�����'					,@PROD_SUBCATEGORY=''   
		EXEC MULTIPLE_UPDATE				@SUBSTRING = '��������� ������'	,@PRODUCER	= '��������� �������'		,@PROD_TYPE ='��������',@PROD_SUBTYPE='����/������������',@PROD_CATEGORY=''						,@PROD_SUBCATEGORY='' ---������� ����� � �������� ������
		EXEC MULTIPLE_UPDATE_PRODCATEGORY	@SUBSTRING = '4 ������'			,@PRODUCER	= '4 ������'				,@PROD_TYPE ='��������'	,@PROD_SUBTYPE='�����/������'		,@PROD_CATEGORY='��������'				,@PROD_SUBCATEGORY=''  
		EXEC MULTIPLE_UPDATE				@SUBSTRING = '4 ������'			,@PRODUCER	= '4 ������'				,@PROD_TYPE ='��������'	,@PROD_SUBTYPE='������/�����'		,@PROD_CATEGORY='�����'					,@PROD_SUBCATEGORY=''   
		EXEC MULTIPLE_UPDATE				@SUBSTRING = '���������� �4'	,@PRODUCER	= '���������� �4'			,@PROD_TYPE ='��������'	,@PROD_SUBTYPE='������������� �������/�������',@PROD_CATEGORY=''			,@PROD_SUBCATEGORY=''   
		EXEC MULTIPLE_UPDATE_PRODCATEGORY	@SUBSTRING = '�������'			,@PRODUCER	= '�������'					,@PROD_TYPE ='������'	,@PROD_SUBTYPE='������ ��� �����'	,@PROD_CATEGORY='���� �������'			,@PROD_SUBCATEGORY=''      
		EXEC MULTIPLE_UPDATE_PRODCATEGORY	@SUBSTRING = '�������'			,@PRODUCER	= '�������'					,@PROD_TYPE ='��������'	,@PROD_SUBTYPE='������/�����'		,@PROD_CATEGORY='���� ��������'			,@PROD_SUBCATEGORY='���������'      -- ������� update ����� �� �����
		EXEC MULTIPLE_UPDATE_PRODCATEGORY	@SUBSTRING = '�������'			,@PRODUCER	= '�������'					,@PROD_TYPE ='��������'	,@PROD_SUBTYPE='������/�����'		,@PROD_CATEGORY='���� ������'			,@PROD_SUBCATEGORY='���������'      -- ������� update ����� �� �����
		EXEC MULTIPLE_UPDATE_PRODCATEGORY	@SUBSTRING = '�������'			,@PRODUCER	= '�������'					,@PROD_TYPE ='��������'	,@PROD_SUBTYPE='�����/�����'		,@PROD_CATEGORY='���� ����������'		,@PROD_SUBCATEGORY='���������'      -- ������� update ����� �� �����
		EXEC MULTIPLE_UPDATE_PRODCATEGORY	@SUBSTRING = '�������'			,@PRODUCER	= '�������'					,@PROD_TYPE ='��������'	,@PROD_SUBTYPE='�����/������'		,@PROD_CATEGORY='����'					,@PROD_SUBCATEGORY='���������'      -- ������� update ����� �� �����
		EXEC MULTIPLE_UPDATE				@SUBSTRING = '�������'			,@PRODUCER	= '�������'					,@PROD_TYPE ='��������'	,@PROD_SUBTYPE='���������/�������������',	@PROD_CATEGORY='���� ��������'	,@PROD_SUBCATEGORY='���������'      -- ������� update ����� �� �����
		EXEC MULTIPLE_UPDATE				@SUBSTRING = '��������'			,@PRODUCER	= '��������'				,@PROD_TYPE ='��������'	,@PROD_SUBTYPE='�������� ��������',	@PROD_CATEGORY='�������� �������'		,@PROD_SUBCATEGORY=''     
		EXEC MULTIPLE_UPDATE				@SUBSTRING = '�������'			,@PRODUCER	= '�������'					,@PROD_TYPE ='��������'	,@PROD_SUBTYPE='�������'			,@PROD_CATEGORY='����� ��� �������� �������������'						,@PROD_SUBCATEGORY=''     
		EXEC MULTIPLE_UPDATE				@SUBSTRING = '������� ����'		,@PRODUCER	= '������� ����'			,@PROD_TYPE ='��������'	,@PROD_SUBTYPE='������������ �������/��������',@PROD_CATEGORY='����'		,@PROD_SUBCATEGORY=''  

		update step0
		set param19_subcategory = '70 / 90',
		param18_category='��������',
		product_name = replace( replace(product_name, '70/90',''),'��������','')
		where product_name like '%70/90%' and product_name like '%��������%'
		
		EXEC MULTIPLE_UPDATE				@SUBSTRING = '�����'			,@PRODUCER	= '�����'					,@PROD_TYPE ='��������'	,@PROD_SUBTYPE='����/������������'	,@PROD_CATEGORY=''						,@PROD_SUBCATEGORY='' ---
		EXEC MULTIPLE_UPDATE				@SUBSTRING = '������������'		,@PRODUCER	= '������������'			,@PROD_TYPE ='��������'	,@PROD_SUBTYPE='������� ����'	,@PROD_CATEGORY='�����'					,@PROD_SUBCATEGORY='���������' ---
		EXEC MULTIPLE_UPDATE_PRODCATEGORY	@SUBSTRING = '����������'		,@PRODUCER	= '������������ �������� �������� �������',	@PROD_TYPE ='��������',@PROD_SUBTYPE='������������ �������/��������',@PROD_CATEGORY='�������'	,@PROD_SUBCATEGORY='' 
		EXEC MULTIPLE_UPDATE_PRODCATEGORY	@SUBSTRING = '���������'		,@PRODUCER	= '������������ �������� �������� �������',	@PROD_TYPE ='��������',@PROD_SUBTYPE='������������ �������/��������',@PROD_CATEGORY='�������'	,@PROD_SUBCATEGORY='' 
		EXEC MULTIPLE_UPDATE_PRODCATEGORY	@SUBSTRING = '����'				,@PRODUCER	= '����',					@PROD_TYPE ='��������'	,@PROD_SUBTYPE='����/������������'	,@PROD_CATEGORY='�������� ����'			,@PROD_SUBCATEGORY='' 
		EXEC MULTIPLE_UPDATE_PRODCATEGORY	@SUBSTRING = '����'				,@PRODUCER	= '����',					@PROD_TYPE ='��������'	,@PROD_SUBTYPE='����/������������'	,@PROD_CATEGORY='�������� �������'		,@PROD_SUBCATEGORY=''
		EXEC MULTIPLE_UPDATE_PRODCATEGORY	@SUBSTRING = '����'				,@PRODUCER	= '����',					@PROD_TYPE ='��������'	,@PROD_SUBTYPE='����/������������'	,@PROD_CATEGORY='�������� �������'		,@PROD_SUBCATEGORY=''  -- ������ �����  �������� ��������
		EXEC MULTIPLE_UPDATE				@SUBSTRING = '����'				,@PRODUCER	= '����',					@PROD_TYPE ='��������'	,@PROD_SUBTYPE='����/������������'	,@PROD_CATEGORY=''						,@PROD_SUBCATEGORY=''
		EXEC MULTIPLE_UPDATE_PRODCATEGORY	@SUBSTRING = '������'			,@PRODUCER	= '������',					@PROD_TYPE ='��������'	,@PROD_SUBTYPE='�������� ��������'	,@PROD_CATEGORY='���������'				,@PROD_SUBCATEGORY=''
		EXEC MULTIPLE_UPDATE_PRODCATEGORY	@SUBSTRING = '������'			,@PRODUCER	= '������',					@PROD_TYPE ='��������'	,@PROD_SUBTYPE='�������'			,@PROD_CATEGORY='���. �������'			,@PROD_SUBCATEGORY=''
		EXEC MULTIPLE_UPDATE_PRODCATEGORY	@SUBSTRING = '������'			,@PRODUCER	= '������',					@PROD_TYPE ='��������'	,@PROD_SUBTYPE='�������'			,@PROD_CATEGORY='����'					,@PROD_SUBCATEGORY=''
		EXEC MULTIPLE_UPDATE_PRODCATEGORY	@SUBSTRING = '������'			,@PRODUCER	= '������',					@PROD_TYPE ='��������'	,@PROD_SUBTYPE='������������ �������/��������'	,@PROD_CATEGORY='������'	,@PROD_SUBCATEGORY=''
		EXEC MULTIPLE_UPDATE_PRODCATEGORY	@SUBSTRING = '������'			,@PRODUCER	= '������',					@PROD_TYPE ='��������'	,@PROD_SUBTYPE='������� �������'	,@PROD_CATEGORY='��������'				,@PROD_SUBCATEGORY='�����'
		EXEC MULTIPLE_UPDATE_PRODCATEGORY	@SUBSTRING = '������'			,@PRODUCER	= '������',					@PROD_TYPE ='��������'	,@PROD_SUBTYPE='������������ �������/��������'	,@PROD_CATEGORY='�������'	,@PROD_SUBCATEGORY=''
		EXEC MULTIPLE_UPDATE_PRODCATEGORY	@SUBSTRING = '������'			,@PRODUCER	= '������',					@PROD_TYPE ='��������'	,@PROD_SUBTYPE='������������ �������/��������'	,@PROD_CATEGORY='���.��������'	,@PROD_SUBCATEGORY=''
		EXEC MULTIPLE_UPDATE				@SUBSTRING = '������'			,@PRODUCER	= '������',					@PROD_TYPE ='��������'	,@PROD_SUBTYPE=''					,@PROD_CATEGORY=''						,@PROD_SUBCATEGORY='' ---
		EXEC MULTIPLE_UPDATE_PRODCATEGORY	@SUBSTRING = '��'				,@PRODUCER	= '��',						@PROD_TYPE ='��������'	,@PROD_SUBTYPE='������������ �������/��������',@PROD_CATEGORY='�������'				,@PROD_SUBCATEGORY=''
		EXEC MULTIPLE_UPDATE_PRODSUBTYPE	@SUBSTRING = '��������'			,@PRODUCER	= '��������',				@PROD_TYPE ='��������'	,@PROD_SUBTYPE='������ ���.'					,@PROD_CATEGORY=''						,@PROD_SUBCATEGORY='' ---
		EXEC MULTIPLE_UPDATE				@SUBSTRING = '��������'			,@PRODUCER	= '��������',				@PROD_TYPE ='��������'	,@PROD_SUBTYPE=''								,@PROD_CATEGORY=''						,@PROD_SUBCATEGORY='' ---
		EXEC MULTIPLE_UPDATE				@SUBSTRING = '�������'			,@PRODUCER	= '�������',				@PROD_TYPE ='��������'	,@PROD_SUBTYPE='����'							,@PROD_CATEGORY=''						,@PROD_SUBCATEGORY='' ---
		EXEC MULTIPLE_UPDATE				@SUBSTRING = '������������� ��'	,@PRODUCER	= '������������� ��',		@PROD_TYPE ='��������'	,@PROD_SUBTYPE='������������ �������/��������'	,@PROD_CATEGORY=''						,@PROD_SUBCATEGORY='' ---
		EXEC MULTIPLE_UPDATE				@SUBSTRING = '�� �������������'	,@PRODUCER	= '�� �������������',		@PROD_TYPE ='��������'	,@PROD_SUBTYPE='�����/������'					,@PROD_CATEGORY=''						,@PROD_SUBCATEGORY='' ---
		EXEC MULTIPLE_UPDATE				@SUBSTRING = '�� �������������'	,@PRODUCER	= '�� �������������',		@PROD_TYPE ='��������'	,@PROD_SUBTYPE='�����/������'					,@PROD_CATEGORY=''						,@PROD_SUBCATEGORY='' ---
		EXEC MULTIPLE_UPDATE				@SUBSTRING = '�����������'		,@PRODUCER	= '�����������',			@PROD_TYPE ='��������'	,@PROD_SUBTYPE='�������� ��������'				,@PROD_CATEGORY=''						,@PROD_SUBCATEGORY='' ---
		EXEC MULTIPLE_UPDATE_PRODCATEGORY	@SUBSTRING = '����������� �������',@PRODUCER= '����������� �������',	@PROD_TYPE ='��������'	,@PROD_SUBTYPE='������� ����'					,@PROD_CATEGORY='����� ���������'		,@PROD_SUBCATEGORY='' ---
		EXEC MULTIPLE_UPDATE				@SUBSTRING = '����������� �������',@PRODUCER= '����������� �������',	@PROD_TYPE ='��������'	,@PROD_SUBTYPE='�������� ��������'				,@PROD_CATEGORY=''						,@PROD_SUBCATEGORY='' ---
		EXEC MULTIPLE_UPDATE_PRODCATEGORY	@SUBSTRING = '���������'		,@PRODUCER	= '���������',				@PROD_TYPE ='��������'	,@PROD_SUBTYPE='����'							,@PROD_CATEGORY=''						,@PROD_SUBCATEGORY='' ---
		EXEC MULTIPLE_UPDATE				@SUBSTRING = '���������'		,@PRODUCER	= '���������',				@PROD_TYPE ='��������'	,@PROD_SUBTYPE=''								,@PROD_CATEGORY=''						,@PROD_SUBCATEGORY='' ---
		EXEC MULTIPLE_UPDATE				@SUBSTRING = '��������'			,@PRODUCER	= '��������',				@PROD_TYPE ='��������'	,@PROD_SUBTYPE='����/������������'				,@PROD_CATEGORY=''						,@PROD_SUBCATEGORY='' ---
		EXEC MULTIPLE_UPDATE				@SUBSTRING ='������� ��������, ������������ ����',@PRODUCER ='�� ����', @PROD_TYPE ='��������'	,@PROD_SUBTYPE='���������/�������������'		,@PROD_CATEGORY=''						,@PROD_SUBCATEGORY='' ---
		EXEC MULTIPLE_UPDATE				@SUBSTRING = '�������'			,@PRODUCER	= '�������',				@PROD_TYPE ='��������'	,@PROD_SUBTYPE='�������� ��������'				,@PROD_CATEGORY='���������'				,@PROD_SUBCATEGORY='' ---
		EXEC MULTIPLE_UPDATE				@SUBSTRING = '����� �������'	,@PRODUCER	= '����� �������',			@PROD_TYPE ='��������'	,@PROD_SUBTYPE='����/������������'				,@PROD_CATEGORY=''						,@PROD_SUBCATEGORY='' ---
		EXEC MULTIPLE_UPDATE				@SUBSTRING = '����� ����'		,@PRODUCER	= '����� ����',				@PROD_TYPE ='��������'	,@PROD_SUBTYPE='����/������������'				,@PROD_CATEGORY=''						,@PROD_SUBCATEGORY='' ---
		EXEC MULTIPLE_UPDATE				@SUBSTRING = '���'				,@PRODUCER	= '���',					@PROD_TYPE ='��������'	,@PROD_SUBTYPE='����/�����'						,@PROD_CATEGORY=''						,@PROD_SUBCATEGORY='' ---
		EXEC MULTIPLE_UPDATE				@SUBSTRING = '������'			,@PRODUCER	= '������',					@PROD_TYPE ='��������'	,@PROD_SUBTYPE='����'							,@PROD_CATEGORY=''						,@PROD_SUBCATEGORY='' ---
		EXEC MULTIPLE_UPDATE				@SUBSTRING = '������'			,@PRODUCER	= '������',					@PROD_TYPE ='��������'	,@PROD_SUBTYPE='����'							,@PROD_CATEGORY=''						,@PROD_SUBCATEGORY='' ---
		EXEC MULTIPLE_UPDATE				@SUBSTRING = '������� ����'		,@PRODUCER	= '������� ����',			@PROD_TYPE ='��������'	,@PROD_SUBTYPE='����/������������'				,@PROD_CATEGORY=''						,@PROD_SUBCATEGORY='' ---
		EXEC MULTIPLE_UPDATE				@SUBSTRING = '����'				,@PRODUCER	= '����',					@PROD_TYPE ='��������'	,@PROD_SUBTYPE='���������/�������������'		,@PROD_CATEGORY=''						,@PROD_SUBCATEGORY='' ---
		EXEC MULTIPLE_UPDATE				@SUBSTRING = '���'				,@PRODUCER	= '���',					@PROD_TYPE ='��������'	,@PROD_SUBTYPE='����/�����'						,@PROD_CATEGORY=''						,@PROD_SUBCATEGORY='' ---
		EXEC MULTIPLE_UPDATE				@SUBSTRING = '�������'			,@PRODUCER	= '�������',				@PROD_TYPE ='��������'	,@PROD_SUBTYPE='����'							,@PROD_CATEGORY=''						,@PROD_SUBCATEGORY='' ---
		EXEC MULTIPLE_UPDATE_PRODCATEGORY	@SUBSTRING = '������� ������'	,@PRODUCER	= '������� ������',			@PROD_TYPE ='��������'	,@PROD_SUBTYPE='������� ����'					,@PROD_CATEGORY='����� ���������'						,@PROD_SUBCATEGORY='' ---
		EXEC MULTIPLE_UPDATE				@SUBSTRING = '������� ������'	,@PRODUCER	= '������� ������',			@PROD_TYPE ='��������'	,@PROD_SUBTYPE='�������� ��������'				,@PROD_CATEGORY=''						,@PROD_SUBCATEGORY='' ---
		EXEC MULTIPLE_UPDATE				@SUBSTRING = '������� ��������'	,@PRODUCER	= '������� ��������',		@PROD_TYPE ='��������'	,@PROD_SUBTYPE='�������'						,@PROD_CATEGORY='����'					,@PROD_SUBCATEGORY='' ---
		EXEC MULTIPLE_UPDATE				@SUBSTRING = '������� �������'	,@PRODUCER	= '������� �������',		@PROD_TYPE ='��������'	,@PROD_SUBTYPE='������������ �������/��������'	,@PROD_CATEGORY='�������'				,@PROD_SUBCATEGORY='' ---
		EXEC MULTIPLE_UPDATE				@SUBSTRING = '������� ��������'	,@PRODUCER	= '������� ��������',		@PROD_TYPE ='��������'	,@PROD_SUBTYPE='������������ �������/��������'	,@PROD_CATEGORY='�������'				,@PROD_SUBCATEGORY='' ---
		EXEC MULTIPLE_UPDATE				@SUBSTRING = '������� ��������'	,@PRODUCER	= '������� ��������',		@PROD_TYPE ='��������'	,@PROD_SUBTYPE='������������ �������/��������'	,@PROD_CATEGORY='�������'				,@PROD_SUBCATEGORY='' ---
		EXEC MULTIPLE_UPDATE				@SUBSTRING = '������� ������'	,@PRODUCER	= '������� ������',			@PROD_TYPE ='��������'	,@PROD_SUBTYPE='���������/�������������'		,@PROD_CATEGORY=''						,@PROD_SUBCATEGORY='' ---
		EXEC MULTIPLE_UPDATE_PRODCATEGORY	@SUBSTRING = '���'				,@PRODUCER	= '���',					@PROD_TYPE ='��������'	,@PROD_SUBTYPE='����'							,@PROD_CATEGORY='������'				,@PROD_SUBCATEGORY='' ---
		EXEC MULTIPLE_UPDATE				@SUBSTRING = '��������� �������',@PRODUCER	= '��������� �������',		@PROD_TYPE ='��������'	,@PROD_SUBTYPE='���������/�������������'		,@PROD_CATEGORY=''						,@PROD_SUBCATEGORY='' ---
		EXEC MULTIPLE_UPDATE				@SUBSTRING = '��-�� ��� ��������� �����������' ,@PRODUCER= '��-�� ��� ��������� �����������',@PROD_TYPE ='��������',@PROD_SUBTYPE='������'	,@PROD_CATEGORY='������'				,@PROD_SUBCATEGORY='' ---
		EXEC MULTIPLE_UPDATE				@SUBSTRING = '������ ����'		,@PRODUCER	= '������ ����',			@PROD_TYPE ='��������'	,@PROD_SUBTYPE='�����/������'					,@PROD_CATEGORY=''						,@PROD_SUBCATEGORY='' ---
		EXEC MULTIPLE_UPDATE				@SUBSTRING = '����� ������'		,@PRODUCER	= '����� ������',			@PROD_TYPE ='��������'	,@PROD_SUBTYPE='����/������������'				,@PROD_CATEGORY=''						,@PROD_SUBCATEGORY='' ---
		EXEC MULTIPLE_UPDATE_PRODCATEGORY	@SUBSTRING = '��� ����'			,@PRODUCER	= '��� ����',				@PROD_TYPE ='��������'	,@PROD_SUBTYPE='������/�����'					,@PROD_CATEGORY='���������'				,@PROD_SUBCATEGORY='���������' ---
		EXEC MULTIPLE_UPDATE_PRODCATEGORY	@SUBSTRING = '��� ����'			,@PRODUCER	= '��� ����',				@PROD_TYPE ='��������'	,@PROD_SUBTYPE='�����/�����'					,@PROD_CATEGORY='����������'			,@PROD_SUBCATEGORY='���������' ---
		EXEC MULTIPLE_UPDATE				@SUBSTRING = '��� ����'			,@PRODUCER	= '��� ����',				@PROD_TYPE ='��������'	,@PROD_SUBTYPE='�����/������'					,@PROD_CATEGORY=''						,@PROD_SUBCATEGORY='���������' ---
		EXEC MULTIPLE_UPDATE				@SUBSTRING = '�����'			,@PRODUCER	= '�����',					@PROD_TYPE ='��������'	,@PROD_SUBTYPE='����/������������'				,@PROD_CATEGORY='����'					,@PROD_SUBCATEGORY='' ---
		EXEC MULTIPLE_UPDATE				@SUBSTRING = '�������� �� ��������',@PRODUCER= '�������� �� ��������',	@PROD_TYPE ='��������',@PROD_SUBTYPE='������������ �������/��������'	,@PROD_CATEGORY=''						,@PROD_SUBCATEGORY='' ---
		EXEC MULTIPLE_UPDATE				@SUBSTRING = '������� �����'	,@PRODUCER	= '������� �����',			@PROD_TYPE ='��������'	,@PROD_SUBTYPE='�������� ��������'				,@PROD_CATEGORY='���������'				,@PROD_SUBCATEGORY='' ---
		EXEC MULTIPLE_UPDATE				@SUBSTRING = '�������'			,@PRODUCER	= '�������',				@PROD_TYPE ='��������'	,@PROD_SUBTYPE='�������� ��������'				,@PROD_CATEGORY='���������'				,@PROD_SUBCATEGORY='' ---
		EXEC MULTIPLE_UPDATE_PRODCATEGORY	@SUBSTRING = '�������'			,@PRODUCER	= '�������',				@PROD_TYPE ='��������'	,@PROD_SUBTYPE='�������� ��������'				,@PROD_CATEGORY='���������'				,@PROD_SUBCATEGORY='' ---
		EXEC MULTIPLE_UPDATE				@SUBSTRING = '�������'			,@PRODUCER	= '�������',				@PROD_TYPE ='��������'	,@PROD_SUBTYPE='����'							,@PROD_CATEGORY=''						,@PROD_SUBCATEGORY='' ---
		EXEC MULTIPLE_UPDATE				@SUBSTRING = '������'			,@PRODUCER	= '������',					@PROD_TYPE ='��������'	,@PROD_SUBTYPE='�������� ��������'				,@PROD_CATEGORY='���������'				,@PROD_SUBCATEGORY='' ---
		EXEC MULTIPLE_UPDATE_PRODCATEGORY	@SUBSTRING = '����'				,@PRODUCER	= '����',					@PROD_TYPE ='��������'	,@PROD_SUBTYPE='�������� ��������'				,@PROD_CATEGORY='���������'				,@PROD_SUBCATEGORY='' ---
		EXEC MULTIPLE_UPDATE				@SUBSTRING = '���������'		,@PRODUCER	= '���������',				@PROD_TYPE ='��������'	,@PROD_SUBTYPE='������������ �������/��������'	,@PROD_CATEGORY='�������'				,@PROD_SUBCATEGORY='' ---
		EXEC MULTIPLE_UPDATE				@SUBSTRING = '��������'			,@PRODUCER	= '��������',				@PROD_TYPE ='������'	,@PROD_SUBTYPE='���������/�������'				,@PROD_CATEGORY='���� ��� ����'			,@PROD_SUBCATEGORY='' ---
		EXEC MULTIPLE_UPDATE				@SUBSTRING = ' ���� '			,@PRODUCER	= '����',					@PROD_TYPE ='��������'	,@PROD_SUBTYPE='������������ �������/��������'	,@PROD_CATEGORY='���.��������'			,@PROD_SUBCATEGORY='' ---
		EXEC MULTIPLE_UPDATE				@SUBSTRING = '����������'		,@PRODUCER	= '����������',				@PROD_TYPE ='��������'	,@PROD_SUBTYPE='�������� ��������'				,@PROD_CATEGORY='���������'				,@PROD_SUBCATEGORY='' ---
		EXEC MULTIPLE_UPDATE				@SUBSTRING = '�������'			,@PRODUCER	= '�������',				@PROD_TYPE ='��������'	,@PROD_SUBTYPE='�������� ��������'				,@PROD_CATEGORY=''						,@PROD_SUBCATEGORY='' ---
		--EXEC MULTIPLE_UPDATE				@SUBSTRING = '��������'			,@PRODUCER	= '��������',				@PROD_TYPE =''			,@PROD_SUBTYPE=''								,@PROD_CATEGORY=''						,@PROD_SUBCATEGORY='' ---
		--EXEC MULTIPLE_UPDATE				@SUBSTRING = '����������'		,@PRODUCER	= '����������',				@PROD_TYPE =''			,@PROD_SUBTYPE=''								,@PROD_CATEGORY=''						,@PROD_SUBCATEGORY='' ---
		EXEC MULTIPLE_UPDATE				@SUBSTRING = '�������-������'	,@PRODUCER	= '�������-������',			@PROD_TYPE ='��������'	,@PROD_SUBTYPE='����/�����'						,@PROD_CATEGORY=''						,@PROD_SUBCATEGORY='' ---
		EXEC MULTIPLE_UPDATE				@SUBSTRING = '������'			,@PRODUCER	= '������',					@PROD_TYPE ='��������'	,@PROD_SUBTYPE='�����/�����'					,@PROD_CATEGORY=''						,@PROD_SUBCATEGORY='' ---
		EXEC MULTIPLE_UPDATE				@SUBSTRING = '������ ��������'	,@PRODUCER	= '������ ��������',		@PROD_TYPE ='��������'	,@PROD_SUBTYPE='�������� ��������'				,@PROD_CATEGORY=''						,@PROD_SUBCATEGORY='���������' ---
		EXEC MULTIPLE_UPDATE				@SUBSTRING='������� ������ ��������',@PRODUCER='������� ������ ��������',@PROD_TYPE ='��������'	,@PROD_SUBTYPE='����/������������'				,@PROD_CATEGORY=''						,@PROD_SUBCATEGORY='' ---
		EXEC MULTIPLE_UPDATE				@SUBSTRING = '��������'			,@PRODUCER	= '��������',				@PROD_TYPE ='��������'	,@PROD_SUBTYPE='����/������������'				,@PROD_CATEGORY=''						,@PROD_SUBCATEGORY='' ---
		EXEC MULTIPLE_UPDATE				@SUBSTRING = '�� ������'		,@PRODUCER	= '�� ������',				@PROD_TYPE ='��������'	,@PROD_SUBTYPE='����/������������'				,@PROD_CATEGORY=''						,@PROD_SUBCATEGORY='' ---
		EXEC MULTIPLE_UPDATE				@SUBSTRING = '���'				,@PRODUCER	= '���',					@PROD_TYPE ='��������'	,@PROD_SUBTYPE='�������� ��������'				,@PROD_CATEGORY='����� ���������'		,@PROD_SUBCATEGORY='' ---
		EXEC MULTIPLE_UPDATE				@SUBSTRING = '�����'			,@PRODUCER	= '�����',					@PROD_TYPE ='��������'	,@PROD_SUBTYPE='������������� �������/�������'	,@PROD_CATEGORY=''						,@PROD_SUBCATEGORY='' ---
		EXEC MULTIPLE_UPDATE				@SUBSTRING = '���� �������'		,@PRODUCER	= '���� �������',			@PROD_TYPE ='��������'	,@PROD_SUBTYPE='���������/�������������'		,@PROD_CATEGORY=''						,@PROD_SUBCATEGORY='' ---
		EXEC MULTIPLE_UPDATE				@SUBSTRING = '����������� ����'	,@PRODUCER	= '����������� ����',		@PROD_TYPE ='��������'	,@PROD_SUBTYPE='����'							,@PROD_CATEGORY=''						,@PROD_SUBCATEGORY='' ---
		EXEC MULTIPLE_UPDATE				@SUBSTRING = '������������'		,@PRODUCER	= '������� �����',			@PROD_TYPE ='��������'	,@PROD_SUBTYPE='�������� ��������'				,@PROD_CATEGORY='���������'				,@PROD_SUBCATEGORY='������' ---
		EXEC MULTIPLE_UPDATE				@SUBSTRING = '�� �������'		,@PRODUCER	= '�� �������',				@PROD_TYPE ='��������'	,@PROD_SUBTYPE='����/������������'				,@PROD_CATEGORY='������������ �� ����'	,@PROD_SUBCATEGORY='���������' ---
		EXEC MULTIPLE_UPDATE				@SUBSTRING='�� ������ ��� �����',@PRODUCER	= '�� ������ ��� �����',	@PROD_TYPE =''			,@PROD_SUBTYPE=''								,@PROD_CATEGORY=''						,@PROD_SUBCATEGORY='' ---
		EXEC MULTIPLE_UPDATE				@SUBSTRING = '������'			,@PRODUCER	= '�������',				@PROD_TYPE ='��������'	,@PROD_SUBTYPE='�������� ��������'				,@PROD_CATEGORY='���������'				,@PROD_SUBCATEGORY='' ---
		EXEC MULTIPLE_UPDATE_PRODCATEGORY	@SUBSTRING = '�����'			,@PRODUCER	= '�����',					@PROD_TYPE ='��������'	,@PROD_SUBTYPE='����'							,@PROD_CATEGORY='���'					,@PROD_SUBCATEGORY='��� ����������' ---
		EXEC MULTIPLE_UPDATE_PRODCATEGORY	@SUBSTRING = '������'			,@PRODUCER	= '������',					@PROD_TYPE ='��������'	,@PROD_SUBTYPE='����'							,@PROD_CATEGORY='����'						,@PROD_SUBCATEGORY='' ---
		EXEC MULTIPLE_UPDATE				@SUBSTRING = '������������ ��������',@PRODUCER= '������������ ��������',@PROD_TYPE ='��������'	,@PROD_SUBTYPE='����'						,@PROD_CATEGORY=''						,@PROD_SUBCATEGORY='' ---
		EXEC MULTIPLE_UPDATE				@SUBSTRING = 'MLEKARA SABAC'	,@PRODUCER	= 'MLEKARA SABAC',			@PROD_TYPE ='��������'	,@PROD_SUBTYPE='����'							,@PROD_CATEGORY=''						,@PROD_SUBCATEGORY='' ---
		EXEC MULTIPLE_UPDATE				@SUBSTRING = '����� ����'		,@PRODUCER	= '����� ����',				@PROD_TYPE ='��������'	,@PROD_SUBTYPE='����'							,@PROD_CATEGORY=''						,@PROD_SUBCATEGORY='' ---
		EXEC MULTIPLE_UPDATE				@SUBSTRING = '������'			,@PRODUCER	= '������',					@PROD_TYPE ='������'	,@PROD_SUBTYPE='������ ��� �����'				,@PROD_CATEGORY='����������'	,@PROD_SUBCATEGORY='' ---
		EXEC MULTIPLE_UPDATE				@SUBSTRING = '�������'			,@PRODUCER	= '�������',				@PROD_TYPE ='������'	,@PROD_SUBTYPE='������ ��� �����'				,@PROD_CATEGORY='����������'	,@PROD_SUBCATEGORY='' ---
		EXEC MULTIPLE_UPDATE				@SUBSTRING = '������'			,@PRODUCER	= '������',					@PROD_TYPE ='������'	,@PROD_SUBTYPE='���������/�������'				,@PROD_CATEGORY=''				,@PROD_SUBCATEGORY='' ---
		EXEC MULTIPLE_UPDATE				@SUBSTRING ='��������� ��������',@PRODUCER	= '��������� ��������',		@PROD_TYPE ='��������'	,@PROD_SUBTYPE='�������'						,@PROD_CATEGORY='������� ���������'	,@PROD_SUBCATEGORY='' ---
		EXEC MULTIPLE_UPDATE				@SUBSTRING = ' ����'			,@PRODUCER	= 'Ҩ��'			,		@PROD_TYPE ='��������'	,@PROD_SUBTYPE='������� �������'				,@PROD_CATEGORY=''	,@PROD_SUBCATEGORY='' ---
		EXEC MULTIPLE_UPDATE				@SUBSTRING = ' Ҩ��'			,@PRODUCER	= 'Ҩ��'			,		@PROD_TYPE ='��������'	,@PROD_SUBTYPE='������� �������'				,@PROD_CATEGORY=''	,@PROD_SUBCATEGORY='' ---
		EXEC MULTIPLE_UPDATE				@SUBSTRING = '���-������'		,@PRODUCER	= '������'			,		@PROD_TYPE ='��������'	,@PROD_SUBTYPE='�������� ��������'				,@PROD_CATEGORY=''	,@PROD_SUBCATEGORY='' ---
		EXEC MULTIPLE_UPDATE				@SUBSTRING = '�������'			,@PRODUCER	= '�����',					@PROD_TYPE ='��������'	,@PROD_SUBTYPE='�������� ��������'				,@PROD_CATEGORY=''	,@PROD_SUBCATEGORY='' ---
		EXEC MULTIPLE_UPDATE				@SUBSTRING = '�������'			,@PRODUCER	= '�����',					@PROD_TYPE ='��������'	,@PROD_SUBTYPE='�������� ��������'				,@PROD_CATEGORY=''	,@PROD_SUBCATEGORY='' ---
		EXEC MULTIPLE_UPDATE				@SUBSTRING = '����� ����'		,@PRODUCER	= '������ ����',			@PROD_TYPE ='��������'	,@PROD_SUBTYPE='������������ �������/��������'	,@PROD_CATEGORY=''	,@PROD_SUBCATEGORY='' ---
		EXEC MULTIPLE_UPDATE				@SUBSTRING = '������ ����'		,@PRODUCER	= '������ ����',			@PROD_TYPE ='��������'	,@PROD_SUBTYPE='������������ �������/��������'	,@PROD_CATEGORY=''	,@PROD_SUBCATEGORY='' ---
		EXEC MULTIPLE_UPDATE				@SUBSTRING = '�������'			,@PRODUCER	= '�������'	,				@PROD_TYPE ='��������'	,@PROD_SUBTYPE='�������� ��������'				,@PROD_CATEGORY=''	,@PROD_SUBCATEGORY='' ---
		EXEC MULTIPLE_UPDATE				@SUBSTRING = '����������'		,@PRODUCER	= '����������'	,			@PROD_TYPE ='��������'	,@PROD_SUBTYPE='�������� ��������'				,@PROD_CATEGORY='������������ �����'	,@PROD_SUBCATEGORY='' ---
		EXEC MULTIPLE_UPDATE				@SUBSTRING = '��������'			,@PRODUCER	= '��������'			,	@PROD_TYPE ='��������'	,@PROD_SUBTYPE='������������� �������/�������'	,@PROD_CATEGORY='�������'	,@PROD_SUBCATEGORY=''
		EXEC MULTIPLE_UPDATE				@SUBSTRING = '������� ����'		,@PRODUCER	= '������� ����'		,	@PROD_TYPE ='��������'	,@PROD_SUBTYPE='������������� �������/�������'	,@PROD_CATEGORY='�������'	,@PROD_SUBCATEGORY=''
		EXEC MULTIPLE_UPDATE_PRODCATEGORY	@SUBSTRING = '������'			,@PRODUCER	= '������',					@PROD_TYPE ='��������'	,@PROD_SUBTYPE='������� ��� ��������'	,@PROD_CATEGORY='����'			,@PROD_SUBCATEGORY=''
		EXEC MULTIPLE_UPDATE_PRODCATEGORY	@SUBSTRING = '������'			,@PRODUCER	= '������',					@PROD_TYPE ='������'	,@PROD_SUBTYPE='������ ��� ��������'	,@PROD_CATEGORY='�����������'	,@PROD_SUBCATEGORY=''
		EXEC MULTIPLE_UPDATE				@SUBSTRING = ' �����'			,@PRODUCER	= '�����'			,		@PROD_TYPE ='��������'	,@PROD_SUBTYPE='�����'					,@PROD_CATEGORY='���'	,@PROD_SUBCATEGORY='������' ---
		EXEC MULTIPLE_UPDATE				@SUBSTRING = ' ����'			,@PRODUCER	= '����'			,		@PROD_TYPE ='��������'	,@PROD_SUBTYPE='���������/�������������',@PROD_CATEGORY=''	,@PROD_SUBCATEGORY='' ---
		EXEC MULTIPLE_UPDATE				@SUBSTRING = '/����'			,@PRODUCER	= '����'			,		@PROD_TYPE ='��������'	,@PROD_SUBTYPE='���������/�������������',@PROD_CATEGORY=''	,@PROD_SUBCATEGORY='' ---
		EXEC MULTIPLE_UPDATE				@SUBSTRING = 'CHAMMY'			,@PRODUCER	= 'CHAMMY',					@PROD_TYPE ='��������'	,@PROD_SUBTYPE='������� ��� ��������'	,@PROD_CATEGORY='����'			,@PROD_SUBCATEGORY=''
		EXEC MULTIPLE_UPDATE				@SUBSTRING = '������'			,@PRODUCER	= '������',					@PROD_TYPE ='��������'	,@PROD_SUBTYPE='������� ��� ��������'	,@PROD_CATEGORY='����'			,@PROD_SUBCATEGORY=''
		EXEC MULTIPLE_UPDATE				@SUBSTRING = '�������'			,@PRODUCER	= '�������',				@PROD_TYPE ='��������'	,@PROD_SUBTYPE='������� ��� ��������'	,@PROD_CATEGORY='����'			,@PROD_SUBCATEGORY=''
		EXEC MULTIPLE_UPDATE				@SUBSTRING = '�������'			,@PRODUCER	= '�������',				@PROD_TYPE ='��������'	,@PROD_SUBTYPE='������� ��� ��������'	,@PROD_CATEGORY='����'			,@PROD_SUBCATEGORY=''
		EXEC MULTIPLE_UPDATE				@SUBSTRING = '�������'			,@PRODUCER	= '�������',				@PROD_TYPE ='��������'	,@PROD_SUBTYPE='������� ��� ��������'	,@PROD_CATEGORY='����'			,@PROD_SUBCATEGORY=''
		EXEC MULTIPLE_UPDATE				@SUBSTRING = '������'			,@PRODUCER	= '������',					@PROD_TYPE ='��������'	,@PROD_SUBTYPE='������� ��� ��������'	,@PROD_CATEGORY='����'			,@PROD_SUBCATEGORY=''
		EXEC MULTIPLE_UPDATE				@SUBSTRING = '�������'			,@PRODUCER	= '�������',				@PROD_TYPE ='��������'	,@PROD_SUBTYPE='������� ��� ��������'	,@PROD_CATEGORY='����'			,@PROD_SUBCATEGORY=''
		EXEC MULTIPLE_UPDATE				@SUBSTRING = '������ ���'		,@PRODUCER	= '������ ���',				@PROD_TYPE ='��������'	,@PROD_SUBTYPE='������� ��� ��������'	,@PROD_CATEGORY='����'			,@PROD_SUBCATEGORY=''
		EXEC MULTIPLE_UPDATE				@SUBSTRING = '�������'			,@PRODUCER	= '�������',				@PROD_TYPE ='��������'	,@PROD_SUBTYPE='������� ��� ��������'	,@PROD_CATEGORY='����'			,@PROD_SUBCATEGORY=''
		EXEC MULTIPLE_UPDATE				@SUBSTRING = '������'			,@PRODUCER	= '������',					@PROD_TYPE ='��������'	,@PROD_SUBTYPE='������� ��� ��������'	,@PROD_CATEGORY='����'			,@PROD_SUBCATEGORY=''
		EXEC MULTIPLE_UPDATE				@SUBSTRING = '���-���'			,@PRODUCER	= '���-���'			,		@PROD_TYPE ='��������'	,@PROD_SUBTYPE='���������/�������������',@PROD_CATEGORY=''	,@PROD_SUBCATEGORY='' ---
		EXEC MULTIPLE_UPDATE				@SUBSTRING = '�����'			,@PRODUCER	= '�����'			,		@PROD_TYPE ='��������'	,@PROD_SUBTYPE='���������/�������������',@PROD_CATEGORY=''	,@PROD_SUBCATEGORY='' ---
		EXEC MULTIPLE_UPDATE				@SUBSTRING = '������� �����'	,@PRODUCER	= '������� �����'	,		@PROD_TYPE ='��������'	,@PROD_SUBTYPE='���������/�������������',@PROD_CATEGORY=''	,@PROD_SUBCATEGORY='' ---
		EXEC MULTIPLE_UPDATE_PRODCATEGORY	@SUBSTRING = '�����'			,@PRODUCER	= '�����',					@PROD_TYPE ='��������'	,@PROD_SUBTYPE='�����/�����'					,@PROD_CATEGORY='��������'			,@PROD_SUBCATEGORY=''
		EXEC MULTIPLE_UPDATE_PRODCATEGORY	@SUBSTRING = '�����'			,@PRODUCER	= '�����',					@PROD_TYPE ='��������'	,@PROD_SUBTYPE='�����/�����'					,@PROD_CATEGORY='������'			,@PROD_SUBCATEGORY=''
		EXEC MULTIPLE_UPDATE_PRODCATEGORY	@SUBSTRING = '�����'			,@PRODUCER	= '�����',					@PROD_TYPE ='��������'	,@PROD_SUBTYPE='���������/�������������'		,@PROD_CATEGORY='�������'	,@PROD_SUBCATEGORY=''
		EXEC MULTIPLE_UPDATE_PRODCATEGORY	@SUBSTRING = '�����'			,@PRODUCER	= '�����',					@PROD_TYPE ='��������'	,@PROD_SUBTYPE='���������/�������������'		,@PROD_CATEGORY='������'	,@PROD_SUBCATEGORY=''
		EXEC MULTIPLE_UPDATE_PRODCATEGORY	@SUBSTRING = '��������'			,@PRODUCER	= '��������',				@PROD_TYPE ='������'	,@PROD_SUBTYPE='������ ��� ���� � ����'			,@PROD_CATEGORY=''			,@PROD_SUBCATEGORY=''
		EXEC MULTIPLE_UPDATE_PRODCATEGORY	@SUBSTRING = '����'				,@PRODUCER	= '����',					@PROD_TYPE ='������'	,@PROD_SUBTYPE='������ ��� ���� � ����'			,@PROD_CATEGORY=''			,@PROD_SUBCATEGORY=''
		EXEC MULTIPLE_UPDATE				@SUBSTRING = '���'				,@PRODUCER	= '���',					@PROD_TYPE ='��������'	,@PROD_SUBTYPE='������������ �������/��������'	,@PROD_CATEGORY=''	,@PROD_SUBCATEGORY='' ---
		EXEC MULTIPLE_UPDATE				@SUBSTRING = '7�������'			,@PRODUCER	= '7�������',				@PROD_TYPE ='��������'	,@PROD_SUBTYPE='������������ �������/��������'	,@PROD_CATEGORY=''	,@PROD_SUBCATEGORY='' ---
		EXEC MULTIPLE_UPDATE				@SUBSTRING = '�����'			,@PRODUCER	= '�����'	,				@PROD_TYPE ='��������'	,@PROD_SUBTYPE='�������� ��������'				,@PROD_CATEGORY=''	,@PROD_SUBCATEGORY='' ---
		EXEC MULTIPLE_UPDATE				@SUBSTRING = '���������'		,@PRODUCER	= '���������'	,			@PROD_TYPE ='��������'	,@PROD_SUBTYPE='������� �������'				,@PROD_CATEGORY=''	,@PROD_SUBCATEGORY='' ---
		EXEC MULTIPLE_UPDATE				@SUBSTRING = '��������'			,@PRODUCER	= '��������'			,	@PROD_TYPE ='��������'	,@PROD_SUBTYPE='������������ �������/��������'	,@PROD_CATEGORY='�������'	,@PROD_SUBCATEGORY=''
		EXEC MULTIPLE_UPDATE				@SUBSTRING = '��������'			,@PRODUCER	= '��������'		   ,	@PROD_TYPE ='��������'	,@PROD_SUBTYPE='������������ �������/��������'	,@PROD_CATEGORY='����'		,@PROD_SUBCATEGORY=''
		EXEC MULTIPLE_UPDATE				@SUBSTRING = '���'				,@PRODUCER	= '���'					,	@PROD_TYPE ='��������'	,@PROD_SUBTYPE='������������ �������/��������'	,@PROD_CATEGORY=''			,@PROD_SUBCATEGORY=''
		EXEC MULTIPLE_UPDATE				@SUBSTRING = '���� ����������'	,@PRODUCER	= '���� ����������'		,	@PROD_TYPE ='��������'	,@PROD_SUBTYPE='������������ �������/��������'	,@PROD_CATEGORY='�����'		,@PROD_SUBCATEGORY=''
		EXEC MULTIPLE_UPDATE				@SUBSTRING = '���˨���'			,@PRODUCER	= '���˨���'	,			@PROD_TYPE ='��������'	,@PROD_SUBTYPE='������� �������'				,@PROD_CATEGORY=''			,@PROD_SUBCATEGORY='' ---
		EXEC MULTIPLE_UPDATE				@SUBSTRING = '������'			,@PRODUCER	= '������'		   ,		@PROD_TYPE ='��������'	,@PROD_SUBTYPE='������������ �������/��������'	,@PROD_CATEGORY='�������'	,@PROD_SUBCATEGORY=''
		EXEC MULTIPLE_UPDATE				@SUBSTRING = '�����'			,@PRODUCER	= '�����',					@PROD_TYPE ='��������'	,@PROD_SUBTYPE='������������ �������/��������'	,@PROD_CATEGORY=''	,@PROD_SUBCATEGORY='' ---
		EXEC MULTIPLE_UPDATE				@SUBSTRING = '�����'			,@PRODUCER	= '�����'	,				@PROD_TYPE ='��������'	,@PROD_SUBTYPE='������� �������'				,@PROD_CATEGORY=''			,@PROD_SUBCATEGORY='' ---
		EXEC MULTIPLE_UPDATE				@SUBSTRING = '��������� �������',@PRODUCER	= '��������� �������',		@PROD_TYPE ='��������'	,@PROD_SUBTYPE='������� �������'				,@PROD_CATEGORY=''			,@PROD_SUBCATEGORY='' ---
		EXEC MULTIPLE_UPDATE				@SUBSTRING = '���� � �����'		,@PRODUCER	= '���� � �����',			@PROD_TYPE ='��������'	,@PROD_SUBTYPE='������� �������'				,@PROD_CATEGORY=''						,@PROD_SUBCATEGORY='' ---
		EXEC MULTIPLE_UPDATE				@SUBSTRING = '���������'		,@PRODUCER	= '���������'	,			@PROD_TYPE ='��������'	,@PROD_SUBTYPE='�������� ��������'				,@PROD_CATEGORY=''						,@PROD_SUBCATEGORY='' ---
		EXEC MULTIPLE_UPDATE				@SUBSTRING = '����������'		,@PRODUCER	= '����������'	,			@PROD_TYPE ='��������'	,@PROD_SUBTYPE='�������� ��������'				,@PROD_CATEGORY=''						,@PROD_SUBCATEGORY='' ---
		EXEC MULTIPLE_UPDATE				@SUBSTRING = '������'			,@PRODUCER	= '������'	,				@PROD_TYPE ='��������'	,@PROD_SUBTYPE='�������� ��������'				,@PROD_CATEGORY='������'				,@PROD_SUBCATEGORY='' ---
		EXEC MULTIPLE_UPDATE				@SUBSTRING = '�������'			,@PRODUCER	= '�������',				@PROD_TYPE ='��������'	,@PROD_SUBTYPE='����'							,@PROD_CATEGORY=''						,@PROD_SUBCATEGORY='' ---
		EXEC MULTIPLE_UPDATE				@SUBSTRING = '��������'			,@PRODUCER	= '��������',				@PROD_TYPE ='��������'	,@PROD_SUBTYPE='����'							,@PROD_CATEGORY=''						,@PROD_SUBCATEGORY='' ---
		EXEC MULTIPLE_UPDATE				@SUBSTRING = 'CHEESE'			,@PRODUCER	= 'CHEESE',					@PROD_TYPE ='��������'	,@PROD_SUBTYPE='����'							,@PROD_CATEGORY=''						,@PROD_SUBCATEGORY='' ---
		EXEC MULTIPLE_UPDATE				@SUBSTRING = 'MAG'				,@PRODUCER	= 'MAG',					@PROD_TYPE ='��������'	,@PROD_SUBTYPE='����'							,@PROD_CATEGORY=''						,@PROD_SUBCATEGORY='' ---
		EXEC MULTIPLE_UPDATE				@SUBSTRING = '���������'		,@PRODUCER	= '���������',				@PROD_TYPE ='������'	,@PROD_SUBTYPE='������ ��� �����'				,@PROD_CATEGORY=''						,@PROD_SUBCATEGORY='' ---
		EXEC MULTIPLE_UPDATE				@SUBSTRING = '������'			,@PRODUCER	= '������',					@PROD_TYPE ='��������'	,@PROD_SUBTYPE='������������ �������/��������'	,@PROD_CATEGORY=''						,@PROD_SUBCATEGORY='' ---
		EXEC MULTIPLE_UPDATE				@SUBSTRING = '������'			,@PRODUCER	= '������',					@PROD_TYPE ='��������'	,@PROD_SUBTYPE='������������ �������/��������'	,@PROD_CATEGORY=''						,@PROD_SUBCATEGORY='' ---
		EXEC MULTIPLE_UPDATE				@SUBSTRING = '�����'			,@PRODUCER	= '�����',					@PROD_TYPE ='��������'	,@PROD_SUBTYPE='���������/�������������'		,@PROD_CATEGORY=''						,@PROD_SUBCATEGORY=''
		EXEC MULTIPLE_UPDATE				@SUBSTRING ='������ ��������'	,@PRODUCER	= '������ ��������',		@PROD_TYPE ='��������'	,@PROD_SUBTYPE='����/�����'						,@PROD_CATEGORY='������'				,@PROD_SUBCATEGORY=''
	
		--select * from step0 where product_name like '%�����%'
		--��� �������� �������������, ��� ��������
		EXEC MULTIPLE_UPDATE			   @SUBSTRING = '���������� ����'	,@PRODUCER	= '',						@PROD_TYPE ='��������'	,@PROD_SUBTYPE='������������ �������/��������'	,@PROD_CATEGORY='���������� ����'	,@PROD_SUBCATEGORY='' ---
		EXEC MULTIPLE_UPDATE			   @SUBSTRING = 'LOVELY DOGS'		,@PRODUCER	= '',						@PROD_TYPE ='������'	,@PROD_SUBTYPE='������ ��� �����'				,@PROD_CATEGORY='���� � ��������'	,@PROD_SUBCATEGORY='' ---
		EXEC MULTIPLE_UPDATE			   @SUBSTRING = '�������-�������'	,@PRODUCER	= '',						@PROD_TYPE ='������'	,@PROD_SUBTYPE='������ ��� �����'				,@PROD_CATEGORY=''	,@PROD_SUBCATEGORY='' ---
		EXEC MULTIPLE_UPDATE			   @SUBSTRING = '������ '			,@PRODUCER	= '',						@PROD_TYPE ='������'	,@PROD_SUBTYPE='������ ��� ���� � ����'				,@PROD_CATEGORY='������'	,@PROD_SUBCATEGORY='������' ---
		
		/* ����� ��������������� ������� */
		-------------------------param1_pricefor----------------------------------------------------------
		-------------------------param1_pricefor----------------------------------------------------------
		-------------------------param1_pricefor----------------------------------------------------------
		UPDATE step0
		SET product_name=replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(product_name,	
										'���� �� ��' , '���� �� ��' ),'���� �� 1��' , '���� �� ��' ),', ��','���� �� ��'),', ���� �� ��','���� �� ��'),
										'���� �� 1��','���� �� ��'),'���/������/��','���/������/���� �� ��'),', ���� �� ��','���� �� ��'),
										'���� �� ��������','���� �� ��'),'���� �� ����','���� �� ��'),
										'���� �� 1�','���� �� �')

		EXEC EXTRACT_SUBSTRING  @SUBSTRING='���� �� ��'
		UPDATE step0 SET param1_pricefor=temp_param where temp_param!=''

		EXEC EXTRACT_SUBSTRING  @SUBSTRING='���� �� ��'
		UPDATE step0 SET param1_pricefor=temp_param where temp_param!=''

		EXEC EXTRACT_SUBSTRING  @SUBSTRING='���� �� ��'
		UPDATE step0 SET param1_pricefor=temp_param where temp_param!=''

		EXEC EXTRACT_SUBSTRING  @SUBSTRING='���� �� �'
		UPDATE step0 SET param1_pricefor=temp_param where temp_param!=''

		

		UPDATE step0
		SET product_name = replace(replace(replace(replace(replace(product_name,'(',' ( '),')',' ) '),'/',' / '),',���',', ���') ,'���,','��� ,')

		UPDATE step0
		SET product_name = replace(product_name,'��� /',' ��� /')
		WHERE product_name like '%[0-9]��� /%'

		/* ������ ������ ������� � ������� � ����� ������*/	
		UPDATE step0
		SET product_name = rtrim(replace(product_name,'  ',' '))

		EXEC EXTRACT_SUBSTRING  @SUBSTRING=' ��� / ���'		UPDATE step0 SET param1_ves=temp_param where temp_param!=''
		EXEC EXTRACT_SUBSTRING  @SUBSTRING=' ��� / ���'		UPDATE step0 SET param1_ves=temp_param where temp_param!=''
		EXEC EXTRACT_SUBSTRING  @SUBSTRING=' ��� / ��'		UPDATE step0 SET param1_ves=temp_param where temp_param!=''
		EXEC EXTRACT_SUBSTRING  @SUBSTRING=' ��� '			UPDATE step0 SET param1_ves=temp_param where temp_param!=''
		EXEC EXTRACT_SUBSTRING  @SUBSTRING=' ��� / ���'		UPDATE step0 SET param1_ves=temp_param where temp_param!=''
		EXEC EXTRACT_SUBSTRING  @SUBSTRING=' ��� / ���'		UPDATE step0 SET param1_ves=temp_param where temp_param!=''

		------------------------------param3_gramm------------------------------------------------
		------------------------------param3_gramm------------------------------------------------
		------------------------------param3_gramm------------------------------------------------

		-- ��������� ���������� ��� �����
		UPDATE step0 
		set product_name=replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(product_name,
				'-������100�','-������ 100�'),'��������14�','�������� 14�'),'���� ������� ���.2�','���� ������� ���. 2�'),'�������80�','������� 80�'),
				'�����170�','����� 170�'),'�������80�','������� 80�'),'����.��������100�','����.�������� 100�'),'�����6�','����� 6�'),'������180�','������ 180�'),
				'���.40�7B1�','���.40�7B1_�'),'���.80387�','���.80387_�'),'������ 4,2�*5','������ 5*4,2�'),'�Ǡ����۠38�','�Ǡ����� 38�'),
				'�Ǡ�������38�','�Ǡ������ 38�'),'���.40��7�1�','���.40��7�1_�'),'�������170�','������� 170�'),'���.����.32�','���.����. 32�'),
				'�����75�','����� 75�'),'���.����.116�','���.����. 116�'),'�����80�','����� 80�'),'����100�','���� 100�'),'����*0,5�','���� *0,5�'),
				'432-318 ��','432-318 _��'),'48 ����','48 _����'),'���������-�����370�','���������-����� 370�'),'4 ��*65','4��*65')				
				,'������Š������ʠ270� /','������Š������� 270� /')

		UPDATE step0
		SET param6_tara = '��',
			product_name = replace(replace(product_name,'� � ��������','�'),'�� � ��������','�')
		where	product_name like '%[0-9]� � ��������%' 
			or product_name like '%[0-9] � � ��������%'  
			or product_name like '%[0-9]�� � ��������%'  
			or product_name like '%[0-9] �� � ��������%'  

		EXEC REPLACE_MEASURE_SIMBOLS  @measure_simbol= '�' , @variant1 = '��'
		EXEC EXTRACT_MEASURE @measure_simbol= '�' 
		UPDATE step0 SET param3_gramm=temp_param

		-- � param3_special ������� ������� (��������� ������, ������� ���� ��������� � �� ����������� �������� ���������� ��� ���������� ��� �������� ��� ���� � �� �������� ��������)
		EXEC EXTRACT_MEASURE @measure_simbol= '�' 
		UPDATE step0 SET param3_special=temp_param where temp_param!=''

		
		------------------------------param3_kg------------------------------------------------
		------------------------------param3_kg------------------------------------------------
		------------------------------param3_kg------------------------------------------------
		-- ��������� ���������� ��� ��
		UPDATE step0 
		set product_name=replace(replace(replace(replace(product_name,	'0,3,��','0,3��'),'�� 90��','��_90��'),'1,2��*4','4*1,2��'),'1,1��*4','4*1,1��')

		EXEC EXTRACT_MEASURE @measure_simbol= '��' 
		UPDATE step0 SET param3_kg=temp_param
		where param3_kg not like '%[0-9]-[0-9][0-9]%'   and product_name not like '%��_90��%'

		EXEC EXTRACT_MEASURE @measure_simbol= '��' 
		UPDATE step0 SET param19_subcategory=temp_param
		where param3_kg  like '%[0-9]-[0-9][0-9]%'   and product_name like '%��_90��%'

		-- � param3_special ������� ������� (��������� ������, ������� ���� ��������� � �� ����������� �������� ���������� ��� ���������� ��� �������� ��� ���� � �� �������� ��������)
		EXEC EXTRACT_MEASURE @measure_simbol= '��' 
		UPDATE step0 SET param3_special=temp_param where temp_param!=''

	
		------------------------------param7_percent------------------------------------------------
		------------------------------param7_percent------------------------------------------------
		------------------------------param7_percent------------------------------------------------

		UPDATE step0 set product_name = replace(product_name, '%','percent')

		EXEC EXTRACT_MEASURE @measure_simbol= 'percent' 
		UPDATE step0 SET param7_percent=replace(temp_param,'percent','%')

		--select * from step0 where param7_percent !=''

		-- � param5_special ������� ������� (��������� ������, ������� ���� ��������� � �� ����������� �������� ���������� ��� ���������� ��� �������� ��� ���� � �� �������� ��������)
		--EXEC EXTRACT_MEASURE @measure_simbol= '%' 

		--select * from step0 where  param7_percent !=''
		------------------------------param8_sm------------------------------------------------
		------------------------------param8_sm------------------------------------------------
		------------------------------param8_sm------------------------------------------------

		UPDATE step0
		set product_name = replace(replace(product_name, ' X ', 'X'), ' � ', '�')

		where product_name like '%[0-9] X [0-9]%'
		OR product_name like '%[0-9] � [0-9]%'

		EXEC EXTRACT_MEASURE @measure_simbol= '��' 
		UPDATE step0 SET param8_sm=temp_param

		--select * from step0 where param8_sm !=''

		-- � param8_special ������� ������� (��������� ������, ������� ���� ��������� � �� ����������� �������� ���������� ��� ���������� ��� �������� ��� ���� � �� �������� ��������)
		EXEC EXTRACT_MEASURE @measure_simbol= '��' 
		UPDATE step0 SET param8_special=temp_param where temp_param!=''

		EXEC EXTRACT_MEASURE @measure_simbol= '�' 
		UPDATE step0 SET param8_m=temp_param

		-- � param8_special ������� ������� (��������� ������, ������� ���� ��������� � �� ����������� �������� ���������� ��� ���������� ��� �������� ��� ���� � �� �������� ��������)
		EXEC EXTRACT_MEASURE @measure_simbol= '�' 
		UPDATE step0 SET param8_special=temp_param where temp_param!=''

		--select * from step0 where param8_special !=''
		
		------------------------------param5_items------------------------------------------------
		------------------------------param5_items------------------------------------------------
		------------------------------param5_items------------------------------------------------
		UPDATE step0
		SET product_name = replace(replace(replace(replace(replace(product_name,'�� � ��������','��'),'��.� ��-��','��'),'��. � ��','��'),'�� � ��','��'),'� ����. 100 ����','100��')
		, param6_tara = '��'
		WHERE	product_name like '%�� � ��������%'
			or product_name  like '%��.� ��-��%'
			or product_name  like '%��. � ��%'
			or product_name  like '%�� � ��%'
			or product_name  like '%� ����. 100 ����%'

		EXEC EXTRACT_MEASURE @measure_simbol= '��' 
		UPDATE step0 SET param5_items=temp_param

		EXEC EXTRACT_MEASURE @measure_simbol= '����' 
		UPDATE step0 SET param5_items=temp_param

		--!! ���������, ��� � �����������, ��� � ����� � ��. �������� �����

		-- � param5_special ������� ������� (��������� ������, ������� ���� ��������� � �� ����������� �������� ���������� ��� ���������� ��� �������� ��� ���� � �� �������� ��������)
		EXEC EXTRACT_MEASURE @measure_simbol= '��' 
		UPDATE step0 SET param5_special=temp_param where temp_param!=''
		
		------------------------------param6_tara------------------------------------------------
		------------------------------param6_tara------------------------------------------------
		------------------------------param6_tara------------------------------------------------
		update step0
		set product_name = replace(product_name,'����� �','������ �')
		where product_name like '%����� �%'

		update step0
		set product_name = replace(product_name,' � / �',' � / ��')
		where product_name like '% � / � %'

		update step0
		set product_name = replace(product_name,'������','�������')
		where product_name like '%������%' and product_name like '%����%'

		update step0
		set product_name = replace(product_name,'�������','��������')
		where product_name like '%�������%' and product_name like '%����%'

		UPDATE step0  SET product_name = replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(product_name
														,'� ������','�����')
														,'������ ��-��','� / ��')
														,'� ��������.����','� / ��'),'������� ��-��','� / ��'),'����� / ��','� / ��'),'�� / ����','� / ��'),'�� / ��','� / ��')
														,'����� ��-��','� / ��'),'��� / ����','� / ��'),'��� / ��','� / ��')
														,'������ ��-��','� / ��'),'���� / ��','� / ��'),'���� ��','� / ��')
														,'��� / ��','� / ��')												
														,'������ ��-��','� / ��')
														,'� ����� / ��������','��� / ��'),'���.��.','��� / �� ')
														,'������� �����','�� / �')	,'� / �','�� / �')
														,'� / ���','�� / ���')

		EXEC EXTRACT_SUBSTRING  @SUBSTRING='�������'		UPDATE step0 SET param6_tara=temp_param where temp_param!=''
		EXEC EXTRACT_SUBSTRING  @SUBSTRING='�����'			UPDATE step0 SET param6_tara=temp_param where temp_param!=''									
		EXEC EXTRACT_SUBSTRING  @SUBSTRING=' � / ��'		UPDATE step0 SET param6_tara=temp_param where temp_param!=''
		EXEC EXTRACT_SUBSTRING  @SUBSTRING=' � / ��'		UPDATE step0 SET param6_tara=temp_param where temp_param!=''
		EXEC EXTRACT_SUBSTRING  @SUBSTRING=' � / ��'		UPDATE step0 SET param6_tara=temp_param where temp_param!=''
		EXEC EXTRACT_SUBSTRING  @SUBSTRING=' � / ��'		UPDATE step0 SET param6_tara=temp_param where temp_param!=''
		EXEC EXTRACT_SUBSTRING  @SUBSTRING=' � / ��'		UPDATE step0 SET param6_tara=temp_param where temp_param!=''
		EXEC EXTRACT_SUBSTRING  @SUBSTRING=' � / ��'		UPDATE step0 SET param6_tara=temp_param where temp_param!=''
		EXEC EXTRACT_SUBSTRING  @SUBSTRING=' ����� / ��'	UPDATE step0 SET param6_tara=temp_param where temp_param!=''
		EXEC EXTRACT_SUBSTRING  @SUBSTRING=' ��� / ��'		UPDATE step0 SET param6_tara=temp_param where temp_param!=''
		EXEC EXTRACT_SUBSTRING  @SUBSTRING=' � / ���'		UPDATE step0 SET param6_tara=temp_param where temp_param!=''
		EXEC EXTRACT_SUBSTRING  @SUBSTRING=' �� / �'		UPDATE step0 SET param6_tara=temp_param where temp_param!=''
		UPDATE step0  SET product_name = replace(replace(replace(replace(replace(product_name,'����� ��������',' �� '),'��������',' �� '),' ����',' �� '),' ��',' �� '),'1��','���� �� ��')
		--EXEC EXTRACT_SUBSTRING  @SUBSTRING=' ��'			UPDATE step0 SET param6_tara=temp_param where temp_param!=''
		-- !!����� �������� �������� � ����������� ��� ��� �������� \ �����
		-- !!���������� ������� ����� ���� ���� JUNIOR ����� �������� / 12
		EXEC EXTRACT_SUBSTRING  @SUBSTRING='���� / ��'		UPDATE step0 SET param6_tara=temp_param where temp_param!=''
		EXEC EXTRACT_SUBSTRING  @SUBSTRING='���� �� ��'		UPDATE step0 SET param1_pricefor=temp_param where temp_param!=''
		EXEC EXTRACT_SUBSTRING  @SUBSTRING=' �� '			UPDATE step0 SET param6_tara=temp_param where temp_param!=''
		EXEC EXTRACT_SUBSTRING  @SUBSTRING='� / �'			UPDATE step0 SET param6_tara=temp_param where temp_param!=''
		EXEC EXTRACT_SUBSTRING  @SUBSTRING='� / �'			UPDATE step0 SET param6_tara=temp_param where temp_param!=''
		EXEC EXTRACT_SUBSTRING  @SUBSTRING='�� / ���'		UPDATE step0 SET param6_tara=temp_param where temp_param!=''
		EXEC EXTRACT_SUBSTRING  @SUBSTRING='�� / ��'		UPDATE step0 SET param6_tara=temp_param where temp_param!=''
		EXEC EXTRACT_SUBSTRING  @SUBSTRING='�� / ���'		UPDATE step0 SET param6_tara=temp_param where temp_param!=''
		EXEC EXTRACT_SUBSTRING  @SUBSTRING='�� / �'			UPDATE step0 SET param6_tara=temp_param where temp_param!=''
		-- ������, ��� �� / � ==������� �����
		EXEC EXTRACT_SUBSTRING  @SUBSTRING='������'			UPDATE step0 SET param6_tara=temp_param where temp_param!=''
		EXEC EXTRACT_SUBSTRING  @SUBSTRING='������'			UPDATE step0 SET param6_tara=temp_param where temp_param!=''
		EXEC EXTRACT_SUBSTRING  @SUBSTRING='� / ���'		UPDATE step0 SET param6_tara=temp_param where temp_param!=''
		EXEC EXTRACT_SUBSTRING  @SUBSTRING='�� / ���'		UPDATE step0 SET param6_tara=temp_param where temp_param!=''
		EXEC EXTRACT_SUBSTRING  @SUBSTRING='�� / � '		UPDATE step0 SET param6_tara=temp_param where temp_param!=''

		--- ����������\ ����������� �� / � == �� / ��� ?

		UPDATE step0
		SET param6_tara = '������',
			product_name=replace(product_name, '������' , '' )
		WHERE	product_name		like '%������%'	-- ���������� ��������� �������� '������'
			and	product_name	not	like '%���������%'
			and	product_name	not like '%������ �/��������%'
			and param6_tara =	''

		EXEC EXTRACT_SUBSTRING  @SUBSTRING='���'			UPDATE step0 SET param6_tara=temp_param where temp_param!=''
		EXEC EXTRACT_SUBSTRING  @SUBSTRING='�����'			UPDATE step0 SET param6_tara=temp_param where temp_param!=''
		EXEC EXTRACT_SUBSTRING  @SUBSTRING='�� / �����'		UPDATE step0 SET param6_tara=temp_param where temp_param!=''
		EXEC EXTRACT_SUBSTRING  @SUBSTRING='�� / ��������'  UPDATE step0 SET param6_tara=temp_param where temp_param!=''
		EXEC EXTRACT_SUBSTRING  @SUBSTRING='�� / ���'		UPDATE step0 SET param6_tara=temp_param where temp_param!=''
		EXEC EXTRACT_SUBSTRING  @SUBSTRING='�����'			UPDATE step0 SET param6_tara=temp_param where temp_param!=''
		EXEC EXTRACT_SUBSTRING  @SUBSTRING='������'			UPDATE step0 SET param6_tara=temp_param where temp_param!=''
		EXEC EXTRACT_SUBSTRING  @SUBSTRING='��������'		UPDATE step0 SET param6_tara=temp_param where temp_param!=''

		UPDATE step0
		SET param6_tara =''
		where	param6_tara ='�����'  
		and (product_name_origin like  '%����� �����%' or product_name_origin like '%�����%�����%' or product_name_origin like  '%�����%������%')

		/*
		select * from step0 where param3_gramm != ''   --5912
		select * from step0 where param3_kg != ''   --458
		select * from step0 where param3_special != ''   --13

		select * from step0 where param4_litr != ''   --1842
		select * from step0 where param4_special != ''   --3

		select * from step0 where param5_items != ''   --1
		select * from step0 where param5_special != ''   --1

		select sales.*, step0.* from step0 
		left join sales
		on step0.product_id=sales.product_id
		where product_name like '%��%'
		*/
		
		------param11_CCC=(���) ------------------------------------------------------------------------------------
		------param11_CCC=(���) ------------------------------------------------------------------------------------
		------param11_CCC=(���) ------------------------------------------------------------------------------------

		update step0 --53
		set product_name = rtrim(replace(replace(product_name,'� ( ��� )','( ��� )'),'� ( ��� )','( ��� )')),
		param12='�'
		where product_name like '%( ��� )%' 

		update step0 --53
		set product_name = rtrim(replace(product_name,'( ��� )','')),
		param11_CCC='(���)'
		where product_name like '%( ��� )%' 
		
		------param14_pch_part =�� / ����---------------------------------------------------------------------------------------
		------param14_pch_part =�� / ����---------------------------------------------------------------------------------------
		------param14_pch_part =�� / ����---------------------------------------------------------------------------------------

		update step0 --3
		set param14_pch_part='��',
		product_name = rtrim(substring(product_name, 1, len(product_name)-2))
		where product_name like '% ��'

		update step0 --2
		set param14_pch_part='����',
		product_name = rtrim(substring(product_name, 1, len(product_name)-4))
		where product_name like '% ����'

		update step0 --6
		set param14_pch_part='����',
		product_name = rtrim(replace(product_name, '( ����',''))
		where product_name like '%( ����%'

		
		------------------------������ ���� (type,subtype, category,subcategory)---------------------------
		------------------------������ ���� (type,subtype, category,subcategory)---------------------------
		------------------------������ ���� (type,subtype, category,subcategory)---------------------------
		UPDATE step0
		SET 
			product_name =  replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace( replace(replace(replace(replace(replace(replace(replace(replace(replace(product_name, 
												'��� ����� ( ���� )','��� ����'),
												'��� / ���','��� ���') 
												,'����������','���������'),'� / �� ��������� ( �������� )','��������� � / ��')
												, '����� �����������','��� � / ��'),'��� �����������','��� � / ��'),'����������� �����','��� � / ��'),'����� ���� �����������','��� � / ��')
												,'����������� �������','�� � / ��')
												, '�� �������','�� ��')
												,'��� �������','��� ��'),'��� ����','��� ��'),'����� �������','��� ��'),'����� ���� �������','��� ��')
												,'������� �������','��� ��'),'��� �������','��� ��')
												,'�����','���')
												,'�������','��')
												,'�������','��������')
												,'SANGRIA','�������')

		UPDATE step0
		SET product_name =  replace(replace(replace(product_name, '��������� ( �������� )',''),'���������',''),'� / ��','��������� � / ��')
		where product_name like '%���������%'
		and product_name like '%����%'
		and product_name like '%� / ��%'

		/* ��� ��� ������� �����, ������� ���� �� ��������� � ������� ������ product_category_rules. �.�. ���� ������� ����������� ����� - ����������� */
	    /* ������ ��������������� ������� */		
		EXEC MULTIPLE_UPDATE_PRODSUBTYPE	@SUBSTRING = '�� ���'		,@PRODUCER	= ''	,@PROD_TYPE ='��������'	,@PROD_SUBTYPE='���� '				,@PROD_CATEGORY=''	,@PROD_SUBCATEGORY='�� ���'
		EXEC MULTIPLE_UPDATE_PRODSUBTYPE	@SUBSTRING = '�� ���'		,@PRODUCER	= ''	,@PROD_TYPE ='��������'	,@PROD_SUBTYPE='������ ���.'		,@PROD_CATEGORY=''	,@PROD_SUBCATEGORY='�� ���'
		EXEC MULTIPLE_UPDATE_PRODSUBTYPE	@SUBSTRING = '��� ����'		,@PRODUCER	= ''	,@PROD_TYPE ='��������'	,@PROD_SUBTYPE='����������'			,@PROD_CATEGORY=''	,@PROD_SUBCATEGORY='��� ����'
		EXEC MULTIPLE_UPDATE				@SUBSTRING = '��� ����'		,@PRODUCER	= ''	,@PROD_TYPE ='��������'	,@PROD_SUBTYPE='����'				,@PROD_CATEGORY=''	,@PROD_SUBCATEGORY='��� ����'
		EXEC MULTIPLE_UPDATE_PRODSUBTYPE	@SUBSTRING = '��� ���'		,@PRODUCER	= ''	,@PROD_TYPE ='��������'	,@PROD_SUBTYPE='������ ���.'		,@PROD_CATEGORY=''	,@PROD_SUBCATEGORY='��� ���'
		EXEC MULTIPLE_UPDATE				@SUBSTRING = '��� ���'		,@PRODUCER	= ''	,@PROD_TYPE ='��������'	,@PROD_SUBTYPE='����'				,@PROD_CATEGORY=''	,@PROD_SUBCATEGORY='��� ���'
		EXEC MULTIPLE_UPDATE_PRODSUBTYPE	@SUBSTRING = '��������� � / ��'	,@PRODUCER	= '',@PROD_TYPE ='��������'	,@PROD_SUBTYPE='����'				,@PROD_CATEGORY=''	,@PROD_SUBCATEGORY='��������� � / ��'
		EXEC MULTIPLE_UPDATE_PRODSUBTYPE	@SUBSTRING='��������� ( �������� )',@PRODUCER='',@PROD_TYPE ='��������'	,@PROD_SUBTYPE='����'				,@PROD_CATEGORY=''	,@PROD_SUBCATEGORY='���������'
		EXEC MULTIPLE_UPDATE_PRODSUBTYPE	@SUBSTRING = '�� � / ��'	,@PRODUCER	= ''	,@PROD_TYPE ='��������'	,@PROD_SUBTYPE='������ ���. ���.'	,@PROD_CATEGORY=''	,@PROD_SUBCATEGORY='�� � / ��'
		EXEC MULTIPLE_UPDATE_PRODSUBTYPE	@SUBSTRING = '�� � / ��'	,@PRODUCER	= ''	,@PROD_TYPE ='��������'	,@PROD_SUBTYPE='������ ���.'		,@PROD_CATEGORY=''	,@PROD_SUBCATEGORY='�� � / ��'
		EXEC MULTIPLE_UPDATE_PRODSUBTYPE	@SUBSTRING = '�� � / ��'	,@PRODUCER	= ''	,@PROD_TYPE ='��������'	,@PROD_SUBTYPE='����'				,@PROD_CATEGORY=''	,@PROD_SUBCATEGORY='�� � / ��'
		EXEC MULTIPLE_UPDATE_PRODSUBTYPE	@SUBSTRING = '��� � / ��'	,@PRODUCER	= ''	,@PROD_TYPE ='��������'	,@PROD_SUBTYPE='������ ���. ���.'	,@PROD_CATEGORY=''	,@PROD_SUBCATEGORY='��� � / ��'
		EXEC MULTIPLE_UPDATE_PRODSUBTYPE	@SUBSTRING = '��� � / ��'	,@PRODUCER	= ''	,@PROD_TYPE ='��������'	,@PROD_SUBTYPE='������ ���.'		,@PROD_CATEGORY=''	,@PROD_SUBCATEGORY='��� � / ��'
		EXEC MULTIPLE_UPDATE_PRODSUBTYPE	@SUBSTRING = '��� � / ��'	,@PRODUCER	= ''	,@PROD_TYPE ='��������'	,@PROD_SUBTYPE='����'				,@PROD_CATEGORY=''	,@PROD_SUBCATEGORY='��� � / ��'
		EXEC MULTIPLE_UPDATE_PRODSUBTYPE	@SUBSTRING = '��� � / ��'	,@PRODUCER	= ''	,@PROD_TYPE ='��������'	,@PROD_SUBTYPE='����������'			,@PROD_CATEGORY=''	,@PROD_SUBCATEGORY='��� � / ��'
		EXEC MULTIPLE_UPDATE_PRODSUBTYPE	@SUBSTRING = '��� � / ��'	,@PRODUCER	= ''	,@PROD_TYPE ='��������'	,@PROD_SUBTYPE='������ ���. ���.'	,@PROD_CATEGORY=''	,@PROD_SUBCATEGORY='��� � / ��'
		EXEC MULTIPLE_UPDATE_PRODSUBTYPE	@SUBSTRING = '��� � / ��'	,@PRODUCER	= ''	,@PROD_TYPE ='��������'	,@PROD_SUBTYPE='������ ���.'		,@PROD_CATEGORY=''	,@PROD_SUBCATEGORY='��� � / ��'
		EXEC MULTIPLE_UPDATE_PRODSUBTYPE	@SUBSTRING = '��� � / ��'	,@PRODUCER	= ''	,@PROD_TYPE ='��������'	,@PROD_SUBTYPE='����'				,@PROD_CATEGORY=''	,@PROD_SUBCATEGORY='��� � / ��'
		EXEC MULTIPLE_UPDATE_PRODSUBTYPE	@SUBSTRING = '��� � / ��'	,@PRODUCER	= ''	,@PROD_TYPE ='��������'	,@PROD_SUBTYPE='����������'			,@PROD_CATEGORY=''	,@PROD_SUBCATEGORY='��� � / ��'
		EXEC MULTIPLE_UPDATE_PRODSUBTYPE	@SUBSTRING = '�� ��'		,@PRODUCER	= ''	,@PROD_TYPE ='��������'	,@PROD_SUBTYPE='����'				,@PROD_CATEGORY=''	,@PROD_SUBCATEGORY='�� ��'
		EXEC MULTIPLE_UPDATE_PRODSUBTYPE	@SUBSTRING = '��� ��'		,@PRODUCER	= ''	,@PROD_TYPE ='��������'	,@PROD_SUBTYPE='����'				,@PROD_CATEGORY=''	,@PROD_SUBCATEGORY='��� ��'
		EXEC MULTIPLE_UPDATE_PRODSUBTYPE	@SUBSTRING = '��� � / ��'	,@PRODUCER	= ''	,@PROD_TYPE ='��������'	,@PROD_SUBTYPE='������ ���. ���.'	,@PROD_CATEGORY=''	,@PROD_SUBCATEGORY='��� � / ��'
		EXEC MULTIPLE_UPDATE_PRODSUBTYPE	@SUBSTRING = '��� � / ��'	,@PRODUCER	= ''	,@PROD_TYPE ='��������'	,@PROD_SUBTYPE='����'				,@PROD_CATEGORY=''	,@PROD_SUBCATEGORY='��� � / ��'
		EXEC MULTIPLE_UPDATE_PRODSUBTYPE	@SUBSTRING = '�� � / ���'	,@PRODUCER	= ''	,@PROD_TYPE ='��������'	,@PROD_SUBTYPE='����'				,@PROD_CATEGORY=''	,@PROD_SUBCATEGORY='�� � / ���'
		EXEC MULTIPLE_UPDATE_PRODSUBTYPE	@SUBSTRING = '��� � / ���'	,@PRODUCER	= ''	,@PROD_TYPE ='��������'	,@PROD_SUBTYPE='����������'			,@PROD_CATEGORY=''	,@PROD_SUBCATEGORY='����������'
		EXEC MULTIPLE_UPDATE_PRODSUBTYPE	@SUBSTRING = '��� � / ���'	,@PRODUCER	= ''	,@PROD_TYPE ='��������'	,@PROD_SUBTYPE='����'				,@PROD_CATEGORY=''	,@PROD_SUBCATEGORY='��� � / ���'
		EXEC MULTIPLE_UPDATE_PRODSUBTYPE	@SUBSTRING = '��� ���'		,@PRODUCER	= ''	,@PROD_TYPE ='��������'	,@PROD_SUBTYPE='����'				,@PROD_CATEGORY=''	,@PROD_SUBCATEGORY='��� ���'
		EXEC MULTIPLE_UPDATE_PRODSUBTYPE	@SUBSTRING = '���'			,@PRODUCER	= ''	,@PROD_TYPE ='��������'	,@PROD_SUBTYPE='����'				,@PROD_CATEGORY=''	,@PROD_SUBCATEGORY='���'
		EXEC MULTIPLE_UPDATE_PRODSUBTYPE	@SUBSTRING = '�� ���������'	,@PRODUCER	= ''	,@PROD_TYPE ='��������'	,@PROD_SUBTYPE='����'				,@PROD_CATEGORY=''	,@PROD_SUBCATEGORY='�� ���������'
		EXEC MULTIPLE_UPDATE_PRODSUBTYPE	@SUBSTRING = '��� ��'		,@PRODUCER	= ''	,@PROD_TYPE ='��������'	,@PROD_SUBTYPE='������ ���. ���.'	,@PROD_CATEGORY=''	,@PROD_SUBCATEGORY='��� ��'
		EXEC MULTIPLE_UPDATE_PRODSUBTYPE	@SUBSTRING = '��� ��'		,@PRODUCER	= ''	,@PROD_TYPE ='��������'	,@PROD_SUBTYPE='������ ���. ���.'	,@PROD_CATEGORY=''	,@PROD_SUBCATEGORY='��� ��'
		EXEC MULTIPLE_UPDATE_PRODSUBTYPE	@SUBSTRING = '���'			,@PRODUCER	= ''	,@PROD_TYPE ='��������'	,@PROD_SUBTYPE='������ ���. ���.'	,@PROD_CATEGORY=''	,@PROD_SUBCATEGORY='���'
		EXEC MULTIPLE_UPDATE_PRODSUBTYPE	@SUBSTRING = '� / ��'		,@PRODUCER	= ''	,@PROD_TYPE ='��������'	,@PROD_SUBTYPE='������ ���. ���.'	,@PROD_CATEGORY=''	,@PROD_SUBCATEGORY='� / ��'
		EXEC MULTIPLE_UPDATE				@SUBSTRING='������ ���. ���.',@PRODUCER	= ''	,@PROD_TYPE ='��������'	,@PROD_SUBTYPE='������ ���. ���.'	,@PROD_CATEGORY=''	,@PROD_SUBCATEGORY=''
		EXEC MULTIPLE_UPDATE_PRODCATEGORY	@SUBSTRING ='��������������',@PRODUCER	= ''	,@PROD_TYPE ='��������'	,@PROD_SUBTYPE='�������'			,@PROD_CATEGORY='������ ���.'	,@PROD_SUBCATEGORY='��������������'
		EXEC MULTIPLE_UPDATE_PRODSUBTYPE	@SUBSTRING = '������'		,@PRODUCER	= ''	,@PROD_TYPE ='��������'	,@PROD_SUBTYPE='������ ���.'		,@PROD_CATEGORY=''	,@PROD_SUBCATEGORY='������'
		EXEC MULTIPLE_UPDATE_PRODSUBTYPE	@SUBSTRING = '�������'		,@PRODUCER	= ''	,@PROD_TYPE ='��������'	,@PROD_SUBTYPE='������ ���.'		,@PROD_CATEGORY=''	,@PROD_SUBCATEGORY='�������'
		EXEC MULTIPLE_UPDATE_PRODSUBTYPE	@SUBSTRING = '������'		,@PRODUCER	= ''	,@PROD_TYPE ='��������'	,@PROD_SUBTYPE='������ ���.'		,@PROD_CATEGORY=''	,@PROD_SUBCATEGORY='������'
		EXEC MULTIPLE_UPDATE_PRODSUBTYPE	@SUBSTRING = '���������'	,@PRODUCER	= ''	,@PROD_TYPE ='��������'	,@PROD_SUBTYPE='������ ���.'		,@PROD_CATEGORY=''	,@PROD_SUBCATEGORY='���������'
		EXEC MULTIPLE_UPDATE_PRODSUBTYPE	@SUBSTRING = '��������'		,@PRODUCER	= ''	,@PROD_TYPE ='��������'	,@PROD_SUBTYPE='������ ���.'		,@PROD_CATEGORY=''	,@PROD_SUBCATEGORY='��������'
		EXEC MULTIPLE_UPDATE_PRODSUBTYPE	@SUBSTRING = '�����'		,@PRODUCER	= ''	,@PROD_TYPE ='��������'	,@PROD_SUBTYPE='������ ���.'		,@PROD_CATEGORY=''	,@PROD_SUBCATEGORY='�����'
		--EXEC MULTIPLE_UPDATE_PRODSUBTYPE  @SUBSTRING = '���'			,@PRODUCER	= ''	,@PROD_TYPE ='��������'	,@PROD_SUBTYPE='������ ���.'		,@PROD_CATEGORY=''	,@PROD_SUBCATEGORY='���'
		EXEC MULTIPLE_UPDATE_PRODSUBTYPE	@SUBSTRING = '�� ��'		,@PRODUCER	= ''	,@PROD_TYPE ='��������'	,@PROD_SUBTYPE='������ ���.'		,@PROD_CATEGORY=''	,@PROD_SUBCATEGORY='�� ��'
		EXEC MULTIPLE_UPDATE_PRODSUBTYPE	@SUBSTRING = '�� ���'		,@PRODUCER	= ''	,@PROD_TYPE ='��������'	,@PROD_SUBTYPE='������ ���.'		,@PROD_CATEGORY=''	,@PROD_SUBCATEGORY='�� ���'
		--EXEC MULTIPLE_UPDATE_PRODSUBTYPE  @SUBSTRING = '��'			,@PRODUCER	= ''	,@PROD_TYPE ='��������'	,@PROD_SUBTYPE='������ ���.'		,@PROD_CATEGORY=''	,@PROD_SUBCATEGORY='������'
		EXEC MULTIPLE_UPDATE				@SUBSTRING = '������ ���.'	,@PRODUCER	= ''	,@PROD_TYPE ='��������'	,@PROD_SUBTYPE='������ ���.'		,@PROD_CATEGORY=''	,@PROD_SUBCATEGORY=''
		---!!�������� �� ���� : �������������, ������
		--����� ���� ������ ��������� ��� � ������ ��������
		EXEC MULTIPLE_UPDATE				@SUBSTRING = '������'		,@PRODUCER	= ''	,@PROD_TYPE ='��������'		,@PROD_SUBTYPE='������'			,@PROD_CATEGORY=''	,@PROD_SUBCATEGORY=''
		EXEC MULTIPLE_UPDATE				@SUBSTRING = '������'		,@PRODUCER	= ''	,@PROD_TYPE ='��������'		,@PROD_SUBTYPE='������'			,@PROD_CATEGORY=''	,@PROD_SUBCATEGORY=''
		EXEC MULTIPLE_UPDATE				@SUBSTRING = '�����'		,@PRODUCER	= ''	,@PROD_TYPE ='��������'		,@PROD_SUBTYPE='�����'			,@PROD_CATEGORY=''	,@PROD_SUBCATEGORY=''
		EXEC MULTIPLE_UPDATE				@SUBSTRING = '��������'		,@PRODUCER	= ''	,@PROD_TYPE ='��������'		,@PROD_SUBTYPE='��������'		,@PROD_CATEGORY=''	,@PROD_SUBCATEGORY=''
		EXEC MULTIPLE_UPDATE				@SUBSTRING = '���.�����.'	,@PRODUCER	= ''	,@PROD_TYPE ='��������'		,@PROD_SUBTYPE='���.�����.'		,@PROD_CATEGORY=''	,@PROD_SUBCATEGORY=''
		EXEC MULTIPLE_UPDATE				@SUBSTRING = '�����'		,@PRODUCER	= ''	,@PROD_TYPE ='��������'		,@PROD_SUBTYPE='�����'			,@PROD_CATEGORY=''	,@PROD_SUBCATEGORY=''
		EXEC MULTIPLE_UPDATE				@SUBSTRING = '����'			,@PRODUCER	= ''	,@PROD_TYPE ='��������'		,@PROD_SUBTYPE='����'			,@PROD_CATEGORY=''	,@PROD_SUBCATEGORY=''
		EXEC MULTIPLE_UPDATE				@SUBSTRING = '������ ���.'	,@PRODUCER	= ''	,@PROD_TYPE ='��������'		,@PROD_SUBTYPE='����'			,@PROD_CATEGORY=''	,@PROD_SUBCATEGORY=''
		EXEC MULTIPLE_UPDATE				@SUBSTRING = '�����������'	,@PRODUCER	= ''	,@PROD_TYPE ='��������'		,@PROD_SUBTYPE='����������� �������' ,@PROD_CATEGORY=''	,@PROD_SUBCATEGORY=''
		EXEC MULTIPLE_UPDATE				@SUBSTRING = '��������'		,@PRODUCER	= ''	,@PROD_TYPE ='��������'		,@PROD_SUBTYPE='��������'		,@PROD_CATEGORY=''	,@PROD_SUBCATEGORY=''
		EXEC MULTIPLE_UPDATE				@SUBSTRING = '� / ���.���.'	,@PRODUCER	= ''	,@PROD_TYPE ='��������'		,@PROD_SUBTYPE='� / ���.���.'	,@PROD_CATEGORY=''	,@PROD_SUBCATEGORY=''
		EXEC MULTIPLE_UPDATE				@SUBSTRING = '����'			,@PRODUCER	= ''	,@PROD_TYPE ='��������'		,@PROD_SUBTYPE='����'			,@PROD_CATEGORY=''	,@PROD_SUBCATEGORY=''
		/* ����� ��������������� ������� */
		
		--select * from step0 where product_name like '%����%' and param17_subtype ='' and product_name like '%������%'
		-- ��� '������ ��� ����' ������� ��������� �� ����� ������ ��� ������ ��� ����

		------------------------------param4_litr------------------------------------------------
		------------------------------param4_litr------------------------------------------------
		------------------------------param4_litr------------------------------------------------

		update step0  set product_name = replace(replace(replace(replace(replace(product_name, '� ', '���� '), '18�,', '18������'), '18�', '18������'), '�,', '���� '), '�.', '���� ')
		where	( product_name like '%[0-9]�%' or product_name like '%[0-9] �%' )
				and (product_name like '%�����%' or product_name like '%�������%' or product_name like '%������%' or product_name like '%������%' or product_name like '%�������%' or product_name like '%���������%' 
					 or product_name like '%����������%' or product_name like '%�������%' or product_name like '%���� ��� ���%' or product_name like '%������%' or product_name like '%�����%')

		update step0  set product_name = replace(product_name, '� ', '������ ')
		where product_name like '%[0-9]� %' 	and ( product_name like '%��������%'  
													or (product_name like '%��������%' and product_name not like '%��������%') 
													or product_name like '%�����-���������%'
													or product_name like '%���������%'
													or product_name like '%������ ��� ��������%'
													or product_name like '%������%'
													or product_name like '%�����%')

		update step0  set product_name = replace(replace(product_name, '� ', '������ '), '�+', '������ ')
		where ( product_name like '%[0-9]�%' or product_name like '%[0-9] �%' ) 	and (product_name like '%�������%'  or product_name like '%������-���������%' or product_name like '%������ � �������%' or product_name like '%������ � / �������������%')

		update step0  set product_name = replace(product_name, '�.', '������  ')
		where product_name like '%[0-9] � %' 	and product_name like '%��������-�������%' 

		update step0  set product_name = replace(product_name, '100�', '100����')
		where  product_name like '%��������%'  and product_name like '%100�%' 

		update step0
		set product_name  =replace(replace(replace(replace(product_name,'3���','3_���'),'5���','5_��� '),'3 ���','3_���'),'5 ���','5_��� ')

		update step0
		set product_name =replace(replace(replace(replace(product_name,'0,75','0,75�'),'1,5','1,5� '),'0,25','0,25�'),'0,5','0,5�')
		where 
		(product_name like '%0,75%'  and product_name not like '%0,75�%' and param16_type = '��������')
		or 
		(product_name like '%1,5%'	 and product_name not like '%1,5�%'  and param16_type = '��������')
		or 
		(product_name like '%0,25%'  and product_name not like '%0,25�%' and param16_type = '��������')
		or 
		(product_name like '%0,5%'   and product_name not like '%0,5�%'  and param16_type = '��������')


		UPDATE step0 
		set product_name=replace(replace(product_name,	'� / �1�','� / � 1�'),'0,75 / 6','0,75� / 6')

		--���������, ����� �� ��������� ����� � ��������

		EXEC EXTRACT_MEASURE @measure_simbol= '�' 
		UPDATE step0 SET param4_litr=temp_param

		-- � param5_special ������� ������� (��������� ������, ������� ���� ��������� � �� ����������� �������� ���������� ��� ���������� ��� �������� ��� ���� � �� �������� ��������)
		EXEC EXTRACT_MEASURE @measure_simbol= '�' 
		UPDATE step0 SET param4_special=temp_param where temp_param!=''
		
		------------------------------param4_ml------------------------------------------------
		------------------------------param4_ml------------------------------------------------
		------------------------------param4_ml------------------------------------------------

		EXEC EXTRACT_MEASURE @measure_simbol= '��' 
		UPDATE step0 SET param4_ml=temp_param

		-- � param5_special ������� ������� (��������� ������, ������� ���� ��������� � �� ����������� �������� ���������� ��� ���������� ��� �������� ��� ���� � �� �������� ��������)
		UPDATE STEP0 set product_name = replace(product_name,'+200��+250��','+450��') where product_name like '%+200��+250��%'
		UPDATE STEP0 set product_name = replace(product_name,'+',' +') where param4_ml !=''

		EXEC EXTRACT_MEASURE @measure_simbol= '��' 
		UPDATE step0 SET param4_special=temp_param where temp_param!=''
		

		------------------------------param10------------------------------------------------
		------------------------------param10------------------------------------------------
		------------------------------param10------------------------------------------------
		---------------- �������� ����� ����� '/'  ------------------------------------------
		update step0  --1033
		set product_name = substring(product_name,1,len(product_name)-1)
		where product_name like '%[0-9]"' 

		UPDATE step0  --8973
		SET param10_num1 = substring(product_name,len(product_name)- CHARINDEX('/ ',reverse(product_name))+2 ,len(product_name)),
			product_name = substring(product_name, 1, len(product_name)- CHARINDEX('/ ',reverse(product_name)) )
		where	 product_name like '%/ '  -- 2 ��� select * from step0 where product_id in (3487,7031)
				OR  product_name like '%/'  -- 2 ��� select * from step0 where product_id in (3487,7031)
				OR product_name like '%/ [0-9]' 
				OR product_name like '%/ [0-9][0-9]'  
				OR product_name like '%/ [0-9][0-9][0-9]'  
				OR product_name like '%/ [0-9][0-9][0-9][0-9]'   

		UPDATE step0 SET product_name = rtrim(product_name)
		UPDATE step0   -- 380
		SET param10_num2 = substring(product_name,len(product_name)- CHARINDEX('/ ',reverse(product_name))+2 ,len(product_name)),
			product_name = substring(product_name, 1, len(product_name)- CHARINDEX('/ ',reverse(product_name)) )
		where	 ( product_name like '%/' and product_id not in (8754) )  -- 6 ���      SELECT * FROM products WHERE product_id in (2039,3339,5040,8013, 8754, 9293, 9416)
				OR product_name like '%/ [0-9]' 
				OR product_name like '%/ [0-9][0-9]' 
				OR product_name like '%/ [0-9][0-9][0-9]'  
				OR product_name like '%/ [0-9][0-9][0-9][0-9]'   
				OR product_name like '%/ [0-9][0-9][0-9][0-9][0-9][0-9]'  

		UPDATE step0 SET product_name = rtrim(product_name)
		UPDATE step0   -- 6
		SET param10_num3 = substring(product_name,len(product_name)- CHARINDEX('/ ',reverse(product_name))+2 ,len(product_name)),
			product_name = substring(product_name, 1, len(product_name)- CHARINDEX('/ ',reverse(product_name)) )
		where	( product_name like '%/' and product_id not in (8754) )  -- 3 ���      SELECT * FROM products WHERE product_id in (7865,8754,9150,10893)
				OR  product_name like '%/ [0-9][0-9][0-9]'  -- 3 ����� 
		
		UPDATE step0 SET product_name = rtrim(product_name)
		
		--select * from step0 where param4_litr = '' and product_name like '%[0-9]�%'
		
		EXEC EXTRACT_SUBSTRING  @SUBSTRING='/ 1 '		UPDATE step0	SET param10_num1=temp_param		where temp_param!=''	and param10_num1=''
														UPDATE step0	SET temp_param=''				where temp_param=param10_num1
														UPDATE step0	SET param10_num2=temp_param		where temp_param!=''	and param10_num2=''
														UPDATE step0	SET temp_param=''				where temp_param=param10_num2						
		EXEC EXTRACT_SUBSTRING  @SUBSTRING='/ 6 '		UPDATE step0	SET param10_num1=temp_param		where temp_param!=''	and param10_num1=''
														UPDATE step0	SET temp_param=''				where temp_param=param10_num1
														UPDATE step0	SET param10_num2=temp_param		where temp_param!=''	and param10_num2=''
														UPDATE step0	SET temp_param=''				where temp_param=param10_num2				
		EXEC EXTRACT_SUBSTRING  @SUBSTRING='/ 12 '		UPDATE step0	SET param10_num1=temp_param		where temp_param!=''	and param10_num1=''
														UPDATE step0	SET temp_param=''				where temp_param=param10_num1
														UPDATE step0	SET param10_num2=temp_param		where temp_param!=''	and param10_num2=''
														UPDATE step0	SET temp_param=''				where temp_param=param10_num2				
		EXEC EXTRACT_SUBSTRING  @SUBSTRING='/ 50 '		UPDATE step0	SET param10_num1=temp_param		where temp_param!=''	and param10_num1=''
														UPDATE step0	SET temp_param=''				where temp_param=param10_num1
														UPDATE step0	SET param10_num2=temp_param		where temp_param!=''	and param10_num2=''
														UPDATE step0	SET temp_param=''				where temp_param=param10_num2				
		EXEC EXTRACT_SUBSTRING  @SUBSTRING='/ 48 '		UPDATE step0	SET param10_num1=temp_param		where temp_param!=''	and param10_num1=''
														UPDATE step0	SET temp_param=''				where temp_param=param10_num1
														UPDATE step0	SET param10_num2=temp_param		where temp_param!=''	and param10_num2=''
														UPDATE step0	SET temp_param=''				where temp_param=param10_num2				
		EXEC EXTRACT_SUBSTRING  @SUBSTRING='/ 24 '		UPDATE step0	SET param10_num1=temp_param		where temp_param!=''	and param10_num1=''
														UPDATE step0	SET temp_param=''				where temp_param=param10_num1
														UPDATE step0	SET param10_num2=temp_param		where temp_param!=''	and param10_num2=''
														UPDATE step0	SET temp_param=''				where temp_param=param10_num2				
		EXEC EXTRACT_SUBSTRING  @SUBSTRING='/ 15 '		UPDATE step0	SET param10_num1=temp_param		where temp_param!=''	and param10_num1=''
														UPDATE step0	SET temp_param=''				where temp_param=param10_num1
														UPDATE step0	SET param10_num2=temp_param		where temp_param!=''	and param10_num2=''
														UPDATE step0	SET temp_param=''				where temp_param=param10_num2				
		EXEC EXTRACT_SUBSTRING  @SUBSTRING='/ 20 ' 		UPDATE step0	SET param10_num1=temp_param		where temp_param!=''	and param10_num1=''
														UPDATE step0	SET temp_param=''				where temp_param=param10_num1
														UPDATE step0	SET param10_num2=temp_param		where temp_param!=''	and param10_num2=''
														UPDATE step0	SET temp_param=''				where temp_param=param10_num2				
		EXEC EXTRACT_SUBSTRING  @SUBSTRING='/ 40 ' 		UPDATE step0	SET param10_num1=temp_param		where temp_param!=''	and param10_num1=''
														UPDATE step0	SET temp_param=''				where temp_param=param10_num1
														UPDATE step0	SET param10_num2=temp_param		where temp_param!=''	and param10_num2=''
														UPDATE step0	SET temp_param=''				where temp_param=param10_num2				
		EXEC EXTRACT_SUBSTRING  @SUBSTRING='/ 18 '		UPDATE step0	SET param10_num1=temp_param		where temp_param!=''	and param10_num1=''
														UPDATE step0	SET temp_param=''				where temp_param=param10_num1
														UPDATE step0	SET param10_num2=temp_param		where temp_param!=''	and param10_num2=''
														UPDATE step0	SET temp_param=''				where temp_param=param10_num2				
		update step0 set product_name = replace(product_name, '/ 36���','/ 36 ���') where product_id in (12138,12140,12147,12151)
		EXEC EXTRACT_SUBSTRING  @SUBSTRING='/ 36 '		UPDATE step0	SET param10_num1=temp_param		where temp_param!=''	and param10_num1=''
														UPDATE step0	SET temp_param=''				where temp_param=param10_num1
														UPDATE step0	SET param10_num2=temp_param		where temp_param!=''	and param10_num2=''
														UPDATE step0	SET temp_param=''				where temp_param=param10_num2				
		EXEC EXTRACT_SUBSTRING  @SUBSTRING='/ 45 '		UPDATE step0	SET param10_num1=temp_param		where temp_param!=''	and param10_num1=''
														UPDATE step0	SET temp_param=''				where temp_param=param10_num1
														UPDATE step0	SET param10_num2=temp_param		where temp_param!=''	and param10_num2=''
														UPDATE step0	SET temp_param=''				where temp_param=param10_num2				
		EXEC EXTRACT_SUBSTRING  @SUBSTRING='/ 100 '		UPDATE step0	SET param10_num1=temp_param		where temp_param!=''	and param10_num1=''
														UPDATE step0	SET temp_param=''				where temp_param=param10_num1
														UPDATE step0	SET param10_num2=temp_param		where temp_param!=''	and param10_num2=''
														UPDATE step0	SET temp_param=''				where temp_param=param10_num2				
		EXEC EXTRACT_SUBSTRING  @SUBSTRING='/ 05 '		UPDATE step0	SET param10_num1=temp_param		where temp_param!=''	and param10_num1=''
														UPDATE step0	SET temp_param=''				where temp_param=param10_num1
														UPDATE step0	SET param10_num2=temp_param		where temp_param!=''	and param10_num2=''
														UPDATE step0	SET temp_param=''				where temp_param=param10_num2				
														UPDATE step0	SET param10_num3=temp_param		where temp_param!=''	and param10_num3=''
														UPDATE step0	SET temp_param=''				where temp_param=param10_num2				

		UPDATE STEP0 
		SET	param10_num1=trim(replace(param10_num1,'/','')),
			param10_num2=trim(replace(param10_num2,'/','')),
			param10_num3=trim(replace(param10_num3,'/',''))
		where param10_num1!='' or param10_num2!='' or param10_num3!=''

		---------------------------������������� �������� / ������ -----------------------
		---------------------------������������� �������� / ������ -----------------------
		---------------------------������������� �������� / ������ -----------------------
		
		update step0
		set product_name =ltrim(replace(product_name, '!',''))
		where product_name like '!%'
		
-----------------------------------------------------------------------------------------------
		UPDATE step0
			SET 
				step0.param16_type =		CASE WHEN step0.param16_type=''			THEN coalesce(product_category_rules1.product_type,			product_category_rules2.product_type,			product_category_rules11.product_type ,'') ELSE step0.param16_type	END,
				step0.param17_subtype =		CASE WHEN step0.param17_subtype=''		THEN coalesce(product_category_rules1.product_subtype,		product_category_rules2.product_subtype,		product_category_rules11.product_subtype,'') ELSE step0.param17_subtype END,
				step0.param18_category =	CASE WHEN step0.param18_category=''		THEN coalesce(product_category_rules1.product_category,		product_category_rules2.product_category,		product_category_rules11.product_category,'') ELSE	step0.param18_category END ,
				step0.param19_subcategory =	CASE WHEN step0.param19_subcategory=''	THEN coalesce(product_category_rules1.product_subcategory,	product_category_rules2.product_subcategory,	product_category_rules11.product_subcategory,'') ELSE step0.param19_subcategory END ,  
				step0.param15_producer=		CASE WHEN step0.param15_producer=''		THEN coalesce(product_category_rules1.producer,				product_category_rules2.producer,				product_category_rules11.producer,'') ELSE step0.param15_producer	END
			FROM step0 
					LEFT JOIN product_category_rules as product_category_rules1
							ON		step0.product_name_origin like		product_category_rules1.product_name_included
								AND step0.product_name_origin not like product_category_rules1.product_name_excluded
								AND product_category_rules1.step_priority=1
					LEFT JOIN product_category_rules as product_category_rules2
							ON		step0.product_name_origin like		product_category_rules2.product_name_included
								AND step0.product_name_origin not like	product_category_rules2.product_name_excluded
								AND product_category_rules2.step_priority=2
					LEFT JOIN		product_category_rules as product_category_rules11
							ON		step0.product_name_origin like		product_category_rules11.product_name_included
								AND step0.product_name_origin not like	product_category_rules11.product_name_excluded
								AND product_category_rules11.step_priority=11
----------------------------------------------------------------------------------
		--select * from step0 where param17_subtype='' and product_name_origin like '%����%'

	
		---------------------------�������������� (�� � ��) � (�� � � ) � ������� ����, ��������� �������������� ����������-----------------------
		---------------------------�������������� (�� � ��) � (�� � � ) � ������� ����, ��������� �������������� ����������-----------------------
		---------------------------�������������� (�� � ��) � (�� � � ) � ������� ����, ��������� �������������� ����������-----------------------

		UPDATE step0
		SET param3_gramm= '2*85�',
			param4_ml = '250��'
		WHERE param3_gramm = '250��+2�85�'

		DROP TABLE IF EXISTS step1
		SELECT 
			replace(replace(param3_kg,'��',''),',','.') as param3_prepared,
			replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(param3_gramm
											,'�',''),',','.'),'�','*'),'��','')
											,'230-250','250'),'240-250','250'),'200-250','250'),'200-210','210'),'230-240','240'),'140-185','185'),'150-160','160')
											,'160-250','250'),'220-240','240'),'350-400','400'),'15-21','21'),'90-100','100'),'200-220','220'),'260-330','330')
											,'6���*5','6*5')  as param3G_prepared,
			replace(replace(replace(replace(param4_litr	
											,'�',''),',','.'),'075','0.75')
											,'1,5-1,75','1,75�') as param4L_prepared,
			replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(param4_ml	
											,'��',''),',','.'),'����500','500'),'2�2','4'),'���������.250','250'),'45+72','117'),'���+75','75')
											,'100-120','120'),'370-400','400') ,'240-250','250'),'230-250','250')  as param4ML_prepared,
			replace(replace(param4_special ,'��',''),'+','')as param4S_prepared,
			* 
		INTO step1
		FROM step0 

		DROP TABLE IF EXISTS step2
		SELECT 
			CASE	
				WHEN 	param3_kg != ''		and param3_prepared		like '[0-9]*%'	 			THEN substring(param3_prepared,		1,1) 
				WHEN	param3_gramm != ''	and param3G_prepared	like '[0-9]*%'				THEN substring(param3G_prepared,	1,1) 
				WHEN	param3_gramm != ''	and param3G_prepared	like '[0-9][0-9]*%'			THEN substring(param3G_prepared,	1,2) 
				WHEN	param3_gramm != ''	and param3G_prepared	like '[0-9][0-9][0-9]*%'	THEN substring(param3G_prepared,	1,3)
				ELSE 1
			END as coef,
	
			CASE 
				WHEN	param3_kg != ''		and param3_prepared		like '[0-9]*%'				THEN substring(param3_prepared, 3,len(param3_prepared)) 
				WHEN	param3_kg != ''		and param3_prepared	not like '[0-9]*%'				THEN param3_prepared
				WHEN	param3_gramm != ''	and param3G_prepared	like '*%'					THEN substring(param3G_prepared, 2,len(param3G_prepared)) 
				WHEN	param3_gramm != ''	and param3G_prepared	like '[0-9]*%'				THEN substring(param3G_prepared, 3,len(param3G_prepared)) 
				WHEN	param3_gramm != ''	and param3G_prepared	like '[0-9][0-9]*%'			THEN substring(param3G_prepared, 4,len(param3G_prepared)) 
				WHEN	param3_gramm != ''	and param3G_prepared	like '[0-9][0-9][0-9]*%'	THEN substring(param3G_prepared, 5,len(param3G_prepared)) 
				ELSE	param3G_prepared
			END  as  valueg	,
	
			1 as coefML,
	
			CASE 
				WHEN	param4_litr != ''		THEN param4L_prepared
				WHEN	param4_ml != ''		THEN param4ML_prepared
			END  as  valueML,
	
			CASE 
				WHEN	param7_percent != ''		THEN trim(replace(replace(replace(replace(param7_percent,'%',''),'���.',''),'���',''),',','.'))
			END  as  valuePercent
			,
			CASE 
				WHEN	param10_num1 != ''		THEN trim(replace(param10_num1,'/',''))
				WHEN	param10_num1 = '' and  param10_num2 != ''		THEN trim(replace(param10_num2,'/',''))
				WHEN	param10_num1 = '' and  param10_num2 = ''	and  param10_num3 != ''	THEN trim(replace(param10_num3,'/',''))
			END  as  valueClassNum
			,* 
		INTO step2
		FROM step1 

		
		DROP TABLE IF EXISTS step3
		SELECT 
			TRY_CONVERT(float, coef) as try_convert_coef   --- ������� �������, ����� �� �������� ���� ���, ��� ��� ��������
			,TRY_CONVERT(float, valueg) as try_convert_valueg
			,TRY_CONVERT(float, coefML) as try_convert_coefML
			,TRY_CONVERT(float, valueML) as try_convert_valueML
			,TRY_CONVERT(float, valuePercent) as try_convert_valuePercent
			,TRY_CONVERT(float, valueClassNum) as try_convert_valueClassNum
			,* 
		INTO step3
		FROM  step2


		DROP TABLE IF EXISTS step4
		SELECT 
			CASE WHEN param3_kg		!= '' THEN try_convert_coef*try_convert_valueg*1000 
				 WHEN param3_gramm	!= '' THEN try_convert_coef*try_convert_valueg
				 WHEN param4_litr	!= '' THEN try_convert_coef*try_convert_valueg*1000 
			END	as gramms,	
			CASE WHEN param4_litr	!= '' THEN try_convert_coefML*try_convert_valueML*1000 
				 WHEN param4_ml		!= '' THEN try_convert_coefML*try_convert_valueML 
			END	as mls,
			try_convert_valuePercent as percents,
			try_convert_valueClassNum as classnum,
			param18_category as product_category,
			*
		INTO step4
		FROM step3
		--��������� �������������� ����������
		UPDATE step4		SET gramms = gramms+75	WHERE param3_special like  '%+75%'    
		UPDATE step4		SET gramms = gramms+45	WHERE param3_special like  '%+45%'
		UPDATE step4		SET mls = mls+TRY_CONVERT(float, param4S_prepared)     WHERE product_name like  '%�����%'     and param4_special !='' 
		UPDATE step4		SET gramms = 13.6	    WHERE product_name_origin like '%���%�������%�����%30%' and gramms is null
		UPDATE step4		SET gramms = 13			WHERE product_name_origin like '%���%�������%�����%20%' and gramms is null
		UPDATE step4		SET mls = 500			WHERE param17_subtype like '����' and param6_tara like '�� / ���'	and param10_num1 = '20' and mls is null
		UPDATE step4		SET mls = 500			WHERE param17_subtype like '����' and param6_tara like '� / �'		and param10_num1 in ('24','6')	and mls is null
		UPDATE step4		SET mls = 1420			WHERE param17_subtype like '����' and param6_tara like '���'			and param10_num1 = '9'	and mls is null and product_name like '%1,42%'
		UPDATE step4		SET mls = 1000			WHERE param17_subtype like '����' and param6_tara like '���'		and param10_num1 = '9'	and mls is null and product_name like '% �%'
		UPDATE step4		SET param16_type = '��������',
								param17_subtype = '�������',
								param18_category = '����',
								param19_subcategory = '��������������'
							WHERE param16_type like '��������'  and param17_subtype = '����' and product_name like '%� / ���%'

		UPDATE step4		SET gramms =20		WHERE product_name_origin like '%������ �������%' and  param10_num1='36' and gramms is null
		UPDATE step4		SET gramms =40		WHERE product_name_origin like '%����� ����������%' and  param10_num1='20'  and gramms is null
		UPDATE step4		SET gramms =26		WHERE product_name_origin like '%����� �����%' and  param10_num1='24' and gramms is null
		UPDATE step4		SET gramms =16		WHERE product_id in ('4549','4568','4567')  -- %3�1 
		UPDATE step4		SET gramms =18		WHERE product_id ='4548' -- %3�1 
		UPDATE step4		SET gramms =20		WHERE product_id ='4550' -- %3�1 

		ALTER TABLE step4 DROP COLUMN try_convert_coef
		ALTER TABLE step4 DROP COLUMN try_convert_valueg
		ALTER TABLE step4 DROP COLUMN coef
		ALTER TABLE step4 DROP COLUMN valueg
		ALTER TABLE step4 DROP COLUMN param3_prepared
		ALTER TABLE step4 DROP COLUMN param3G_prepared
		ALTER TABLE step4 DROP COLUMN try_convert_coefML
		ALTER TABLE step4 DROP COLUMN try_convert_valueML
		ALTER TABLE step4 DROP COLUMN coefML
		ALTER TABLE step4 DROP COLUMN valueML
		ALTER TABLE step4 DROP COLUMN param4L_prepared
		ALTER TABLE step4 DROP COLUMN param4ML_prepared
		ALTER TABLE step4 DROP COLUMN param4S_prepared
		ALTER TABLE step4 DROP COLUMN valuePercent
		ALTER TABLE step4 DROP COLUMN try_convert_valuePercent
		ALTER TABLE step4 DROP COLUMN valueClassNum
		ALTER TABLE step4 DROP COLUMN try_convert_valueClassNum

---���������:
---1." ���� ""��� �������� ���"" ���.9-11percent�, ��������
/*  */
--select * from step0 where parse_status =0 --1562

	END
GO

DROP PROCEDURE IF EXISTS ALTER_PRODUCTS
GO
CREATE PROCEDURE ALTER_PRODUCTS
	AS
	BEGIN
		ALTER TABLE products ADD		gramms						float		NULL , 
										 mls						float		NULL , 
										 peacemeal					varchar(32) NULL , 
										 product_type				varchar(32) NULL , 
										 product_subtype			varchar(32) NULL , 
										 product_category			varchar(32) NULL , 
										 percents					float		NULL , 
										 classnum					float		NULL , 
										 tara						varchar(32) NULL , 
										 producer					varchar(60) NULL ;
		
	END
GO
/*
ADD_PARAMETERS_TO_PRODUCTS
��������� ��������� ����� ������������ ���������� � ������� products
����� ����������� �� ���� ���������� �������
*/
DROP PROCEDURE IF EXISTS ADD_PARAMETERS_TO_PRODUCTS
GO
CREATE PROCEDURE ADD_PARAMETERS_TO_PRODUCTS
	AS
	BEGIN
				
		UPDATE products
		SET 
			gramms	= (select gramms from step4 where step4.product_id=products.product_id),
			mls		= (select mls from step4 where step4.product_id=products.product_id),
			percents= (select percents from step4 where step4.product_id=products.product_id),
			classnum= (select classnum from step4 where step4.product_id=products.product_id),
			tara	= (select param6_tara from step4 where step4.product_id=products.product_id),
			producer= (select param15_producer from step4 where step4.product_id=products.product_id),
			product_type = (select 	CASE 
										WHEN param16_type = '��������' THEN '��������'
										ELSE param16_type 
									END as product_type 
							from step4 
							where step4.product_id=products.product_id),
			product_subtype = (select 	CASE 
											WHEN param16_type = '��������' THEN '��������'
											ELSE param17_subtype 
										END as product_subtype 
							from step4 
							where step4.product_id=products.product_id),
			product_category = (select 
									CASE 
										WHEN  param16_type = '��������' THEN param17_subtype
										ELSE  param18_category
									END as 	product_category 
								from step4 
								where step4.product_id=products.product_id),
			peacemeal = (select CASE 
									WHEN param1_pricefor	= '���� �� ��'	THEN '������'
									WHEN param1_ves			like  '%���%'	THEN '������'
									WHEN param1_pricefor	= '���� �� �'	THEN '������'
								END as peacemeal 
							from step4 
							where step4.product_id=products.product_id)

	END
GO


/*
CALCULATE_STATISTICS  (product_id+store_id)
��������� ������������ ���������� (P)�� ���������� ������ (product_id+store_id) � (S)�� ��������� ������� (product_subtype+store_id):
1. ���������� ��������� ����: 
	1.1.������� ���������� ��������� ���� � ��� �� �������� �� ��� ������� ������ (abs_dec) 
	1.2.������� ������������� ��������� ���� � % �� �������� �� ��� ������� ������ (rel_dev) 
	(�����������: ���������� ��������� �� price1)
2.������� ������� ������  (history_depth)
3.��������� ������ (������� ������ ������ ��������� � ������ �������� � ����) 
					(s_count. � ������� sales_statistics - � ������� ���������, 
					� ������� subtype_statistics_bydate - � ������� subtype  ) 
	3.b - ������� ���������� ������\subtype � ������ �������� � ���� (avg_s_count)
4.�������� ������ 
	4.1.������� �� ������� ������� ����� ������ ���� ������� (������� ��������) (part_of_proceeds) PS
	4.2.������� ������ �� ������ ������ ���� ������� (�� ������� ��������)	(part_of_sales) PS

5. ������� ��������� ���� ��� ������� ������. (����� ���������� ���)
	(count_unique_fact_price)	- � ������ �������� ������ ���� �� ���������� ����������� ������� 
										� ������������� s_count

��������� 4 �������:
sales_statistics  
product_id_statistics  
product_subtype_statistics
product_type_statistics
-- (�������) subtype_statistics_bydate
*/
DROP PROCEDURE IF EXISTS CALCULATE_STATISTICS
GO
CREATE PROCEDURE CALCULATE_STATISTICS
	AS
	BEGIN
		
		DROP TABLE if exists sales_statistics
		SELECT	sales.*,				
				products.product_type,
				products.product_subtype,
				CASE	WHEN s_count >0 
						THEN s_amount/s_count 
				END as fact_price,
				CASE	WHEN time_series_flag=1 
						THEN price1 - avg_product_store_price 
				END	as abs_dev,	/*absolute deviation*/
				
				CASE	WHEN time_series_flag=1 
						THEN ((price1 - avg_product_store_price)/avg_product_store_price)*100 
				END as rel_dev, /*relative deviation*/

				(	select		sum(s2.s_count) 
					from		sales  s2
					where		sales.store_id=s2.store_id 
					group by	s2.store_id
				) as store_sales,

				(	select		sum(s2.s_amount) 
					from		sales s2
					where		sales.store_id=s2.store_id 
					group by	s2.store_id
				) as store_proceeds
		INTO	sales_statistics
		FROM	sales,products
		where	sales.product_id=products.product_id 
		ORDER BY store_id,product_id,s_date		
		
		DROP TABLE if exists product_id_statistics
		SELECT  sales_statistics.store_id,
				sales_statistics.product_id,
				max(sales_statistics.product_type)							as product_type,
				max(sales_statistics.product_subtype)						as product_subtype,
				avg(abs(sales_statistics.abs_dev))							as avg_abs_dev,
				avg(abs(sales_statistics.rel_dev))							as avg_rel_dev,
				min(sales_statistics.min_date)								as min_date,
				max(sales_statistics.max_date)								as max_date,
				count(sales_statistics.time_series_flag)					as history_depth,
				avg(sales_statistics.s_count)								as avg_s_count,
				avg(store_sales)											as store_sales,
				avg(store_proceeds)											as store_proceeds,
				(sum(sales_statistics.s_count)	/ avg(store_sales))*100		as percent_of_sales,
				(sum(sales_statistics.s_amount)	/ avg(store_proceeds))*100	as percent_of_proceeds,				
				count(distinct sales_statistics.fact_price)					as count_unique_fact_price,
				NULL														as screened_attr
		INTO	product_id_statistics
		FROM	sales_statistics
		GROUP BY sales_statistics.store_id, sales_statistics.product_id

		DROP TABLE if exists product_subtype_statistics
		SELECT  sales_statistics.store_id,
				count(distinct product_id)														as count_of_product,
				max(sales_statistics.product_type)												as product_type,
				sales_statistics.product_subtype,
				avg(abs(sales_statistics.abs_dev))												as avg_abs_dev,
				avg(abs(sales_statistics.rel_dev))												as avg_rel_dev,
				min(sales_statistics.min_date)													as min_date,
				max(sales_statistics.max_date)													as max_date,
				DATEDIFF(day,min(sales_statistics.min_date),max(sales_statistics.max_date))		as history_depth,
				avg(sales_statistics.s_count)													as avg_s_count,
				avg(store_sales)																as store_sales,
				avg(store_proceeds)																as store_proceeds,
				(sum(sales_statistics.s_count)	/ avg(store_sales))*100							as percent_of_sales,
				(sum(sales_statistics.s_amount)	/ avg(store_proceeds))*100						as percent_of_proceeds,				
				count(distinct sales_statistics.fact_price)										as count_unique_fact_price,
				NULL																			as screened_attr
		INTO	product_subtype_statistics
		FROM	sales_statistics
		GROUP BY sales_statistics.store_id, sales_statistics.product_subtype


		DROP TABLE if exists product_type_statistics
		SELECT  sales_statistics.store_id,
				count(distinct product_id)														as count_of_product,
				sales_statistics.product_type													as product_type,
				avg(abs(sales_statistics.abs_dev))												as avg_abs_dev,
				avg(abs(sales_statistics.rel_dev))												as avg_rel_dev,
				min(sales_statistics.min_date)													as min_date,
				max(sales_statistics.max_date)													as max_date,
				DATEDIFF(day,min(sales_statistics.min_date),max(sales_statistics.max_date))		as history_depth,
				avg(sales_statistics.s_count)													as avg_s_count,
				avg(store_sales)																as store_sales,
				avg(store_proceeds)																as store_proceeds,
				(sum(sales_statistics.s_count)	/ avg(store_sales))*100							as percent_of_sales,
				(sum(sales_statistics.s_amount)	/ avg(store_proceeds))*100						as percent_of_proceeds,				
				count(distinct sales_statistics.fact_price)										as count_unique_fact_price,
				NULL																			as screened_attr
		INTO	product_type_statistics
		FROM	sales_statistics
		GROUP BY sales_statistics.store_id, sales_statistics.product_type
	


/*
		DROP TABLE if exists product_subtype_statistics_bydate
		SELECT	s_date,
				store_id,
				max(product_type) as product_type,
				product_subtype,
				sum(s_count) as s_count			

		INTO	product_subtype_statistics_bydate
		FROM	sales_statistics
		GROUP BY store_id, product_subtype, s_date
*/
	END
GO


/*
CREATE_ScreeningParameters
��������� ������� ������� ���������� ��������� dbo.ScreeningParameters, ���� ��������� ������ ����������
*/
DROP PROCEDURE IF EXISTS CREATE_ScreeningParameters
GO
CREATE PROCEDURE CREATE_ScreeningParameters (	
			@min_avg_abs_dev				float = NULL,		@max_avg_abs_dev				float = NULL, 
			@min_avg_rel_dev				float = NULL,		@max_avg_rel_dev				float = NULL, 
			@min_history_depth				float = NULL,		@max_history_depth				float = NULL, 
			@min_avg_s_count				float = NULL,		@max_avg_s_count				float = NULL, 
			@min_percent_of_sales			float = NULL,		@max_percent_of_sales			float = NULL, 
			@min_percent_of_proceeds		float = NULL,		@max_percent_of_proceeds		float = NULL, 
			@min_count_unique_fact_price	float = NULL,		@max_count_unique_fact_price	float = NULL
			)
	AS
	BEGIN

		DROP TABLE if exists ScreeningParameters
		CREATE TABLE ScreeningParameters (		
			column_name		nvarchar(30)	NULL,
			min_value		float			NULL,
			max_value		float			NULL,
			parameter_descr	nvarchar(200)	NULL
		);
		
		INSERT INTO ScreeningParameters (column_name,min_value,max_value,parameter_descr) VALUES 
		('avg_abs_dev',				@min_avg_abs_dev,				@max_avg_abs_dev ,				'C������ ���������� ��������� ���� � ��� �� �������� �� ��� ������� ������'),
		('avg_rel_dev',				@min_avg_rel_dev,				@max_avg_rel_dev ,				'C������ ������������� ��������� ���� � % �� �������� �� ��� ������� ������'),
		('history_depth',			@min_history_depth,				@max_history_depth ,			'������� ������� ������'),
		('avg_s_count',				@min_avg_s_count,				@max_avg_s_count ,				'��������� ������ (������� ���-�� � ���� � ������ ��������)'),
		('percent_of_sales',		@min_percent_of_sales,			@max_percent_of_sales ,			'��������, ������� �� ������� ������� ������ ��������'),
		('percent_of_proceeds',		@min_percent_of_proceeds,		@max_percent_of_proceeds ,		'��������, ������� �� ������ ������ ��������)')	,	
		('count_unique_fact_price', @min_count_unique_fact_price,	@max_count_unique_fact_price ,	'������� ��������� ���')

	END
GO


/*
CREATE_ScreeningParameters
� ��������� ��������� ������������ ������ ��� ������������ �������� screened_attr = 1, ��� �������, 
��������������� �������� �� ������� ScreeningParameters � �������, ��������� � ��������� @screened_table

����� ��������� ������� 'screened_' + @screened_table  (-_statistics) ������ � ��������, ��� ������� screened_attr = 1
	��������, ���� screened_table = 'product_id_statistics', ��
	��������� ������� screened_product_id

����� ��������� ������� 'screened_' + @screened_table  - '_statistics)' + '_agg ' 
c ������������� ������������ ������������ ��������� � �������� 
	��������, ���� screened_table = 'product_id_statistics', ��
	��������� ������� screened_product_id_agg
*/
DROP PROCEDURE IF EXISTS ScreenProducts 
GO
CREATE PROCEDURE ScreenProducts (@screened_table nvarchar(50) = 'product_id_statistics')
	AS
	BEGIN		
	---DECLARE @screened_table nvarchar(50)
	---set @screened_table= 'product_id_statistics'
		
		DECLARE @screen_set_sql_0 nvarchar(max)
		set  @screen_set_sql_0 = 'UPDATE	' + @screened_table + '
								SET		screened_attr = NULL'
		EXEC (@screen_set_sql_0 )

		DECLARE @screen_set_sql nvarchar(max)
		set  @screen_set_sql = 'UPDATE	' + @screened_table + '
								SET		screened_attr = 1
								WHERE 1=1 '

		DROP TABLE IF EXISTS temp_param_table
		SELECT column_name, min_value, max_value, row_number() OVER (ORDER BY column_name) as  num_param
		INTO temp_param_table
		FROM	ScreeningParameters 
		WHERE	max_value is not NULL 
				or	min_value is not null

		DECLARE @count_screen_param int
		SET  @count_screen_param = (	SELECT count(*)	FROM temp_param_table 
										WHERE	max_value is not NULL or	min_value is not null	)		

		WHILE @count_screen_param > 0
			BEGIN
				IF ((SELECT min_value from temp_param_table where num_param = @count_screen_param) is not NULL
					and (SELECT max_value from temp_param_table where num_param = @count_screen_param) is not NULL)
					BEGIN
						SET  @screen_set_sql = @screen_set_sql + ' AND ( <VAR1> >= <VAR2> AND <VAR1> <= <VAR3> )' 
						SET  @screen_set_sql = replace(@screen_set_sql, '<VAR1>', (SELECT column_name from temp_param_table where num_param = @count_screen_param))
						SET  @screen_set_sql = replace(@screen_set_sql, '<VAR2>', (SELECT min_value from temp_param_table where num_param = @count_screen_param))
						SET  @screen_set_sql = replace(@screen_set_sql, '<VAR3>', (SELECT max_value from temp_param_table where num_param = @count_screen_param))
					END				
				ELSE IF ((SELECT min_value from temp_param_table where num_param = @count_screen_param) is not NULL)
					BEGIN
						SET  @screen_set_sql = @screen_set_sql + ' AND ( <VAR1> >= <VAR2> )' 
						SET  @screen_set_sql = replace(@screen_set_sql, '<VAR1>', (SELECT column_name from temp_param_table where num_param = @count_screen_param))
						SET  @screen_set_sql = replace(@screen_set_sql, '<VAR2>', (SELECT min_value from temp_param_table where num_param = @count_screen_param))
					END
				ELSE 
					BEGIN
						SET  @screen_set_sql = @screen_set_sql + ' AND (<VAR1> <= <VAR3> )'
						SET  @screen_set_sql = replace(@screen_set_sql, '<VAR1>', (SELECT column_name from temp_param_table where num_param = @count_screen_param))
						SET  @screen_set_sql = replace(@screen_set_sql, '<VAR3>', (SELECT max_value from temp_param_table where num_param = @count_screen_param))
					END
				SET @count_screen_param = @count_screen_param -1
			END		
		--select @screen_set_sql
		EXEC(@screen_set_sql)

		DECLARE @table_name2 nvarchar(50)
		DECLARE @table_name3 nvarchar(50)
		set @table_name2= 'screened_' + @screened_table 
		set @table_name2= replace(@table_name2,'_statistics','')
		set @table_name3= @table_name2 + '_agg'

		DECLARE @screening_drop	nvarchar(max)
		DECLARE @screening_sql	nvarchar(max)
		
		SET  @screening_drop =	'DROP  TABLE IF EXISTS	' + @table_name2 
		SET  @screening_sql =	'SELECT * ' +
								' INTO  ' + @table_name2 + 
								' FROM  ' + @screened_table +
								' WHERE screened_attr = 1'

		EXEC(@screening_drop)
		EXEC(@screening_sql)	

		DECLARE @comulative_drop	nvarchar(max)
		DECLARE @comulative_sql		nvarchar(max)
		
		SET  @comulative_drop =	'DROP  TABLE IF EXISTS	' + @table_name3 
		SET  @comulative_sql =	'SELECT		[store_id]								
											,sum([avg_s_count])				as avg_s_count				
											,sum([percent_of_sales])		as percent_of_sales
											,sum([percent_of_proceeds])		as percent_of_proceeds' +
								' INTO  ' + @table_name3 + 
								' FROM  ' + @table_name2 +
								' GROUP BY store_id'

		EXEC(@comulative_drop)
		EXEC(@comulative_sql)			

	END
GO

/* 
PRICE_CONVERTING
��������� ���� �� s_date+store_id+product_id,  ����������� � ������ ������/����
��������� ������a sales_prepared (�� ���� ������� sales_statistics)
� sales_prepared �������� ������ ������ �� ���������, ��������� �������, 
������ ������ ������ ���������� ��������� ��� ������� + ������� 
*/
DROP PROCEDURE IF EXISTS PRICE_CONVERTING 
GO
CREATE PROCEDURE PRICE_CONVERTING (@gramms int = 1000, @mls int = 1000)
	AS
	BEGIN		
		DROP TABLE IF EXISTS sales_prepared
		SELECT	ss.s_date
				,ss.store_id
				,ss.product_id  
				,ss.[s_amount]
				,ss.[s_count]
				,ss.[price1]										
		
				,p.gramms					
				,p.mls
			--	,p.peacemeal

				,CASE	
					WHEN ss.product_type = '������'					THEN ss.price1
					WHEN p.gramms	is not null and p.gramms !=0	THEN (ss.price1/p.gramms)*@gramms
					WHEN p.mls		is not null and p.mls	 !=0	THEN (ss.price1/p.mls)*@mls
					WHEN p.peacemeal	is not null					THEN ss.price1
					WHEN ss.product_subtype in ('����')				THEN ss.price1
					WHEN ss.product_id in (8971,3679)				THEN ss.price1
				END as price1_converted

			--	,ss.[price2]
			--	,ss.[price3]
			--	,ss.[auto_sign]
			--	,ss.[fact_price]
			--	,ss.[avg_product_store_price]
			--	,ss.[time_series_flag]
     
			--	,ss.[abs_dev]
			--	,ss.[rel_dev]
			--	,ss.[min_date]
			--	,ss.[max_date]
				,ss.[product_type]
				,ss.[product_subtype]				
				,p.product_category
				,p.product_name
				,p.percents
				,p.classnum
				,p.tara

				--,sp.screened_attr,
				,sp.[avg_abs_dev]
				,sp.[avg_rel_dev]
				--,sp.[min_date]
				--,sp.[max_date]
				,sp.[history_depth]
				,sp.[avg_s_count]
				,sp.[percent_of_sales]
				,sp.[percent_of_proceeds]
				,sp.[count_unique_fact_price]
				
				--,p.gramms
				--,p.mls
				--,p.peacemeal
		INTO sales_prepared
		FROM sales_statistics ss, screened_product_id as sp, products as p
		WHERE	ss.product_id = sp.product_id
			and	ss.store_id = sp.store_id
			and sp.screened_attr = 1
			and ss.time_series_flag = 1
			and ss.product_id = p.product_id


		DROP INDEX IF EXISTS IX_sales_prepared_PK				ON sales_prepared
		DROP INDEX IF EXISTS IX_sales_prepared_product_type		ON sales_prepared
		DROP INDEX IF EXISTS IX_sales_prepared_product_subtype  ON sales_prepared
		DROP INDEX IF EXISTS IX_sales_prepared_product_category ON sales_prepared
		DROP INDEX IF EXISTS IX_sales_prepared_store_id			ON sales_prepared
		DROP INDEX IF EXISTS IX_sales_prepared_percents			ON sales_prepared
		DROP INDEX IF EXISTS IX_sales_prepared_tara				ON sales_prepared
		DROP INDEX IF EXISTS IX_sales_prepared_gramms			ON sales_prepared
		DROP INDEX IF EXISTS IX_sales_prepared_mls				ON sales_prepared
		DROP INDEX IF EXISTS IX_sales_prepared_history_depth	ON sales_prepared

		CREATE UNIQUE CLUSTERED INDEX IX_sales_prepared_PK					ON sales_prepared ([s_date],[store_id],[product_id])
		CREATE NONCLUSTERED		INDEX IX_sales_prepared_product_type		ON sales_prepared ([product_type])
		CREATE NONCLUSTERED		INDEX IX_sales_prepared_product_subtype		ON sales_prepared ([product_subtype])  
		CREATE NONCLUSTERED		INDEX IX_sales_prepared_product_category	ON sales_prepared ([product_category]) 
		CREATE NONCLUSTERED		INDEX IX_sales_prepared_store_id			ON sales_prepared (store_id)   
		CREATE NONCLUSTERED		INDEX IX_sales_prepared_percents			ON sales_prepared (percents)
		CREATE NONCLUSTERED		INDEX IX_sales_prepared_tara				ON sales_prepared (tara)  
		CREATE NONCLUSTERED		INDEX IX_sales_prepared_gramms				ON sales_prepared (gramms) 
		CREATE NONCLUSTERED		INDEX IX_sales_prepared_mls					ON sales_prepared (mls)  
		CREATE NONCLUSTERED		INDEX IX_sales_prepared_history_depth		ON sales_prepared ([history_depth])  
		/* ������� ������ �������, ���� ����� ������������� ���������� ����� �������� ��������� */

	END
GO

/* ���� ����� �������� ��������� ������� �� �� ���� ������, � ������ �� �����, �� ����� ����� ��������� �������
��������� SALES_PREPARED_RESTRICT - ������������ ������ ������� sales_prepared, �� ������� product_subtype=@product_subtype
�������� @product_subtype
���� @product_subtype = None, �� ������� �� ����������,
����� � ������� �������� ������ ������ � �������� WHERE product_subtype='�������� ��������'

��������� �������� � ������� ������ ���� ��������  product_subtype.
�������, ����� ����������� �� ������� ������ ��� �� ������.
������������, ���� � �������� ����� ����� ������� �� �������, �� ������ ����, � ������� ���� ��������� ��������� ��� ��������� ��� ������ ������

*/
DROP PROCEDURE IF EXISTS SALES_PREPARED_RESTRICT
GO
CREATE PROCEDURE SALES_PREPARED_RESTRICT (@product_subtype varchar(32) = NULL)
	AS
	BEGIN	
		IF @product_subtype is not NULL
			BEGIN 
				DELETE FROM sales_prepared WHERE product_subtype != @product_subtype
			END
	END
GO


/* 
CREATE_GroupParameters
������� � ��������� ������� GroupParameters
*/

/* CREATE_GroupParameters
1. ��������� ���������� ������� AttributeToContect, � ������� ���������� ���� �� products

������� AttributeToContect ����� �������� �����
-��������,��� ������� ��������� � �������� ����������� �����, 
	������� ������� �������� � �������� ����������� (=1, ���� �������� ��������� � �����������)
	������� ����������������\�������������� ��������
	������� , � ������� ������ �������� ��� ���������
����� ��� ������� join'���� �� product_id

����� � AttributeToContect ����������� ���� �� ������������

2. ��������� ������� � ��������� ������� GroupParameters � ��������� ��������� �������� � ������ (�� ���� sales_prepared)
� GroupParameters_unique (���� � ��������� ���������)

*/

DROP PROCEDURE IF EXISTS CREATE_GroupParameters
GO
CREATE PROCEDURE CREATE_GroupParameters 
	AS
	BEGIN

		DROP TABLE IF EXISTS AttributeToContect
		SELECT	TABLE_NAME AS tab_name,
				COLUMN_NAME AS column_name,
				DATA_TYPE AS data_type,
				CASE WHEN data_type  like '%CHAR%'  THEN 1 END as is_categorical, 
				CASE 
					WHEN column_name in ('gramms')					THEN ' >= ' 
					WHEN column_name in ('mls')						THEN ' >= '  
					WHEN column_name in ('percents')				THEN ' >= '

				END as operator1,
				CASE 
					WHEN column_name in ('product_type')	THEN ' = '
					WHEN column_name in ('product_subtype') THEN ' = ' 
					WHEN column_name in ('product_category')THEN ' = '
					WHEN column_name in ('gramms')			THEN ' < ' 
					WHEN column_name in ('mls')				THEN ' < '  
					WHEN column_name in ('tara')			THEN ' = ' 
					WHEN column_name in ('percents')		THEN ' < '
				--	WHEN column_name in ('classnum')		THEN ' = '
				END as operator2,
				CASE 					
					WHEN column_name in ('gramms')			THEN ' 1 ' 
					WHEN column_name in ('mls')				THEN ' 2 '  
					WHEN column_name in ('percents')		THEN ' 3 '
				--	WHEN column_name in ('classnum')		THEN ' 2 '
				END as rangs_num,
				CASE 
					WHEN column_name in ('product_type')	THEN 1
					WHEN column_name in ('product_subtype') THEN 2 
					WHEN column_name in ('product_category')THEN 3
					WHEN column_name in ('gramms')			THEN 4 
					WHEN column_name in ('mls')				THEN 4  
					WHEN column_name in ('tara')			THEN 4 
					WHEN column_name in ('percents')		THEN 4
				--	WHEN column_name in ('classnum')		THEN 4
				END as attr_priority,
				row_number() OVER (ORDER BY CASE 
					
					WHEN column_name in ('product_type')	THEN 1
					WHEN column_name in ('product_subtype') THEN 2 
					WHEN column_name in ('product_category')THEN 3
					WHEN column_name in ('gramms')			THEN 3 
					WHEN column_name in ('mls')				THEN 3  
					WHEN column_name in ('tara')			THEN 3 
					WHEN column_name in ('percents')		THEN 3
				--	WHEN column_name in ('classnum')		THEN 3
				END )	AS attr_num
		into AttributeToContect
		FROM INFORMATION_SCHEMA.COLUMNS
		WHERE table_name='products'
		and column_name in ('product_type','product_subtype'
							,'product_category'
							,'gramms','mls','tara','percents'
							--,'classnum'
							)		  


		INSERT INTO AttributeToContect values
	--	('sales_prepared', 'avg_abs_dev',				'float', NULL, ' >= ' , ' < ',' 2 ', 4, 9 ),
	--	('sales_prepared', 'avg_rel_dev',				'float', NULL, ' >= ' , ' < ',' 2 ', 4, 10 ),
		('sales_prepared', 'history_depth',				'float', NULL, ' >= ' , ' < ',' 2 ', 4, 8 )
		--,
	--	('sales_prepared', 'avg_s_count',				'float', NULL, ' >= ' , ' < ',' 2 ', 4, 11 ),
	--	('sales_prepared', 'percent_of_sales',			'float', NULL, ' >= ' , ' < ',' 2 ', 4, 12 ),
	--	('sales_prepared', 'percent_of_proceeds',		'float', NULL, ' >= ' , ' < ',' 2 ', 4, 13 ),
	--	('sales_prepared', 'count_unique_fact_price',	'float', NULL, ' >= ' , ' < ',' 2 ', 4, 14 )
		

		DECLARE @sql_create_GroupParameters nvarchar(max)
		SET @sql_create_GroupParameters = 
			'	DROP TABLE if exists GroupParameters
				CREATE TABLE GroupParameters (
					group_level				int				NOT NULL
					,product_group			varchar(20)		NOT NULL	
					,store_id				int					NULL	
			'
	
		DECLARE @attr_count int = (select max(attr_num) from AttributeToContect)
		DECLARE @i int  = (select min(attr_num) from AttributeToContect)
		WHILE @i <= @attr_count
			BEGIN
				IF  ((select is_categorical from AttributeToContect where attr_num = @i) is not NULL) 
					BEGIN
						SET @sql_create_GroupParameters = @sql_create_GroupParameters	+ ',	' + (select column_name from AttributeToContect where attr_num = @i) 
																						+ '	' + replace((select data_type from AttributeToContect where attr_num = @i),'varchar','varchar(32)')
																						+ ' NULL ' 
					END
				ELSE 
					BEGIN
						SET @sql_create_GroupParameters = @sql_create_GroupParameters	+ ',	' + (select column_name from AttributeToContect where attr_num = @i) + '_min' + 
																						+ '	' + (select data_type from AttributeToContect where attr_num = @i)
																						+ ' NULL ' 
						SET @sql_create_GroupParameters = @sql_create_GroupParameters	+ ',	' + (select column_name from AttributeToContect where attr_num = @i) + '_max' + 
																						+ '	' + (select data_type from AttributeToContect where attr_num = @i)
																						+ ' NULL ' 
					END
				SET @i = @i + 1
			END
		SET @sql_create_GroupParameters = @sql_create_GroupParameters	+ ' )'
		select (@sql_create_GroupParameters)
		EXEC(@sql_create_GroupParameters)


		DROP INDEX IF EXISTS IX_GroupParameters_PK					ON GroupParameters
		DROP INDEX IF EXISTS IX_GroupParameters_product_type		ON GroupParameters
		DROP INDEX IF EXISTS IX_GroupParameters_product_subtype		ON GroupParameters
		DROP INDEX IF EXISTS IX_GroupParameters_product_category	ON GroupParameters
		DROP INDEX IF EXISTS IX_GroupParameters_store_id			ON GroupParameters
		DROP INDEX IF EXISTS IX_GroupParameters_percents_min		ON GroupParameters
		DROP INDEX IF EXISTS IX_GroupParameters_percents_max		ON GroupParameters
		DROP INDEX IF EXISTS IX_GroupParameters_tara				ON GroupParameters
		DROP INDEX IF EXISTS IX_GroupParameters_gramms_min			ON GroupParameters
		DROP INDEX IF EXISTS IX_GroupParameters_gramms_max			ON GroupParameters
		DROP INDEX IF EXISTS IX_GroupParameters_mls_min				ON GroupParameters
		DROP INDEX IF EXISTS IX_GroupParameters_mls_max				ON GroupParameters
		DROP INDEX IF EXISTS IX_GroupParameters_history_depth_min	ON GroupParameters
		DROP INDEX IF EXISTS IX_GroupParameters_history_depth_max	ON GroupParameters

		CREATE UNIQUE CLUSTERED INDEX IX_GroupParameters_PK					ON GroupParameters (group_level,product_group,store_id,product_type,[product_subtype],[product_category])
		CREATE NONCLUSTERED		INDEX IX_GroupParameters_product_type		ON GroupParameters ([product_type])
		CREATE NONCLUSTERED		INDEX IX_GroupParameters_product_subtype	ON GroupParameters ([product_subtype])  
		CREATE NONCLUSTERED		INDEX IX_GroupParameters_product_category	ON GroupParameters ([product_category]) 
		CREATE NONCLUSTERED		INDEX IX_GroupParameters_store_id			ON GroupParameters (store_id)   
		CREATE NONCLUSTERED		INDEX IX_GroupParameters_percents_min		ON GroupParameters (percents_min)
		CREATE NONCLUSTERED		INDEX IX_GroupParameters_percents_max		ON GroupParameters (percents_max)
		CREATE NONCLUSTERED		INDEX IX_GroupParameters_tara				ON GroupParameters (tara)  
		CREATE NONCLUSTERED		INDEX IX_GroupParameters_gramms_min			ON GroupParameters (gramms_min) 
		CREATE NONCLUSTERED		INDEX IX_GroupParameters_gramms_max			ON GroupParameters (gramms_max) 
		CREATE NONCLUSTERED		INDEX IX_GroupParameters_mls_min			ON GroupParameters (mls_min)  
		CREATE NONCLUSTERED		INDEX IX_GroupParameters_mls_max			ON GroupParameters (mls_max)  
		CREATE NONCLUSTERED		INDEX IX_GroupParameters_history_depth_min	ON GroupParameters ([history_depth_min])  
		CREATE NONCLUSTERED		INDEX IX_GroupParameters_history_depth_max	ON GroupParameters ([history_depth_max])  



		--������ ��������� ������� ��������� ��������� � ������
		DECLARE @level int =0	
		DECLARE @attribute varchar(32) 
		DECLARE @sql_insert nvarchar(max) 
		DECLARE @sql_select nvarchar(max) 
		DECLARE @rangs_count int
		DECLARE @j int = 1


		--attr_priority=1 product_name
		SET @attribute= (select column_name from AttributeToContect where attr_priority = 1)
		SET @sql_insert = '   	INSERT INTO GroupParameters (group_level,product_group, '+@attribute+') '
		SET @sql_select = ' 				(SELECT  DISTINCT	'+trim(cast(@level as char))+' , 
												concat('+cast(@level as char)+',''_'',row_number() OVER (ORDER BY '+@attribute+')), 
												'+@attribute+'  
							FROM sales_prepared 
							GROUP BY '+@attribute+')'							
		select(@sql_insert+@sql_select)
		EXEC(@sql_insert+@sql_select)
		SET	@level = @level +1

		--attr_priority=2 product_name+product_subtype
		SET @attribute= (select column_name from AttributeToContect where attr_priority = 2)
		SET @sql_insert = '   	INSERT INTO GroupParameters (group_level,product_group,product_type,'+@attribute+') '
		SET @sql_select = ' 				(SELECT  DISTINCT	'+trim(cast(@level as char))+' , 
												concat('+trim(cast(@level as char))+',''_'',row_number() OVER (ORDER BY '+@attribute+')), 
												product_type,
												'+@attribute+'  
							FROM sales_prepared 
							GROUP BY product_type, '+@attribute+')'
		select(@sql_insert+@sql_select)
		EXEC(@sql_insert+@sql_select)
		SET	@level = @level +1

		--attr_priority=3 product_name+product_subtype+product_category
		SET @attribute= (select column_name from AttributeToContect where attr_priority = 3)
		SET @sql_insert = '   	INSERT INTO GroupParameters (group_level,product_group,product_type,product_subtype,'+@attribute+') '
		SET @sql_select = ' 				(SELECT  DISTINCT	'+trim(cast(@level as char))+' , 
												concat('+trim(cast(@level as char))+',''_'',row_number() OVER (ORDER BY '+@attribute+')), 
												product_type,product_subtype,
												'+@attribute+'  
							FROM sales_prepared 
							GROUP BY product_type, product_subtype,'+@attribute+')'
		select(@sql_insert+@sql_select)
		EXEC(@sql_insert+@sql_select)
		SET	@level = @level +1


		--attr_priority=4 product_type + product_subtype + product_categoty + @attribute
		SET @i = (select min(attr_num) from AttributeToContect where attr_priority not in (1,2,3))
		WHILE @i <= @attr_count
			BEGIN
				SET  @attribute = (select column_name from AttributeToContect where attr_num = @i)
				IF  (((select is_categorical from AttributeToContect where attr_num = @i) is not NULL)
					and (@attribute  not in ('product_type','product_subtype','product_category' )))
					BEGIN
						select 'cat1'	
						SET @sql_insert = '   	INSERT INTO GroupParameters (group_level,product_group,product_type,product_subtype,product_category,'+@attribute+') '
						SET @sql_select = ' 	  
							SELECT  DISTINCT	'+cast(@level as char)+' , 
												concat('+cast(@level as char)+',''_'',
																			(select row_num from (select product_type, product_subtype, product_category, row_number() OVER (ORDER BY products.product_type, products.product_subtype, products.product_category) as row_num	from products 	where products.product_type != '''' and products.product_subtype != '''' and products.product_category != '''' group by  products.product_type, products.product_subtype, products.product_category) as t2 
																			where t2.product_type=sales_prepared.product_type and t2.product_subtype=sales_prepared.product_subtype and t2.product_category=sales_prepared.product_category)
																		,''_'',
																			(select row_num from (select products.'+@attribute+', row_number() OVER (ORDER BY products.'+@attribute+') as row_num	from products 	group by  products.'+@attribute+') as t2 
																			where t2.'+@attribute+'=sales_prepared.'+@attribute+')
																		), 
												product_type,
												product_subtype,
												product_category,
												'+@attribute+'  
							FROM sales_prepared 
							GROUP BY product_type,product_subtype,product_category, '+@attribute							
						SELECT(@sql_insert+@sql_select)
						EXEC(@sql_insert+@sql_select)
						SET @level = @level+1	
					END
				ELSE 
					BEGIN
						
						select 'notcat2_1'
						SET  @rangs_count = (select rangs_num from AttributeToContect where attr_num = @i)
						SET  @j=1
						select @rangs_count
						WHILE @j <= @rangs_count
							BEGIN	
								select 'notcat2_1_1'
								SET @sql_insert = '   	INSERT INTO GroupParameters (group_level,product_group,store_id, product_type,'+@attribute+'_min,'+@attribute+'_max) '
								SET @sql_select = ' 	 SELECT DISTINCT                
																'+cast(@level as char)+' , 
																concat('+cast(@level as char)+',''_'',
																			(select row_num from (select product_type, row_number() OVER (ORDER BY products.product_type) as row_num	from products 	where product_type != '''' group by  products.product_type) as t2 
																			where t2.product_type=sales_prepared.product_type)
																		,''_'','+cast(@j as char)+',''(s'', store_id ,'')'' ), 
																store_id ,
																product_type,             
																min('+@attribute+') + ((max('+@attribute+') - min('+@attribute+'))/'+cast(@rangs_count as char)+')*('+cast(@j-1 as char)+')  as '+@attribute+'_min,          
																min('+@attribute+') + ((max('+@attribute+') - min('+@attribute+'))/'+cast(@rangs_count as char)+')*('+cast(@j as char)+')	as '+@attribute+'_max

															  FROM sales_prepared                 
															  WHERE  '+@attribute+' is not null             
															  GROUP BY product_type, store_id
															  order by product_type, store_id'
														
								SELECT(@sql_insert+@sql_select)
								EXEC(@sql_insert+@sql_select)
								SET @j = @j + 1			
	 						END
						SET @level = @level+1
						SET @j = 1
						WHILE @j <= @rangs_count
							BEGIN
							 	select 'notcat2_1_2'
								SET @sql_insert = '   	INSERT INTO GroupParameters (group_level,product_group,store_id,product_type,product_subtype,'+@attribute+'_min,'+@attribute+'_max) '
								SET @sql_select = ' 
															SELECT DISTINCT
																'+cast(@level as char)+' , 
																concat('+cast(@level as char)+',''_'',
																			(select row_num from (select product_type, product_subtype, row_number() OVER (ORDER BY products.product_type,products.product_subtype) as row_num	from products 	where product_type != '''' and product_subtype != '''' group by  products.product_type,products.product_subtype) as t2 
																			where t2.product_type=sales_prepared.product_type and  t2.product_subtype=sales_prepared.product_subtype)
																		,''_'','+cast(@j as char)+',''(s'',  store_id  ,'')'' ), 																		
																store_id,
																product_type,
																product_subtype,            
																min('+@attribute+') + ((max('+@attribute+') - min('+@attribute+'))/'+cast(@rangs_count as char)+')*('+cast(@j-1 as char)+')  as '+@attribute+'_min,          
																min('+@attribute+') + ((max('+@attribute+') - min('+@attribute+'))/'+cast(@rangs_count as char)+')*('+cast(@j as char)+')	as '+@attribute+'_max
															FROM sales_prepared 
															WHERE  '+@attribute+' is not null
															GROUP BY product_type, product_subtype, store_id
															order by product_type, product_subtype, store_id'
								SELECT(@sql_insert+@sql_select)								
								EXEC(@sql_insert+@sql_select)
								SET @j = @j + 1								
							END
						SET @level = @level+1
						SET @j = 1
						WHILE @j <= @rangs_count
							BEGIN
							 	select 'notcat2_1_3'
								SET @sql_insert = '   	INSERT INTO GroupParameters (group_level,product_group,store_id,product_type,product_subtype,product_category,'+@attribute+'_min,'+@attribute+'_max) '
								SET @sql_select = ' 
															SELECT DISTINCT
																'+cast(@level as char)+' , 
																concat('+cast(@level as char)+',''_'',
																			(select row_num from (select product_type, product_subtype, product_category,  row_number() OVER (ORDER BY products.product_type,products.product_subtype, product_category) as row_num	from products 	where product_type != '''' and product_subtype != '''' and products.product_category != '''' group by  products.product_type,products.product_subtype,products.product_category) as t2 
																			where t2.product_type=sales_prepared.product_type and  t2.product_subtype=sales_prepared.product_subtype and  t2.product_category=sales_prepared.product_category)
																		,''_'','+cast(@j as char)+',''(s'',  store_id  ,'')'' ), 																		
																store_id,
																product_type,
																product_subtype,  
																product_category,
																min('+@attribute+') + ((max('+@attribute+') - min('+@attribute+'))/'+cast(@rangs_count as char)+')*('+cast(@j-1 as char)+')  as '+@attribute+'_min,          
																min('+@attribute+') + ((max('+@attribute+') - min('+@attribute+'))/'+cast(@rangs_count as char)+')*('+cast(@j as char)+')	as '+@attribute+'_max
															FROM sales_prepared 
															WHERE  '+@attribute+' is not null
															GROUP BY product_type, product_subtype, product_category,store_id
															order by product_type, product_subtype, product_category,store_id'
								SELECT(@sql_insert+@sql_select)								
								EXEC(@sql_insert+@sql_select)
								SET @j = @j + 1								
							END
						SET @level = @level+1
					END

				SET @i = @i + 1
			END

			DROP TABLE IF EXISTS GroupParameters_unique
			SELECT distinct [group_level]
								  ,min([product_group]) as product_group
								  ,[store_id]
								  ,[product_type]
								  ,[product_subtype]
								  ,[product_category]
								  ,[percents_min]
								  ,[percents_max]
								  ,[tara]
								  ,[gramms_min]
								  ,[gramms_max]
								  ,[mls_min]
								  ,[mls_max]
								  ,[history_depth_min]
								  ,[history_depth_max]
			INTO GroupParameters_unique
				  FROM [PRICE_FORMATION].[dbo].[GroupParameters]
				  group by [group_level],[store_id],[product_type],[product_subtype],[product_category]
						  ,[percents_min]
						  ,[percents_max]
						  ,[tara]
						  ,[gramms_min]
						  ,[gramms_max]
						  ,[mls_min]
						  ,[mls_max]
						  ,[history_depth_min]
						  ,[history_depth_max]
	END
GO


/* 
PRICE_CONTEXT
������� � ��������� ������� sales_price_context � ������� ����������
*/
DROP PROCEDURE IF EXISTS  PRICE_CONTEXT
GO
CREATE PROCEDURE PRICE_CONTEXT 
	AS
	BEGIN	

		DECLARE @levels_count int
		SET @levels_count= (select max(group_level) from GroupParameters_unique)

		DECLARE @sql_for_apply_group nvarchar(max)
		SET @sql_for_apply_group = 'DROP TABLE 	if EXISTS sales_prepared2
									SELECT DISTINCT'
		
		DECLARE @i int
		SET @i = (select min(group_level) from GroupParameters_unique)
		WHILE @i <= @levels_count
			BEGIN
				SET @sql_for_apply_group = @sql_for_apply_group + ' CASE WHEN gp' + trim(cast(@i as char)) + '.group_level=' + trim(cast(@i as char))  + ' THEN gp' +  trim(cast(@i as char)) + '.product_group END as group_level' + trim(cast(@i as char)) + ', '
				SET @i = @i+1
			END

		SET @sql_for_apply_group = @sql_for_apply_group + '
																	ss.*
																	INTO sales_prepared2
																	FROM		sales_prepared ss '
		SET @i = (select min(group_level) from GroupParameters_unique)

		WHILE @i <= @levels_count
			BEGIN /* ��� ��������� �� ���������� ���������� ���������� */ /*

				SET @sql_for_apply_group = @sql_for_apply_group + '	left JOIN	GroupParameters_unique	gp' + trim(cast(@i as char)) + '
																	ON	gp' + trim(cast(@i as char)) + '.group_level=' + trim(cast(@i as char)) + ' 
																	and (ss.store_id = gp' + trim(cast(@i as char)) + '.store_id or gp' + trim(cast(@i as char)) + '.store_id is  null)
																	and (ss.product_type = gp' + trim(cast(@i as char)) + '.product_type or gp' + trim(cast(@i as char)) + '.product_type is  null)
																	and (ss.product_subtype = gp' + trim(cast(@i as char)) + '.product_subtype or gp' + trim(cast(@i as char)) + '.product_subtype is  null)
																	and (ss.product_category = gp' + trim(cast(@i as char)) + '.product_category or gp' + trim(cast(@i as char)) + '.product_category is  null)
																	and (ss.tara = gp' + trim(cast(@i as char)) + '.tara or gp' + trim(cast(@i as char)) + '.tara is  null)
																	and (ss.gramms	>= gp'	+ trim(cast(@i as char)) + '.gramms_min or gp' + trim(cast(@i as char)) + '.gramms_min is  null)
																	and (ss.gramms	< gp'	+ trim(cast(@i as char)) + '.gramms_max or gp' + trim(cast(@i as char)) + '.gramms_max is  null)
																	and (ss.mls		>= gp'	+ trim(cast(@i as char)) + '.mls_min or gp' + trim(cast(@i as char)) + '.mls_min is  null)
																	and (ss.mls		< gp'	+ trim(cast(@i as char)) + '.mls_max or gp' + trim(cast(@i as char)) + '.mls_max is  null)
																	and (ss.percents	>= gp'	+ trim(cast(@i as char)) + '.percents_min or gp' + trim(cast(@i as char)) + '.percents_min is  null)
																	and (ss.percents	< gp'	+ trim(cast(@i as char)) + '.percents_max or gp' + trim(cast(@i as char)) + '.percents_max is  null)
																	and (ss.avg_abs_dev	>= gp'	+ trim(cast(@i as char)) + '.avg_abs_dev_min or gp' + trim(cast(@i as char)) + '.avg_abs_dev_min is  null)
																	and (ss.avg_abs_dev	< gp'	+ trim(cast(@i as char)) + '.avg_abs_dev_max or gp' + trim(cast(@i as char)) + '.avg_abs_dev_max is  null)
																	and (ss.avg_rel_dev	>= gp'	+ trim(cast(@i as char)) + '.avg_rel_dev_min or gp' + trim(cast(@i as char)) + '.avg_rel_dev_min is  null)
																	and (ss.avg_rel_dev	< gp'	+ trim(cast(@i as char)) + '.avg_rel_dev_max or gp' + trim(cast(@i as char)) + '.avg_rel_dev_max is  null)
																	and (ss.history_depth	>= gp'	+ trim(cast(@i as char)) + '.history_depth_min or gp' + trim(cast(@i as char)) + '.history_depth_min is  null)
																	and (ss.history_depth	< gp'	+ trim(cast(@i as char)) + '.history_depth_max or gp' + trim(cast(@i as char)) + '.history_depth_max is  null)
																	and (ss.avg_s_count	>= gp'	+ trim(cast(@i as char)) + '.avg_s_count_min or gp' + trim(cast(@i as char)) + '.avg_s_count_min is  null)
																	and (ss.avg_s_count	< gp'	+ trim(cast(@i as char)) + '.avg_s_count_max or gp' + trim(cast(@i as char)) + '.avg_s_count_max is  null)
																	and (ss.percent_of_sales	>= gp'	+ trim(cast(@i as char)) + '.percent_of_sales_min or gp' + trim(cast(@i as char)) + '.percent_of_sales_min is  null)
																	and (ss.percent_of_sales	< gp'	+ trim(cast(@i as char)) + '.percent_of_sales_max or gp' + trim(cast(@i as char)) + '.percent_of_sales_max is  null)
																	and (ss.percent_of_proceeds	>= gp'	+ trim(cast(@i as char)) + '.percent_of_proceeds_min or gp' + trim(cast(@i as char)) + '.percent_of_proceeds_min is  null)
																	and (ss.percent_of_proceeds	< gp'	+ trim(cast(@i as char)) + '.percent_of_proceeds_max or gp' + trim(cast(@i as char)) + '.percent_of_proceeds_max is  null)
																	and (ss.count_unique_fact_price	>= gp'	+ trim(cast(@i as char)) + '.count_unique_fact_price_min or gp' + trim(cast(@i as char)) + '.count_unique_fact_price_min is  null)
																	and (ss.count_unique_fact_price	< gp'	+ trim(cast(@i as char)) + '.count_unique_fact_price_max or gp' + trim(cast(@i as char)) + '.count_unique_fact_price_max is  null)
																	'
				*/

				SET @sql_for_apply_group = @sql_for_apply_group + '	left JOIN	GroupParameters_unique	gp' + trim(cast(@i as char)) + '
																	ON	gp' + trim(cast(@i as char)) + '.group_level=' + trim(cast(@i as char)) + ' 
																	and (ss.store_id = gp' + trim(cast(@i as char)) + '.store_id or gp' + trim(cast(@i as char)) + '.store_id is  null)
																	and (ss.product_type = gp' + trim(cast(@i as char)) + '.product_type or gp' + trim(cast(@i as char)) + '.product_type is  null)
																	and (ss.product_subtype = gp' + trim(cast(@i as char)) + '.product_subtype or gp' + trim(cast(@i as char)) + '.product_subtype is  null)
																	and (ss.product_category = gp' + trim(cast(@i as char)) + '.product_category or gp' + trim(cast(@i as char)) + '.product_category is  null)
																	and (ss.tara = gp' + trim(cast(@i as char)) + '.tara or gp' + trim(cast(@i as char)) + '.tara is  null)
																	and (ss.gramms	>= gp'	+ trim(cast(@i as char)) + '.gramms_min or gp' + trim(cast(@i as char)) + '.gramms_min is  null)
																	and (ss.gramms	< gp'	+ trim(cast(@i as char)) + '.gramms_max or gp' + trim(cast(@i as char)) + '.gramms_max is  null or gp' + trim(cast(@i as char)) + '.gramms_max =  gp' + trim(cast(@i as char)) + '.gramms_min)
																	and (ss.mls		>= gp'	+ trim(cast(@i as char)) + '.mls_min or gp' + trim(cast(@i as char)) + '.mls_min is  null)
																	and (ss.mls		< gp'	+ trim(cast(@i as char)) + '.mls_max or gp' + trim(cast(@i as char)) + '.mls_max is  null or gp' + trim(cast(@i as char)) + '.mls_max =  gp' + trim(cast(@i as char)) + '.mls_min)
																	and (ss.percents	>= gp'	+ trim(cast(@i as char)) + '.percents_min or gp' + trim(cast(@i as char)) + '.percents_min is  null)
																	and (ss.percents	< gp'	+ trim(cast(@i as char)) + '.percents_max or gp' + trim(cast(@i as char)) + '.percents_max is  null or gp' + trim(cast(@i as char)) + '.percents_max =  gp' + trim(cast(@i as char)) + '.percents_min)
																	and (ss.history_depth	>= gp'	+ trim(cast(@i as char)) + '.history_depth_min or gp' + trim(cast(@i as char)) + '.history_depth_min is  null)
																	and (ss.history_depth	< gp'	+ trim(cast(@i as char)) + '.history_depth_max or gp' + trim(cast(@i as char)) + '.history_depth_max is  null or gp' + trim(cast(@i as char)) + '.history_depth_max =  gp' + trim(cast(@i as char)) + '.history_depth_min)
																	'

				SET @i = @i+1
			END
		select(@sql_for_apply_group)	
		EXEC(@sql_for_apply_group)


		DECLARE @sql_for_price_calc nvarchar(max)
		SET @sql_for_price_calc = 'DROP TABLE if EXISTS sales_price_context
									SELECT  '		
		SET @i = (select min(group_level) from GroupParameters_unique)
		WHILE @i <= @levels_count
			BEGIN
				SET @sql_for_price_calc = @sql_for_price_calc + 'group_level' + trim(cast(@i as char)) + ','
				DECLARE @func_num int
				SET @func_num = 3
				DECLARE @j int
				SET @j = 0
				WHILE @j < @func_num
					BEGIN
						declare @func varchar(3)
						if @j=0 begin set @func='min' end
						if @j=1 begin set @func='avg' end
						if @j=2 begin set @func='max' end

						SET @sql_for_price_calc = @sql_for_price_calc + 
																		--'(	select ' + @func + '(price1_converted) 
																		--	from sales_prepared2 s' + trim(cast(@i as char)) + ' 
																		--	where	s' + trim(cast(@i as char)) + '.group_level' + trim(cast(@i as char)) + '=ss.group_level' + trim(cast(@i as char)) + '
																		--		and s' + trim(cast(@i as char)) + '.store_id=ss.store_id
																		--		and s' + trim(cast(@i as char)) + '.s_date=ss.s_date ) 
																		-- as ' + @func + '_cprice_l' + trim(cast(@i as char)) + ',
																	
																		 'price1_converted-
																		 (	select ' + @func + '(price1_converted) 
																			from sales_prepared2 s' + trim(cast(@i as char)) + ' 
																			where	s' + trim(cast(@i as char)) + '.group_level' + trim(cast(@i as char)) + '=ss.group_level' + trim(cast(@i as char)) + '
																				and s' + trim(cast(@i as char)) + '.store_id=ss.store_id
																				and s' + trim(cast(@i as char)) + '.s_date=ss.s_date ) 
																		 as abs_dev_' + @func + '_cprice_l' + trim(cast(@i as char)) + ',
																		 
																		 (price1_converted-
																		 (	select ' + @func + '(price1_converted) 
																			from sales_prepared2 s' + trim(cast(@i as char)) + ' 
																			where	s' + trim(cast(@i as char)) + '.group_level' + trim(cast(@i as char)) + '=ss.group_level' + trim(cast(@i as char)) + '
																				and s' + trim(cast(@i as char)) + '.store_id=ss.store_id
																				and s' + trim(cast(@i as char)) + '.s_date=ss.s_date )) /  price1_converted
													
																		 as rel_dev_' + @func + '_cprice_l' + trim(cast(@i as char)) + '  ,
																		'
						SET @j = @j+1
					END
				SET @i = @i+1
			END
		SET @sql_for_price_calc = @sql_for_price_calc + '	product_id,store_id,s_date,product_type,product_subtype,product_category,price1_converted as price, s_count
															INTO sales_price_context
															FROM sales_prepared2 ss'

		SELECT @sql_for_price_calc
		EXEC(@sql_for_price_calc)

	END
GO


/* ���������  CREATE_PLAN_FOR_PRODUCT_SUBTYPE � CREATE_PLAN_FOR_PRODUCT ������� 
 ������� �����������, ������� ����� ������������ ��� ����������� ����������� nlopt */
DROP PROCEDURE IF EXISTS  CREATE_PLAN_FOR_PRODUCT_SUBTYPE
GO
CREATE PROCEDURE CREATE_PLAN_FOR_PRODUCT_SUBTYPE (@min_plan_koef float = 0.1, @max_plan_koef float = 0.1)
	AS
	BEGIN
		DROP TABLE IF EXISTS target_product_subtype_plan
		SELECT DISTINCT product_type, product_subtype, 
				sum(s_amount)*(1 - @min_plan_koef) as min_plan,
				sum(s_amount)*(1 + @max_plan_koef)  as max_plan
		INTO target_product_subtype_plan
		FROM	sales_prepared 
		WHERE	product_subtype is not null and  product_subtype != ''
		GROUP BY product_type, product_subtype
		ORDER BY product_type, product_subtype

	END
GO 

DROP PROCEDURE IF EXISTS  CREATE_PLAN_FOR_PRODUCT
GO
CREATE PROCEDURE CREATE_PLAN_FOR_PRODUCT (@min_plan_koef float = 0.2, @max_plan_koef float = 0.2)
	AS
	BEGIN
		DROP TABLE IF EXISTS target_product_store_plan
		SELECT DISTINCT product_id, store_id, 
				avg(s_amount)*(1 - @min_plan_koef) as min_plan,
				avg(s_amount)*(1 + @max_plan_koef)  as max_plan
		INTO target_product_store_plan
		FROM	sales_prepared 
		-- WHERE	product_subtype is not null and  product_subtype != ''
		GROUP BY product_id, store_id
		ORDER BY product_id, store_id

	END
GO 



USE MASTER
GO