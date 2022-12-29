/* уНРКЪММХЙ еКЕМЮ. цКНСАЮИР, 2020 */
/* 
щРН ТЮИК, Б ЙНРНПНЛ НОПЕДЕКЪЧРЯЪ (МН МЕ БШГШБЮЧРЯЪ) ОПНЖЕДСПШ. 
хЯЙКЧВЕМХЕ: НОПЕДЕКЪЕРЯЪ Х БШГШБЮЕРЯЪ ОПНЖЕДСПЮ ЯНГДЮМХЪ АЮГШ ДЮММШУ.
щРНР ТЮИК МСФМН ГЮОСЯРХРЭ ОЕПБШЛ, МХВЕЦН МЕ ЛЕМЪЪ.
*/


/*	
CREATE_DATABASE_PRICE_FORMATION ОПНЖЕДСПЮ
яНГДЮЕР АЮГС ДЮММШУ PRICE_FORMATION 
гЮДЮЕР COLLATE Х RECOVERY 
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
LOAD_FROM_FILES ОПНЖЕДСПЮ
1. яНГДЮЕР РЮАКХЖШ sales, products, stores. 
2. гЮЦПСФЮЕР Б МХУ ДЮММШЕ ХГ ТЮИКНБ (Я НОПЕДЕКЕММШЛХ ЯНЯРЮБНЛ Х ЯНДЕПФЮМХЕЛ ОНКЕИ). дХПЕЙРНПХЪ Х МЮХЛЕМНБЮМХЕ ТЮИКНБ ОЕПЕДЮЧРЯЪ Б ОПНЖЕДСПС ВЕПЕГ  Б ОЕПЕЛЕММШУ.
3. яМЮВЮКЮ ГЮЦПСФЮЕР ДЮММШЕ Б ОНКЪ nvarchar, ОНРНЛ ОПХНАПЮГСЕР Б date, nvarchar, float.
4. гЮДЮЕР ОЕПБХВМШЕ Х БМЕЬМХЕ ЙКЧВХ ДКЪ РЮАКХЖ.
*/
DROP PROCEDURE IF EXISTS LOAD_FROM_FILES
GO
CREATE PROCEDURE LOAD_FROM_FILES (@DIRECTORY		varchar(300) = 'C:\Users\elena.khotlyannik\Documents\2020AAPrice\sales01.04.2016-01.04.2018\',
								  @SALES_FILE1		varchar(100) = 'бШЦПСГЙЮ ОПНДЮФ Я 01.04.2016 - 31.12.2016.csv',								  
								  @SALES_FILE2		varchar(100) = 'бШЦПСГЙЮ ОПНДЮФ Я 01.01.2017 - 01.10.2017.csv',
								  @SALES_FILE3		varchar(100) = 'бШЦПСГЙЮ ОПНДЮФ Я 02.10.2017 - 01.04.2018.csv',
								  @PRODUCTS_FILE	varchar(100) = 'яОПЮБНВМХЙ РНБЮПНБ(ОН ЙНРНПШЛ АШКХ ОПНДЮФХ Я 01.04.2016-01.04.2018).csv',
								  @STORES_FILE		varchar(100) = 'лЮЦЮГХМШ.csv'								  
								  )
	AS 
	BEGIN
	/*1.1.хМХЖХЮКХГЮЖХЪ РЮАКХЖШ ОПНДЮФ*/
		DROP TABLE IF EXISTS sales 
		CREATE TABLE sales	(	[s_date]		nvarchar(50) NOT NULL,
								[store_id]		nvarchar(50) NOT NULL,
								[product_id]	nvarchar(50) NOT NULL,
								[s_amount]		nvarchar(50),
								[s_count]		nvarchar(50)
							)
		
		/*1.2.гЮЦПСГЙЮ ДЮММШУ Б РЮАКХЖС ОПНДЮФ ХГ 3-У ТЮИКНБ*/
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
		
		/* ОПНБЕПЙЮ ЙНКХВЕЯРБЮ ГЮОХЯЕИ Б РЮАКХЖЕ ОПНДЮФ
		select count( *) from  sales
		*/

		/*1.3.оПЕНАПЮГНБЮМХЕ РХОНБ ДЮММШУ Б РЮАКХЖЕ ОПНДЮФ */
		ALTER TABLE sales ALTER COLUMN 	s_date		date	NOT NULL
		ALTER TABLE sales ALTER COLUMN 	store_id	bigint	NOT NULL
		ALTER TABLE sales ALTER COLUMN 	product_id	bigint	NOT NULL
		
		UPDATE sales set s_amount=TRY_CONVERT(float, REPLACE(s_amount,',','.'))
		ALTER TABLE sales ALTER COLUMN s_amount float
		
		UPDATE sales set s_count=TRY_CONVERT(float, REPLACE(s_count,',','.'))
		ALTER TABLE sales ALTER COLUMN s_count float
		
		/* 1.4 ДНАЮБКЕМХЕ ЙКЧВЮ */
		ALTER table sales
		ADD CONSTRAINT PK_sales PRIMARY KEY CLUSTERED (s_date, store_id, product_id)
		
		
		/*2.1.хМХЖХЮКХГЮЖХЪ ЯОПЮБНВМХЙЮ РНБЮПНБ */
		DROP TABLE IF EXISTS [products]
		CREATE TABLE [products] (	[product_id] nvarchar(50) NOT NULL,
									[product_name] nvarchar(150)
								)

		/*2.2.гЮЦПСГЙЮ ДЮММШУ Б ЯОПЮБНВМХЙ РНБЮПНБ */
		SET @BULK_INSERT = '
		BULK INSERT products 
		FROM ''' + @DIRECTORY  + @PRODUCTS_FILE + '''
		WITH	(	FIRSTROW=2,
					FIELDTERMINATOR = '';'', 
					ROWTERMINATOR = ''0x0a'',
					CODEPAGE = ''1251''
				)'
		EXEC(@BULK_INSERT)
		
		/*2.3.ОПЕНАПЮГНБЮМХЕ РХОЮ ДЮММШУ Х ДНАЮБКЕМХЕ ЙКЧВЮ Б РЮАКХЖС РНБЮПНБ */
		ALTER TABLE products ALTER COLUMN 	product_id bigint NOT NULL

		ALTER table products
		ADD CONSTRAINT PK_products PRIMARY KEY CLUSTERED (product_id)

		
		/*3.1.хМХЖХЮКХГЮЖХЪ ЯОПЮБНВМХЙЮ ЛЮЦЮГХМНБ */
		DROP TABLE IF EXISTS stores ;
		CREATE TABLE stores	(	[store_id] nvarchar(50) NOT NULL,
								[store_name] nvarchar(150)
							)
		
		/*3.2.гЮЦПСГЙЮ ДЮММШУ Б ЯОПЮБНВМХЙ ЛЮЦЮГХМНБ */
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
		
		/*3.3.оПЕНАПЮГНБЮМХЕ РХОНБ Б РЮАКХЖЕ ЛЮЦЮГХМНБ */
		ALTER TABLE stores ALTER COLUMN store_id bigint NOT NULL

		/*3.4.дНАЮБКЕМХЕ ЙКЧВЮ Б РЮАКХЖС ЛЮЦЮГХМНБ */
		ALTER table stores
		ADD CONSTRAINT PK_stores PRIMARY KEY CLUSTERED (store_id)
		

		/* 4.4 ДНАЮБКЕМХЕ БМЕЬМХУ ЙКЧВЕИ Б РЮАКХЖЕ ОПНДЮФ */
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
ANONIMISING ОПНЖЕДСПЮ 
1. ХГЛЕМЪЕР ХДЕМРХТХЙЮРНП ЛЮЦЮГХМЮ , ОЕПЕХЛЕМНБШБЮЕР ЛЮЦЮГХМШ
2. ХГЛЕМЪЕР ХДЕМРХТХЙЮРНП РНБЮПЮ, ОЕПЕБНДХР Б БЕПУМХИ ПЕЦХЯРП РНБЮПШ
3. ЙНКХВЕЯРБН СЛМНФЮЕР МЮ 5, ЖЕМС МЮ 1.3
*/
DROP PROCEDURE IF EXISTS ANONIMISING
GO
CREATE PROCEDURE ANONIMISING
	AS
	BEGIN
	/* ХГЛЕМЕМХЕ ХДЕМРХТХЙЮРНПЮ ЛЮЦЮГХМЮ */
		UPDATE [stores] 
		SET store_id= (	SELECT new_id 
							FROM (SELECT	ROW_NUMBER() OVER(ORDER BY [store_name]) new_id,
											store_id
								  FROM [stores]  )  
							AS tab
							WHERE tab.store_id=[stores].store_id)


		/* ХГЛЕМЕМХЕ ХДЕМРХТХЙЮРНПЮ РНБЮПЮ */
		UPDATE [products]  
		SET product_id= (	SELECT new_id 
							FROM (SELECT	ROW_NUMBER() OVER(ORDER BY [product_name]) new_id,
											product_id
								  FROM [products]   )  
							AS tab
							WHERE tab.product_id=[products] .product_id)

		/* оЕПЕХЛЕМНБЮМХЕ ЛЮЦЮГХМНБ */
		UPDATE [stores] 
		SET store_name = 'люцюгхм ю' WHERE store_id=1

		UPDATE [stores] 
		SET store_name = 'люцюгхм а' WHERE store_id=2

		/* бЕПУМХИ ПЕЦХЯРП ДКЪ РНБЮПНБ */
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
оПНЖЕДСПЮ ДКЪ ЯНГДЮМХЪ МЕОПЕПШБМНЦН БПЕЛЕММНЦН ПЪДЮ ОН ЙЮФДНЛС ОПНДСЙРС 
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
		/*  дНАЮБХЛ ОНКЪ Б sales */
		ALTER TABLE [sales] ADD 
				price1					float NULL,		-- ДКЪ ДНАЮБКЕММШУ ГЮОХЯЕИ, ГМЮВЕМХЕ ПЮБМН ЯПЕДМЕЛС ОН БЯЕЛ БШАНПЙЕ (ЙПНЛЕ МСКЕБШУ) 
				price2					float NULL,		-- ДКЪ ДНАЮБКЕММШУ ГЮОХЯЕИ, ГМЮВЕМХЕ ПЮБМН ГМЮВЕМХЧ ГЮ ОПЕДШДСЫСЧ МЕМСКЕБСЧ ДЮРС 
				price3					float NULL,		-- ДКЪ ДНАЮБКЕММШУ ГЮОХЯЕИ, ГМЮВЕМХЕ ПЮБМН  ЯПЕДМЕЛС ЛЕФДС ДБСЛЪ АКХФЮИЬХЛХ МЕМСКЕБШЛХ  	
				auto_sign				integer NULL,	-- 
														-- , ПЮБМН 1 ДКЪ ГЮЦПСФЕММШУ ГЮОХЯЕИ c ОНКНФХРЕКЭМШЛХ ГМЮВЕМХЪЛХ ЯСЛЛ Х ЙНКХВЕЯРБЮ
														-- , ПЮБМН 0 ДКЪ ГЮЦПСФЕММШУ ГЮОХЯЕИ c меОНКНФХРЕКЭМШЛХ ГМЮВЕМХЪЛХ ЯСЛЛ ХКХ ЙНКХВЕЯРБЮ
														-- , ПЮБМН 5 ДКЪ ДНАЮБКЕММШУ ГЮОХЯЕИ (ГЮОХЯХ ДНАЮБКЕМХ ДКЪ ДНОНКМЕМХЪ БПЕЛЕММНЦН ПЪДЮ)
				prev_notnull_date		date,
				prev_notnull_price		float,
				next_notnull_date		date,	
				next_notnull_price		float,
				avg_product_store_price float,
				time_series_flag		int	,			/* "ОПХГМЮЙ БПЕЛЕММНЦН ДХЮОЮГНМЮ" =1, ЕЯКХ ГЮОХЯЭ БМСРПХ БПЕЛЕММНЦН ПЪДЮ ДКЪ ЙНМЙПЕРМНИ ОЮПШ опндсйр+люцюгхм */
				min_date				date,
				max_date				date;

	END
GO


/*	TIME_SERIES_AND_PRICES
оПНЖЕДСПЮ ДНОНКМЪЕР БПЕЛЕММНИ ПЪД
пЮЯЯВХРШБЮЕР РПХ БЮПХЮМРЮ ЯСЛЛ
оПНЯРЮБКЪЕР ОПХГМЮЙ, СЙЮГШБЮЧЫХИ ГЮЦПСФЕМЮ ЯРПНЙЮ ХКХ ДНАЮБКЕМЮ Б ПЮЛЙЮУ ДНОНКМЕМХЪ БПЕЛЕММНЦН ПЪДЮ
оПНЯРЮБКЪЕР ОПХГМЮЙ, ЕЯКХ ЯРПНЙЮ бмсрпх БПЕЛЕММНЦН ДХЮОЮГНМЮ ДКЪ ЙНМПЕРМНИ ОЮПШ опндсйр+люцюгхм
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

оПНЖЕДСПЮ ГЮЛЕМЪЕР ЯХЛБНК @variant1, ЙНРНПШИ ЯКЕДСЕР ОНЯКЕ ВХЯКЮ ЯПЮГС ХКХ ВЕПЕГ ОПНАЕК,
 МЮ ЯХЛБНК @measure_simbol.
мЮОПХЛЕП, ДКЪ ЦПЮЛЛ: @variant1 = цп, @measure_simbol=ц.

оПХ ЩРНЛ ДЕКЮЕРЯЪ ОПНБЕПЙЮ, ВРН МЕОНЯПЕДЯРБЕММН ОНЯКЕ ЯХЛБНКЮ @variant1 МЕ ЯРНХР АСЙБЮ (ВРНАШ МЕ ГЮЛЕМХРЭ , МЮОПХЛЕП, ЯНВЕРЮМХЕ '3 цпюмюрю' МЮ '3 цюмюрю' )
*/
DROP PROCEDURE IF EXISTS REPLACE_MEASURE_SIMBOLS 
GO
CREATE PROCEDURE REPLACE_MEASURE_SIMBOLS  (@measure_simbol nvarchar(20),
											--  @remove_measure int =1,
											@variant1  nvarchar(20) = '',
											@variant2  nvarchar(20) = '')
	AS
	BEGIN
		/* ДНАЮБКЪЧ ОПНАЕКШ ДН Х ОНЯКЕ (,),/  */	
		UPDATE step0
		SET product_name = replace(replace(replace(product_name,'(',' ( '),')',' ) '),'/',' / ')

		/* САХПЮЧ КХЬМХЕ ОПНАЕКШ Х ОПНАЕКШ Б ЙНМЖЕ ЯРПНЙХ*/	
		UPDATE step0
		SET product_name = rtrim(replace(product_name,'  ',' '))

		/* ГЮЛЕМЮ ОЕПБНЦН БЮПХЮРХБМНЦН ЯХЛБНКЮ МЮ НЯМНБМНИ*/
		UPDATE step0
		SET product_name = replace(product_name, @variant1 , @measure_simbol)
		WHERE	(product_name like concat('%[0-9]',@variant1,'%')
			or product_name like concat('%[0-9] ',@variant1,'%'))
			and product_name not like concat('%[0-9]',@variant1,'[ю-ъ]%')
			and product_name not like concat('%[0-9] ',@variant1,'[ю-ъ]%')

		/*СДЮКЪЧ ОПНАЕКШ ЛЕФДС ВХЯКНЛ Х ЯХЛБНКНЛ ХГЛЕПЕМХЪ (МЮОПХЛЕП, ГЮЛЕМЪЧ [0-9] ц МЮ [0-9]ц)*/
		UPDATE step0 
		SET product_name = replace(product_name, concat(' ',@measure_simbol), @measure_simbol)  
		WHERE product_name like   concat('%[0-9] ',@measure_simbol,'%')

	END
GO


/*
EXTRACT_MEASURE
оПНЖЕДСПЮ БШВКЕМЪЕР ЯНВЕРЮМХЪ ВХЯКЮ Я ЯХЛБНКНЛ. мЮОПХЛЕП, 100ц. 
дНАЮБКЪЕР ЩРХ ЯНВЕРЮМХЪ Б ОНКЕ temp_param, Х СДЮКЪЕР ЩРХ ЯНВЕРЮМХЪ ХГ ОНКЪ product_name (ОПХ ОПХГМЮЙЕ @remove_measure = 1 ).
гМЮВЕМХЕ ХГ temp_param ОНЯКЕ БШОНКМЕМХЪ ОПНЖЕДСПШ МСФМН ОЕПЕМЕЯРХ Б ОНЯРНЪММШИ ДПСЦНИ ЯРНКАЕЖ
оНДСЛЮРЭ, НАМСКХРЭ КХ ЕЦН Б МЮВЮКЕ ОПНЖЕДСПШ
оЮПЮЛЕРПШ
	@measure_simbol - ЯХЛБНК, ЯНВЕРЮМХЕ Я ЙНРНПШЛ МЮДН МЮИРХ
	(ОНЙЮ МЕ ПЕЮКХГНБЮМ) @remove_measure - ОПХГМЮЙ, СДЮКЪРЭ КХ МЮИДЕММНЕ ЯНВЕРЮМХЕ ХГ product_name (ОНЙЮ МЕ ПЕЮКХГНБЮМ)
	*/
DROP PROCEDURE	IF EXISTS EXTRACT_MEASURE 
GO
CREATE PROCEDURE EXTRACT_MEASURE  (@measure_simbol nvarchar(20))
	AS
	BEGIN
		/* ДНАЮБКЪЧ ОПНАЕКШ ДН Х ОНЯКЕ (,),/  */	
		UPDATE step0
		SET product_name = replace(replace(replace(product_name,'(',' ( '),')',' ) '),'/',' / ')

		/* САХПЮЧ КХЬМХЕ ОПНАЕКШ Х ОПНАЕКШ Б ЙНМЖЕ ЯРПНЙХ*/	
		UPDATE step0
		SET product_name = rtrim(replace(product_name,'  ',' '))
		
		/*СДЮКЪЧ ОПНАЕКШ ЛЕФДС ВХЯКНЛ Х ЯХЛБНКНЛ ХГЛЕПЕМХЪ (МЮОПХЛЕП, ГЮЛЕМЪЧ [0-9] ц МЮ [0-9]ц)*/
		UPDATE step0 
		SET product_name = replace(product_name, concat(' ',@measure_simbol), @measure_simbol)  
		WHERE product_name like   concat('%[0-9] ',@measure_simbol,'%')
			and product_name not like concat('%[0-9]',@measure_simbol,'[ю-ъ]%')

		/* НОПЕДЕКЕЪЧ ЙЮЙНЕ-РН БУНФДЕМХЕ [0-9]ц*/
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
			and product_name not like concat('%[0-9]',@measure_simbol,'[ю-ъ]%')

		/* НОПЕДЕКЪЧ ОНГХЖХЧ АКХФЮИЬЕЦН ЯКЕБЮ ОПНАЕКЮ */
		DROP TABLE IF EXISTS step02
		SELECT	*,
				--substring(product_name, 1,last_num_index) as sub1,
				--reverse(substring(product_name, 1,last_num_index)) as sub2,
				--charindex(' ',reverse(substring(product_name, 1,last_num_index))) as charindex_space_rev,
				len(substring(product_name, 1,last_num_index)) - charindex(' ',reverse(substring(product_name, 1,last_num_index)))+1 as charindex_space
		INTO step02 	
		FROM step01
	
		/* НОПЕДЕКЪЧ ... ЯЛ ЙНЛЛЕМР Б ГЮОПНЯЕ */
		DROP TABLE IF EXISTS step03
		SELECT	product_id,
				product_name,
				last_num_index,
				charindex_space,
				(last_num_index-charindex_space)+len(@measure_simbol) as count_simb,	--ЙНКХВЕЯРБН ЯХЛБНКНБ, ЙНРНПНЕ ОНИДЕР Б ОЕПЮЛЕРП
				last_num_index+len(@measure_simbol)+1 as charindex_after,				--ХМДЕЙЯ ОЕПБНЦН ЯХЛБНКЮ ОНЯКЕ @measure_simbol
				charindex_space-1 as charindex_before						-- ХМДЕЙЯ ЯХЛБНКЮ ОЕПЕД ОПНАЕКНЛ
		INTO step03
		FROM step02

		/* нОПЕДЕКЪЧ ОЮПЮЛЕРП Х МНБСЧ ЯРПНЙС */
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
			and product_name not like concat('%[0-9]',@measure_simbol,'[ю-ъ]%')


	
	END
GO

/* ЯНГДЮМХЕ РЮАКХЖШ ОПЮБХК ДКЪ ЙЮРЕЦНПХГЮЖХХ ОПНДСЙРЮ */
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


/* ГЮОНКМЕМХЕ РЮАКХЖШ ОПЮБХК ДКЪ ЙЮРЕЦНПХГЮЖХХ ОПНДСЙРЮ */ 
/* ЩРН ЖЕКЕБНИ БЮПХЮМР, Б РНЛ ЯЛШЯКЕ, ВРН ДЮКЭЬЕ БЯРПЕВЮЧРЯЪ ДПСЦХЕ ЯОНЯНАШ ЙЮРЕЦНПХГЮЖХХ, МН НМХ ОНЪБКЪКХЯЭ "ХЯРНПХВЕЯЙХ", Х МЮДН АШ ОЕПЕМЕЯРХ ХУ Б ЩРС РЮАКХЖС ОПЮБХК */
DROP PROCEDURE IF EXISTS  FILL_product_category_rules
GO
CREATE PROCEDURE FILL_product_category_rules 
	AS
	BEGIN
			INSERT INTO product_category_rules 
			(step_priority,		product_name_included,	product_name_excluded,	product_type,	product_subtype,	product_category,		product_subcategory,		producer)
			VALUES
			(1,					'%ъижн%оепеоек%',		'',					'опндсйрш',		'ъижю',					'ъижн оепеоекхмне',			'',					''),
			(2,					'%ъижн%',				'',					'опндсйрш',		'ъижю',					'ъижн йспхмне',				'',					''),
			(11,				'%ыедпне%гюярнкэе%',	'',					'',				'',						'',							'',					'ой кюлю'),
			(1,					'пшаю %',				'',					'опндсйрш',		'пшаю/пшанопндсйрш',	'пшаю',						'',					''),
			(1,					'йпеберйх %',			'',					'опндсйрш',		'пшаю/пшанопндсйрш',	'йпеберйх',					'',					''),
			(1,					'лъян йпхкъ %',			'',					'опндсйрш',		'пшаю/пшанопндсйрш',	'лъян йпхкъ',				'',					''),
			(1,					'йюкэлюп %',			'',					'опндсйрш',		'пшаю/пшанопндсйрш',	'йюкэлюп',					'',					''),
			(1,					'яюипю%',				'',					'опндсйрш',		'пшаю/пшанопндсйрш',	'яюипю',					'',					''),
			(1,					'%опеяепбш%',			'',					'опндсйрш',		'пшаю/пшанопндсйрш',	'опеяепбш',					'',					''),
			(1,					'%ьопнрш%',				'',					'опндсйрш',		'пшаю/пшанопндсйрш',	'ьопнрШ',					'',					''),
			(1,					'оевемэ%рпеяйх%',		'',					'опндсйрш',		'пшаю/пшанопндсйрш',	'оевемэ рпеяйх',			'',					''),
			(1,					'оевемэ%лхмрюъ%',		'',					'опндсйрш',		'пшаю/пшанопндсйрш',	'оевемэ лхмрюъ',			'',					''),
			(1,					'%рсмеж%',				'%бхяйюя%',			'опндсйрш',		'пшаю/пшанопндсйрш',	'рсмеж',					'',					''),
			(1,					'цнпасью%',				'%гюоев%',			'опндсйрш',		'пшаю/пшанопндсйрш',	'цнпасью',					'',					''),
			(1,					'йпюанбше оюкнвйх%',	'%гюоев%',			'опндсйрш',		'пшаю/пшанопндсйрш',	'йпюанбше оюкнвйх',			'',					''),
			(1,					'йпюанбне оюкнвйх%',	'%гюоев%',			'опндсйрш',		'пшаю/пшанопндсйрш',	'йпюанбше оюкнвйх',			'',					''),
			(2,					'хйпю %',				'',					'опндсйрш',		'пшаю/пшанопндсйрш',	'хйпю',						'',					''),

			(1,					'%хйпю%цпха%',			'',				'опндсйрш',		'йскхмюпхъ/онкстюапхйюрш',	'нбнымюъ хйпю',				'',					''),
			(1,					'%хйпю%аюйкюфюм%',		'',				'опндсйрш',		'йскхмюпхъ/онкстюапхйюрш',	'нбнымюъ хйпю',				'',					''),
			(1,					'%хйпю%йюаюв%',			'',				'опндсйрш',		'йскхмюпхъ/онкстюапхйюрш',	'нбнымюъ хйпю',				'',					''),

			(1,					'%оюьрер%лъян%орхжш%',	'',				'опндсйрш',		'йскхмюпхъ/онкстюапхйюрш',	'оюьрер',					'лъян орхжш',			''),

			(1,					'лъян жшокемйю%',		'',					'опндсйрш',		'лъян/орхжю',			'жшокемнй',					'лъян жшокемйю',		''),
			(2,					'лъян орхжш%',			'',					'опндсйрш',		'лъян/орхжю',			'лъян орхжш',				'лъян орхжш',			''),
			(2,					'%апникеп%',			'',					'опндсйрш',		'лъян/орхжю',			'жшокемнй',					'лъян орхжш',			''),
			(1,					'жшокемнй%',			'',					'опндсйрш',		'лъян/орхжю',			'жшокемнй',					'',					''),
			(1,					'оевемэ %',				'',					'опндсйрш',		'лъян/орхжю',			'оевемэ',					'',					''),
			(1,					'упъыхйх йспхмше%',		'',					'опндсйрш',		'лъян/орхжю',			'йспхжю',		'',					''),
			(1,					'%йпшкн йспхмне%',		'',					'опндсйрш',		'лъян/орхжю',			'йспхжю',		'',					''),
			(1,					'%лъян%йспхмне%',		'',					'опндсйрш',		'лъян/орхжю',			'йспхжю',		'',					''),
			(1,					'мюанп дкъ рсьемхъ%',		'',					'опндсйрш',		'лъян/орхжю',		'мюанп дкъ рсьемхъ',		'',					''),

			(1,					'%яшп рбнпнфмши%',		'',					'опндсйрш',		'яшпш',					'яшп рбнпнфмши',			'',					''),
			(1,					'%яшп тюянбюммши%',		'',					'опндсйрш',		'яшпш',					'яшп тюянбюммши',			'',					''),
			(1,					'%яшпмши опндсйр%',		'',					'опндсйрш',		'яшпш',					'яшпмши опндсйр',			'',					''),
			(1,					'яшп %',				'',					'опндсйрш',		'яшпш',					'яшп %',					'',					''),

			(1,					'инцспр %',					'',					'опндсйрш',		'лнкнвмше опндсйрш',	'инцспр',					'',					''),
			(1,					'%рбнпнц %',				'',					'опндсйрш',		'лнкнвмше опндсйрш',	'рбнпнц',					'',					''),
			(1,					'%ахтхднй %',				'',					'опндсйрш',		'лнкнвмше опндсйрш',	'йетхп',					'',					''),
			(1,					'%пъфемйю %',				'',					'опндсйрш',		'лнкнвмше опндсйрш',	'пъфемйю',					'',					''),
			(1,					'%якхбйх %',				'',					'опндсйрш',		'лнкнвмше опндсйрш',	'якхбйх',					'',					''),
			(1,					'%ахнинцспр %',				'',					'опндсйрш',		'лнкнвмше опндсйрш',	'инцспр',					'',					''),
			(1,					'опндсйр инцспрмши%',		'',					'опндсйрш',		'лнкнвмше опндсйрш',	'инцспр',					'',					''),
			(1,					'%ахнрбнпнц %',				'',					'опндсйрш',		'лнкнвмше опндсйрш',	'рбнпнц',					'',					''),
			(1,					'%ахнопндсйр%',				'',					'опндсйрш',		'лнкнвмше опндсйрш',	'инцспр',					'',					''),
			(1,					'%ямефнй %',				'',					'опндсйрш',		'лнкнвмше опндсйрш',	'ямефнй',					'',					''),
			(1,					'%яшпнй %',					'',					'опндсйрш',		'лнкнвмше опндсйрш',	'рбнпнфмши опндсйр',					'',			''),
			(1,					'деяепр рбнпнф%',			'',					'опндсйрш',		'лнкнвмше опндсйрш',	'рбнпнфмши опндсйр',					'',					''),
			(1,					'%рбнпнфмши опндсйр%',		'',					'опндсйрш',		'лнкнвмше опндсйрш',	'рбнпнфмши опндсйр',		'',					''),
			(1,					'%опндсйр рбнпнфмши%',		'',					'опндсйрш',		'лнкнвмше опндсйрш',	'рбнпнфмши опндсйр',		'',					''),
			(1,					'лнкнйн%',					'',					'опндсйрш',		'лнкнвмше опндсйрш',	'лнкнйн',					'',					''),
			(1,					'%лнкнйняндепфюыхи ондсйр%','',					'опндсйрш',		'лнкнвмше опндсйрш',	'лнкнйняндепфюыхи ондсйр',	'',					''),
			(1,					'%рбнпнфемне%',				'',					'опндсйрш',		'лнкнвмше опндсйрш',	'рбнпнц',				'',					''),
			(1,					'рбнпнфнй%',				'',					'опндсйрш',		'лнкнвмше опндсйрш',	'рбнпнц',				'',					''),
			(1,					'рбнпнфмне гепмн%',			'',					'опндсйрш',		'лнкнвмше опндсйрш',	'рбнпнц',				'',					''),
			(1,					'%йетхп%',					'',					'опндсйрш',		'лнкнвмше опндсйрш',	'йетхп',					'',					''),
			(1,					'%яшпнй цкюгхпнбюммши%',	'',					'опндсйрш',		'лнкнвмше опндсйрш',	'яшпнй цкюгхпнбюммши',		'',					''),
			(1,					'бюп%яцсы%',				'',					'опндсйрш',		'лнкнвмше опндсйрш',	'яцсыемне лнкнйн',				'',					''),
			(1,					'яцсыемйю%',				'',					'опндсйрш',		'лнкнвмше опндсйрш',	'яцсыемне лнкнйн',				'',					''),
			(1,					'йюйюн яцсы%',				'',					'опндсйрш',		'лнкнвмше опндсйрш',	'йюйюн яцсы',					'',					''),
			(1,					'ялерюмю%',					'',					'опндсйрш',		'лнкнвмше опндсйрш',	'ялерюмю',					'',					''),
			(1,					'лнкнвмши о%',				'',					'опндсйрш',		'лнкнвмше опндсйрш',	'лнкнвмши опндсйр',			'',					''),
			(1,					'йнйреикэ лнкнвмши%',		'',					'опндсйрш',		'лнкнвмше опндсйрш',	'йнйреикэ лнкнвмши',			'',					''),
			(1,					'лнкнвмши йнйреикэ%',		'',					'опндсйрш',		'лнкнвмше опндсйрш',	'йнйреикэ лнкнвмши',			'',					''),
			(1,					'йнйреикэ люфх%',			'',					'опндсйрш',		'лнкнвмше опндсйрш',	'йнйреикэ лнкнвмши',			'',					''),
			(1,					'осддхмц%',					'',					'опндсйрш',		'лнкнвмше опндсйрш',	'инцспр',			'',					''),
			(1,					'осдхмц%',					'',					'опндсйрш',		'лнкнвмше опндсйрш',	'инцспр',			'',					''),

			
			(1,					'ахн%релю%',				'',					'опндсйрш',		'деряйне охрюмхе',	'лнкнвмши опндсйр дкъ дереи',		'',				''),
			(1,					'очпе%р╗лю%',				'',					'опндсйрш',		'деряйне охрюмхе',	'очпе дкъ дереи',		'',				''),
			(1,					'%очпе%мъмъ%',				'',					'опндсйрш',		'деряйне охрюмхе',	'очпе дкъ дереи',		'',				''),
			
			(1,					'%сйяся%',					'',					'опндсйрш',		'йнмяепбюжхъ/янсяш',	'сйяся',					'',					''),
			(1,					'%люинмег%',				'%люинмег[ю-ъ]%',	'опндсйрш',		'йнмяепбюжхъ/янсяш',	'люинмег',					'',					''),
			(1,					'люинмегмши янся%',			'',					'опндсйрш',		'йнмяепбюжхъ/янсяш',	'люинмег',					'',					''),
			(1,					'янся%',					'',					'опндсйрш',		'йнмяепбюжхъ/янсяш',	'янся',						'',					''),
			(1,					'йервсо %',					'',					'опндсйрш',		'йнмяепбюжхъ/янсяш',	'йервсо',					'',					''),
			(1,					'нцспжш%янке%',				'',					'опндсйрш',		'йнмяепбюжхъ/янсяш',	'нцспжш янкемше',			'',					''),
			(1,					'%лнпяйюъ%йюосярю%',		'',					'опндсйрш',		'йнмяепбюжхъ/янсяш',	'лнпяйюъ йюосярю',			'',					''),
			(2,					'%йбюьем%',					'опхопюбю%',		'опндсйрш',		'йнмяепбюжхъ/янсяш',	'йюосярю йбюьемюъ',			'',					''),
			(1,					'%сйясямюъ йхякнрю%',		'',					'опндсйрш',		'йнмяепбюжхъ/янсяш',	'сйяся',					'',					''),
			(1,					'%юдфхйю%',					'%йнркерю%',		'опндсйрш',		'йнмяепбюжхъ/янсяш',	'юдфхйю',					'',					''),
			(1,					'%кхлнммюъ йхякнрю%',		'',					'опндсйрш',		'йнмяепбюжхъ/янсяш',	'кхлнммюъ йхякнрю',			'',					''),
			(1,					'%рнлюрмюъ оюярю%',			'',					'опндсйрш',		'йнмяепбюжхъ/янсяш',	'рнлюрмюъ оюярю',			'',					''),
			(1,					'йхкэйю%р / я%',			'',					'опндсйрш',		'йнмяепбюжхъ/янсяш',	'йхкэйю б р / я',			'',					''),
			(1,					'йхкэйю%р/я%',			'',					'опндсйрш',		'йнмяепбюжхъ/янсяш',	'йхкэйю б р / я',			'',					''),
			(1,					'люякхмш%',					'',					'опндсйрш',		'йнмяепбюжхъ/янсяш',	'люякхмш%',					'',					''),

			(1,					'йксамхйю %',				'',					'опндсйрш',		'тпсйрш/ъцндш',		'йксамхйю',						'',					''),
			(1,					'апсямхйю%',				'',					'опндсйрш',		'тпсйрш/ъцндш',		'апсямхйю',						'',					''),
			(1,					'цпеиотпср%',				'',					'опндсйрш',		'тпсйрш/ъцндш',		'цпеиотпср',					'',					''),
			(1,					'йкчйбю%',					'',					'опндсйрш',		'тпсйрш/ъцндш',		'йкчйбю',						'',					''),
			(1,					'вепеьмъ%',					'',					'опндсйрш',		'тпсйрш/ъцндш',		'вепеьмъ',						'',					''),
			(1,					'цпсью%',					'',					'опндсйрш',		'тпсйрш/ъцндш',		'цпсью',						'',					''),
			(1,					'онлекн%',					'',					'опндсйрш',		'тпсйрш/ъцндш',		'онлекн',						'',					''),
			(1,					'ъакнй%',					'',					'опндсйрш',		'тпсйрш/ъцндш',		'ъакнйн',						'',					''),
			(1,					'"ъакнй%',					'',					'опндсйрш',		'тпсйрш/ъцндш',		'ъакнйн',						'',					''),
			(1,					'%аюмюмш %',				'',					'опндсйрш',		'тпсйрш/ъцндш',		'аюмюмш',						'',					''),
			(1,					'юмюмюя %',					'',					'опндсйрш',		'тпсйрш/ъцндш',		'юмюмюя',						'',					''),
			(1,					'йхбх %',					'',					'опндсйрш',		'тпсйрш/ъцндш',		'йхбх',							'',					''),
			(1,					'юоекэяхмш%',				'',					'опндсйрш',		'тпсйрш/ъцндш',		'юоекэяхмш',					'',					''),
			(1,					'%люмдюпхмш%',				'',					'опндсйрш',		'тпсйрш/ъцндш',		'люмдюпхмш',					'',					''),
			(1,					'люмдюпхм %',				'',					'опндсйрш',		'тпсйрш/ъцндш',		'люмдюпхмш',					'',					''),
			(1,					'оепяхйх%',					'',					'опндсйрш',		'тпсйрш/ъцндш',		'оепяхйх',						'',					''),
			(1,					'вепмнякхб %',				'',					'опндсйрш',		'тпсйрш/ъцндш',		'вепмнякхб',					'',					''),
			(1,					'йспюцю %',					'',					'опндсйрш',		'тпсйрш/ъцндш',		'йспюцю',						'',					''),
			(1,					'юпасг%',					'',					'опндсйрш',		'тпсйрш/ъцндш',		'юпасг',						'',					''),
			(1,					'бхмнцпюд%',				'',					'опндсйрш',		'тпсйрш/ъцндш',		'бхмнцпюд',						'',					''),
			(1,					'мейрюпхмш %',				'',					'опндсйрш',		'тпсйрш/ъцндш',		'мейрюпхмш',					'',					''),
			(1,					'кхлнмш%',					'',					'опндсйрш',		'тпсйрш/ъцндш',		'кхлнмш',						'',					''),
			(1,					'бхьмъ%',					'',					'опндсйрш',		'тпсйрш/ъцндш',		'бхьмъ',						'',					''),
			(1,					'цпюмюр%',					'цпюмюр[ю-ъ]%',		'опндсйрш',		'тпсйрш/ъцндш',		'цпюмюр',						'',					''),
			(1,					'дшмъ%',					'',					'опндсйрш',		'тпсйрш/ъцндш',		'дшмъ',							'',					''),
			(1,					'%ъцндю йксамхйю%',			'',					'опндсйрш',		'тпсйрш/ъцндш',		'йксамхйю',						'',					''),
			(1,					'тхмхйх%',					'',					'опндсйрш',		'тпсйрш/ъцндш',		'тхмхйх',						'',					''),
			(1,					'усплю%',					'',					'опндсйрш',		'тпсйрш/ъцндш',		'усплю',						'',					''),
			(1,					'якхбю%',					'',					'опндсйрш',		'тпсйрш/ъцндш',		'якхбю',						'',					''),
			(1,					'юапхйня,%',				'',					'опндсйрш',		'тпсйрш/ъцндш',		'юапхйня',						'',					''),
			
			(1,					'цпхаш %',					'',					'опндсйрш',		'нпеух/цпхаш',		'цпхаш',						'',					''),
			(1,					'ьюлохмэнмш %',				'',					'опндсйрш',		'нпеух/цпхаш',		'ьюлохмэнмш',					'',					''),
			(1,					'юпюухя %',					'',					'опндсйрш',		'нпеух/цпхаш',		'юпюухя',						'',					''),
			(1,					'тсмдсй %',					'',					'опндсйрш',		'нпеух/цпхаш',		'тсмдсй',						'',					''),
			(1,					'лхмдюкэ %',				'',					'опндсйрш',		'нпеух/цпхаш',		'лхмдюкэ',						'',					''),
			(1,					'цпежйхи %',				'',					'опндсйрш',		'нпеух/цпхаш',		'цпежйхи нпеу',					'',					''),
			(1,					'цежйхи %',					'',					'опндсйрш',		'нпеух/цпхаш',		'цпежйхи нпеу',					'',					''),
			(1,					'йеьэч %',					'',					'опндсйрш',		'нпеух/цпхаш',		'йеьэч',						'',					''),
			(1,					'ршйбеммше яелевйх %',		'',					'опндсйрш',		'нпеух/цпхаш',		'ршйбеммше яелевйх',			'',					''),
			(1,					'цпежйхи нпеу%',			'',					'опндсйрш',		'нпеух/цпхаш',		'цпежйхи нпеу',					'',					''),
			(1,					'йедпнбше нпеух%',			'',					'опндсйрш',		'нпеух/цпхаш',		'йедпнбше нпеух',				'',					''),

			(1,					'%бндю%аег цюгю%',			'',					'опндсйрш',		'бндю',				'бндю аег цюгю',				'',					''),
			(1,					'%бндю%цюгхп%',				'',					'опндсйрш',		'бндю',				'бндю цюгхпнбюммюъ',			'',					''),
						
			(1,					'%мюохрнй%',				'',					'опндсйрш',		'мюохрйх',			'мюохрнй',						'',					''),
			(1,					'%йбюя%',					'',					'опндсйрш',		'мюохрйх',			'йбюя',							'',					''),
			(1,					'%деряйне ьюлоюмяйне%',		'',					'опндсйрш',		'мюохрйх',			'деряйне ьюлоюмяйне',			'',					''),
			(1,					'%лнпя%',	'%лнпяйюъ%йюосярю%',				'опндсйрш',		'мюохрйх',			'лнпя',							'',					''),
			(1,					'янй %',					'',					'опндсйрш',		'мюохрйх',			'янй',							'',					''),
			(1,					'мейрюп %',					'',					'опндсйрш',		'мюохрйх',			'мейрюп',						'',					''),
			(1,					'йнлонр %',					'',					'опндсйрш',		'мюохрйх',			'йнлонр',						'',					''),
			(2,					'%бндю%',		'%[ю-ъ]бндю%',					'опндсйрш',		'мюохрйх',			'бндю',							'',					''),

			(1,					'%укеа%',			'%вхояш%',					'опндсйрш',		'укеанаскнвмше хгдекхъ/бшоевйю',	'укеа',			'',					''),
			(1,					'%охпнц %',				   '',					'опндсйрш',		'укеанаскнвмше хгдекхъ/бшоевйю',	'охпнц',		'',					''),
			(1,					'%аюцер%',				   '',					'опндсйрш',		'укеанаскнвмше хгдекхъ/бшоевйю',	'аюцер',		'',					''),
			(1,					'кюбюь %',				   '',					'опндсйрш',		'укеанаскнвмше хгдекхъ/бшоевйю',	'кюбюь',		'',					''),
			(1,					'аскнвйю%',				   '',					'опндсйрш',		'укеанаскнвмше хгдекхъ/бшоевйю',	'аскнвйю',		'',					''),
			(1,					'яднаю%',				   '',					'опндсйрш',		'укеанаскнвмше хгдекхъ/бшоевйю',	'аскнвйю',		'',					''),
			(1,					'%кеоеьйю%',		       '',					'опндсйрш',		'укеанаскнвмше хгдекхъ/бшоевйю',	'кеоеьйю',		'',					''),
			(1,					'%аюрнм%',			'%аюрнмвхй%',				'опндсйрш',		'укеанаскнвмше хгдекхъ/бшоевйю',	'укеа',			'',					''),
						
			(1,					'оептейр тхр%',				'',					'опндсйрш',		'охрюмхе дкъ фхбнрмшу',		'охрюмхе дкъ фхбнрмшу',	'',					''),	


			(1,					'йсйспсгю %',				 '',					'опндсйрш',		'нбных/гекемэ',	'йсйспсгю',						'',					''),
			(1,					'гекемши цнпньей %',		 '',					'опндсйрш',		'нбных/гекемэ',	'гекемши цнпньей',				'',					''),
			(1,					'%юяянпрх нбнымне%',		 '',					'опндсйрш',		'нбных/гекемэ',	'юяянпрх нбнымне',				'',					''),
			(1,					'нцспжш%',		 'нцспжш%янке%',					'опндсйрш',		'нбных/гекемэ',	'нцспжш',						'',					''),
			(1,					'%йюосярю%',				 '',					'опндсйрш',		'нбных/гекемэ',	'йюосярю',						'',					''),
			(1,					'онлхднп%',					 '',					'опндсйрш',		'нбных/гекемэ',	'онлхднпш',						'',					''),
			(1,					'йюаювйх%',					 '',					'опндсйрш',		'нбных/гекемэ',	'йюаювйх',						'',					''),
			(1,					'оепеж%',				'%лнкнр%',					'опндсйрш',		'нбных/гекемэ',	'оепеж',						'',					''),
			(1,					'%лнпйнбэ%',				 '',					'опндсйрш',		'нбных/гекемэ',	'лнпйнбэ',						'',					''),
			(2,					'йюпрнтекэ %',				 '',					'опндсйрш',		'нбных/гекемэ',	'йсйспсгю',						'',					''),
			(1,					'ксй%',						 '',					'опндсйрш',		'нбных/гекемэ',	'ксй',						'',					''),
			(1,					'аюйкюфюмш%',				 '',					'опндсйрш',		'нбных/гекемэ',	'аюйкюфюмш',						'',					''),
			(1,					'педхя%',					'',						'опндсйрш',		'нбных/гекемэ',	'педхя',						'',					''),
			(1,					'ябейкю%',					 '',					'опндсйрш',		'нбных/гекемэ',	'ябейкю',						'',					''),
			(1,					'гекемэ %',					 '',					'опндсйрш',		'нбных/гекемэ',	'гекемэ',						'',					''),
			(1,					'веямнй%',					'',						'опндсйрш',		'нбных/гекемэ',	'веямнй',						'',					''),
			(1,					'мюанп гекемх%',			 '',					'опндсйрш',		'нбных/гекемэ',	'мюанп гекемх',					'',					''),
			(1,					'яюкюр%б%цнпьнвйе%',		'',						'опндсйрш',		'нбных/гекемэ',	'яюкюр',						'',					''),
			(1,					'яюкюр%юияаепц%',			'',						'опндсйрш',		'нбных/гекемэ',	'яюкюр',						'',					''),
			(1,					'яюкюр%псййнкю%',			'',						'опндсйрш',		'нбных/гекемэ',	'яюкюр',						'',					''),
			(1,					'яюкюр%лхйя%',				'',						'опндсйрш',		'нбных/гекемэ',	'яюкюр',						'',					''),
			(1,					'тюянкэ%',					'',						'опндсйрш',		'нбных/гекемэ',	'тюянкэ',						'',					''),
			(1,					'сйпно%',					'',						'опндсйрш',		'нбных/гекемэ',	'сйпно',						'',					''),
			(1,					'оерпсьйю%',				'',						'опндсйрш',		'нбных/гекемэ',	'оерпсьйю',						'',					''),
			(1,					'рнлюрш%',					'',						'опндсйрш',		'нбных/гекемэ',	'рнлюрш',						'',					''),
						
			(2,					'яюкюр%',				 '%яюкюрмхй%',				'опндсйрш',		'йскхмюпхъ/онкстюапхйюрш',			'яюкюр',		'',					''),
			(1,					'%йнркер%',					 '',					'опндсйрш',		'йскхмюпхъ/онкстюапхйюрш',			'йнркерю',		'',					''),
			(1,					'йнркерю%',					 '',					'опндсйрш',		'йскхмюпхъ/онкстюапхйюрш',			'йнркерю',		'',					''),
			(1,					'%яюпдекэйх%',				 '',					'опндсйрш',		'йскхмюпхъ/онкстюапхйюрш',			'яюпдекэйх',	'',					''),
			(1,					'аскэнм %',					 '',					'опндсйрш',		'йскхмюпхъ/онкстюапхйюрш',			'аскэнм',		'',					''),
			(1,					'%цпемйх%',					 '',					'опндсйрш',		'йскхмюпхъ/онкстюапхйюрш',			'цпемйх',		'',					''),
			(1,					'яепбекюр %',				 '',					'опндсйрш',		'йскхмюпхъ/онкстюапхйюрш',			'яепбекюр',		'',					''),
			(1,					'оекэлемх %',				 '',					'опндсйрш',		'йскхмюпхъ/онкстюапхйюрш',			'оекэлемх',		'',					''),
			(1,					'ьохйювйх %',				 '',					'опндсйрш',		'йскхмюпхъ/онкстюапхйюрш',			'ьохйювйх',		'',					''),
			(1,					'ункндеж %',				 '',					'опндсйрш',		'йскхмюпхъ/онкстюапхйюрш',			'ункндеж',		'',					''),
			(1,					'аейнм %',					 '',					'опндсйрш',		'йскхмюпхъ/онкстюапхйюрш',			'аейнм',		'',					''),
			(1,					'%цскъь%',					 '',					'опндсйрш',		'йскхмюпхъ/онкстюапхйюрш',			'цскъь',		'',					''),
			(1,					'цскъь %',					 '',					'опндсйрш',		'йскхмюпхъ/онкстюапхйюрш',			'цскъь',		'',					''),
			(1,					'яюкдекэйх %',				 '',					'опндсйрш',		'йскхмюпхъ/онкстюапхйюрш',			'яюкдекэйх',		'',					''),
			(1,					'%оюьрер%',					 '',					'опндсйрш',		'йскхмюпхъ/онкстюапхйюрш',			'оюьрер',		'',					''),
			(1,					'%янкъмй%',					 '',					'опндсйрш',		'йскхмюпхъ/онкстюапхйюрш',			'янкъмйю',		'',					''),
			(1,					'%оюкнвйх%йспхмше%',		 '',					'опндсйрш',		'йскхмюпхъ/онкстюапхйюрш',			'йспхмше оюкнвйх','',				''),
			(1,					'люмрш %',					 '',					'опндсйрш',		'йскхмюпхъ/онкстюапхйюрш',			'люмрш',		'',					''),
			(1,					'%бюпемхйх%',				 '',					'опндсйрш',		'йскхмюпхъ/онкстюапхйюрш',			'бюпемхйх',		'',					''),
			(1,					'%йновем%',					 '',					'опндсйрш',		'йскхмюпхъ/онкстюапхйюрш',			'цнрнбне акчдн',		'',					''),
			(2,					'%цнбъдхмю%',				 '',					'опндсйрш',		'йскхмюпхъ/онкстюапхйюрш',			'цнбъдхмю',		'',					''),
			(1,					'%йнкаюя%',					 '',					'опндсйрш',		'йскхмюпхъ/онкстюапхйюрш',			'йнкаюяю',		'',					''),
			(1,					'яюкълх%',					 '',					'опндсйрш',		'йскхмюпхъ/онкстюапхйюрш',			'йнкаюяю',		'',					''),
			(1,					'бервхмю%',					 '',					'опндсйрш',		'йскхмюпхъ/онкстюапхйюрш',			'бервхмю',		'',					''),
			(1,					'%яняхяй%',					 '',					'опндсйрш',		'йскхмюпхъ/онкстюапхйюрш',			'яняхяйх',		'',					''),
			(2,					'%ясо%',					 '',					'опндсйрш',		'йскхмюпхъ/онкстюапхйюрш',			'ясо',			'',					''),
			(1,					'%рсьемюъ%',				 '',					'опндсйрш',		'йскхмюпхъ/онкстюапхйюрш',			'цнрнбне акчдн','',					''),
			(1,					'%веасоеккх%',				 '',					'опндсйрш',		'йскхмюпхъ/онкстюапхйюрш',			'веасоеккх',	'',					''),
			(1,					'%ондфюпйю%',				 '',					'опндсйрш',		'йскхмюпхъ/онкстюапхйюрш',			'ондфюпйю',		'',					''),
			(1,					'%тюпь%',					 '',					'опндсйрш',		'йскхмюпхъ/онкстюапхйюрш',			'тюпь',			'',					''),
			(1,					'%лнпйнбэ%он%йнпеи%',		 '',					'опндсйрш',		'йскхмюпхъ/онкстюапхйюрш',			'лнпйнбэ он-йнпеиЯЙХ',	'',			''),
			(1,					'%уе %',					 '',					'опндсйрш',		'йскхмюпхъ/онкстюапхйюрш',			'уе',			'',					''),
			(1,					'%бюпемш%',					 '',					'опндсйрш',		'йскхмюпхъ/онкстюапхйюрш',			'цнрнбне акчдн','',					''),
			(1,					'%бюпемю%',					 '',					'опндсйрш',		'йскхмюпхъ/онкстюапхйюрш',			'цнрнбне акчдн','',					''),
			(1,					'%оевемш%',					 '',					'опндсйрш',		'йскхмюпхъ/онкстюапхйюрш',			'цнрнбне акчдн','',					''),
			(1,					'%гюоевем%',				 '',					'опндсйрш',		'йскхмюпхъ/онкстюапхйюрш',			'цнрнбне акчдн','',					''),
			(1,					'%оевемю%',					 '',					'опндсйрш',		'йскхмюпхъ/онкстюапхйюрш',			'цнрнбне акчдн','',					''),
			(1,					'%фюпемш%',					 '',					'опндсйрш',		'йскхмюпхъ/онкстюапхйюрш',			'цнрнбне акчдн','',					''),
			(1,					'%фюпемю%',					 '',					'опндсйрш',		'йскхмюпхъ/онкстюапхйюрш',			'цнрнбне акчдн','',					''),
			(1,					'%пюцс%',					 '',					'опндсйрш',		'йскхмюпхъ/онкстюапхйюрш',			'пюцс',			'',					''),
			(1,					'%акхмвхйх%',				 '',					'опндсйрш',		'йскхмюпхъ/онкстюапхйюрш',			'акхмвхйх',		'',					''),
			(1,					'реярн%',					 '',					'опндсйрш',		'йскхмюпхъ/онкстюапхйюрш',			'реярн',		'',					''),
			(1,					'%он-днлюьмелс%',			 '',					'опндсйрш',		'йскхмюпхъ/онкстюапхйюрш',			'цнрнбне акчдн','',					''),
			(1,					'ящмдбхв%',					 '',					'опндсйрш',		'йскхмюпхъ/онкстюапхйюрш',			'ящмдбхв',		'',					''),
			(1,					'унр%днц%',					 '',					'опндсйрш',		'йскхмюпхъ/онкстюапхйюрш',			'унр-днц',		'',					''),
			(1,					'окнб%',					 '',					'опндсйрш',		'йскхмюпхъ/онкстюапхйюрш',			'окнб',			'',					''),
			(1,					'%яюкюр%йюосярю%',			'',						'опндсйрш',		'йскхмюпхъ/онкстюапхйюрш',			'яюкюр',		'',					''),
			(1,					'нкюдэх%',					'',						'опндсйрш',		'йскхмюпхъ/онкстюапхйюрш',			'нкюдэх',		'',					''),
			(1,					'%йюосярю%яюкюр%',			'',						'опндсйрш',		'йскхмюпхъ/онкстюапхйюрш',			'яюкюр',		'',					''),
			
			(1,					'%люплекюд%',				'',						'опндсйрш',		'йнмдхрепяйхе хгдекхъ/якюднярх',	'люплекюд',		'',					''),
			(1,					'%ьнйнкюд%',				'',						'опндсйрш',		'йнмдхрепяйхе хгдекхъ/якюднярх',	'ьнйнкюд',		'',					''),
			(1,					'%йнмтерш%',				'',						'опндсйрш',		'йнмдхрепяйхе хгдекхъ/якюднярх',	'йнмтерю',		'',					''),
			(1,					'йнмтерю %',				'',						'опндсйрш',		'йнмдхрепяйхе хгдекхъ/якюднярх',	'йнмтерю',		'',					''),
						
			(1,					'гетхп %',					'',						'опндсйрш',		'йнмдхрепяйхе хгдекхъ/якюднярх',	'гетхп',		'',					''),
			(1,					'рнпр %',					'',						'опндсйрш',		'йнмдхрепяйхе хгдекхъ/якюднярх',	'рнпр',			'',					''),
			(2,					'деяепр юмрнмнб%',				'',						'опндсйрш',		'йнмдхрепяйхе хгдекхъ/якюднярх',	'деяепр',		'',					''),
			(2,					'%я хгчлнл%',				'',						'опндсйрш',		'йнмдхрепяйхе хгдекхъ/якюднярх',	'деяепр',		'',					''),
			(2,					'%я люйнл%',				'',						'опндсйрш',		'йнмдхрепяйхе хгдекхъ/якюднярх',	'деяепр',		'',					''),
			(1,					'охпнфмне %',				'',						'опндсйрш',		'йнмдхрепяйхе хгдекхъ/якюднярх',	'охпнфмне',		'',					''),
			(1,					'дпюфе %',					'',						'опндсйрш',		'йнмдхрепяйхе хгдекхъ/якюднярх',	'йнмтерю',		'',					''),
			(1,					'йскхв %',					'',						'опндсйрш',		'йнмдхрепяйхе хгдекхъ/якюднярх',	'йскхв',		'',					''),
			(1,					'окчьйю %',					'',						'опндсйрш',		'йнмдхрепяйхе хгдекхъ/якюднярх',	'окчьйю',		'',					''),
			(1,					'оюярхкю %',				'',						'опндсйрш',		'йнмдхрепяйхе хгдекхъ/якюднярх',	'оюярхкю',		'',					''),
			(1,					'йюпюлекэ %',				'',						'опндсйрш',		'йнмдхрепяйхе хгдекхъ/якюднярх',	'йскхв',		'',					''),
			(1,					'%аюрнмвхй%',				'',						'опндсйрш',		'йнмдхрепяйхе хгдекхъ/якюднярх',	'йюпюлекэ',		'',					''),
			(1,					'%целюрнцем%',				'',						'опндсйрш',		'йнмдхрепяйхе хгдекхъ/якюднярх',	'целюрнцем',	'',					''),
			(1,					'%кедемжш%',				'',						'опндсйрш',		'йнмдхрепяйхе хгдекхъ/якюднярх',	'йюпюлекэ',		'',					''),
			(1,					'%феб.йнмтерю%',			'',						'опндсйрш',		'йнмдхрепяйхе хгдекхъ/якюднярх',	'йнмтерю',		'',					''),
			(1,					'%мюанп%йнмтер%',			'',						'опндсйрш',		'йнмдхрепяйхе хгдекхъ/якюднярх',	'йнмтерю',		'',					''),
			(1,					'%бютек%',					'%онкнремже%',			'опндсйрш',		'йнмдхрепяйхе хгдекхъ/якюднярх',	'бюткх',		'',					''),
			(1,					'%бюткх%',					'%онкнремже%',			'опндсйрш',		'йнмдхрепяйхе хгдекхъ/якюднярх',	'бюткх',		'',					''),
			(1,					'йпейеп %',					'',						'опндсйрш',		'йнмдхрепяйхе хгдекхъ/якюднярх',	'йпейеп',		'',					''),
			(1,					'оевемэе %',				'',						'опндсйрш',		'йнмдхрепяйхе хгдекхъ/якюднярх',	'оевемэе',		'',					''),
			(1,					'%вюй-вюй%',				'',						'опндсйрш',		'йнмдхрепяйхе хгдекхъ/якюднярх',	'оевемэе',		'',					''),
			(1,					'оевемэе%пнцюкхй%',			'',						'опндсйрш',		'йнмдхрепяйхе хгдекхъ/якюднярх',	'оевемэе',		'',					''),
			(1,					'йпемдекэйх %',				'',						'опндсйрш',		'йнмдхрепяйхе хгдекхъ/якюднярх',	'оевемэе',		'',					''),
			(1,					'%опъмхй%',					'',						'опндсйрш',		'йнмдхрепяйхе хгдекхъ/якюднярх',	'опъмхй',		'',					''),
			(1,					'%аюпюмй%',					'',						'опндсйрш',		'йнмдхрепяйхе хгдекхъ/якюднярх',	'аюпюмйх',		'',					''),
			(1,					'%всою-всоя%',				'',						'опндсйрш',		'йнмдхрепяйхе хгдекхъ/якюднярх',	'йюпюлекэ',		'',					''),
			(1,					'%ыепаер%',					'',						'опндсйрш',		'йнмдхрепяйхе хгдекхъ/якюднярх',	'ыепаер',		'',					''),
			(1,					'%внйн%оюи%',				'',						'опндсйрш',		'йнмдхрепяйхе хгдекхъ/якюднярх',	'йнмтерю',		'',					''),
			(1,					'уюкбю%',					'',						'опндсйрш',		'йнмдхрепяйхе хгдекхъ/якюднярх',	'уюкбю',		'',					''),
			(1,					'пскер%ъьйхмн%',			'',						'опндсйрш',		'йнмдхрепяйхе хгдекхъ/якюднярх',	'пскер',		'',					'ъьйхмн'),
			(1,					'%йнгхмюй%',				'',						'опндсйрш',		'йнмдхрепяйхе хгдекхъ/якюднярх',	'йнгхмюй',		'',					''),
			(1,					'хпхя%',					'',						'опндсйрш',		'йнмдхрепяйхе хгдекхъ/якюднярх',	'хпхя',			'',					''),
			(1,					'%ъцндю опнрепрюъ%',		'',						'опндсйрш',		'йнмдхрепяйхе хгдекхъ/якюднярх',	'деяепр',		'',					''),
			(2,					'%йейя%',			'%ялеяэ д%' ,					'опндсйрш',		'йнмдхрепяйхе хгдекхъ/якюднярх',	'йейя',			'',					''),

			(1,					'%опхопюбю%',				'',						'опндсйрш',		'аюйюкеъ',							'опхопюбю',		'',					''),
			(1,					'яюуюп%',			'яюуюпмхжю%',					'опндсйрш',		'аюйюкеъ',							'яюуюп',		'',					''),
			(1,					'йпюулюк%',					'',					'опндсйрш',		'аюйюкеъ',								'йпюулюк',		'',					''),
			(1,					'%лсйю%',					'',						'опндсйрш',		'аюйюкеъ',							'лсйю',			'',					''),
			(1,					'%дпнффх%',					'',						'опндсйрш',		'аюйюкеъ',							'дпнффх',		'',					''),
			(1,					'%йпсою%',					'',						'опндсйрш',		'аюйюкеъ',							'йпсою',		'',					''),
			(1,					'вюи %',					'',						'опндсйрш',		'аюйюкеъ',							'вюи',			'',					''),
			(1,					'%люйюпнм%',				'',						'опндсйрш',		'аюйюкеъ',							'люйюпнмШ',		'',					''),
			(1,					'йнте %',					'',					'опндсйрш',		'аюйюкеъ',							'йнте',			'',					''),
			(1,					'яелевйх%',					'',						'опндсйрш',		'аюйюкеъ',							'яелевйх',		'',					''),
			(1,					'оепеж%лнкнр%',				'',						'опндсйрш',		'аюйюкеъ',							'оепеж лнкнрши','',					''),
			(1,					'йюйюн%',					'йюйюн яцсы%',			'опндсйрш',		'аюйюкеъ',							'йюйюн',		'',					''),
			(1,					'цнр. гюбрпюй%',			'',						'опндсйрш',		'аюйюкеъ',							'укноэъ',		'',					''),
			(1,					'фекюрхм%',					'',						'опндсйрш',		'аюйюкеъ',							'фекюрхм',		'',					''),
			(1,					'ялеяэ люццх%',	'',						'опндсйрш',		'аюйюкеъ',		'ялеяэ дкъ ашярпнцн опхцнрнбкемхъ',	'',					''),
			(1,					'йюпрнтекэмне очпе а/о%',	'',						'опндсйрш',		'аюйюкеъ',		'ялеяэ дкъ ашярпнцн опхцнрнбкемхъ',	'',					''),
			(1,					'йюью а/о%',				'',						'опндсйрш',		'аюйюкеъ',		'ялеяэ дкъ ашярпнцн опхцнрнбкемхъ',	'',					''),
			(1,					'кюоью а/о%',				'',						'опндсйрш',		'аюйюкеъ',		'ялеяэ дкъ ашярпнцн опхцнрнбкемхъ',	'',					''),
			(1,					'ясо а/о%',					'',						'опндсйрш',		'аюйюкеъ',		'ялеяэ дкъ ашярпнцн опхцнрнбкемхъ',	'',					''),
			(1,					'укноэъ%',					'',						'опндсйрш',		'аюйюкеъ',							'укноэъ',		'',					''),
			(1,					'янкэ %',					'',						'опндсйрш',		'аюйюкеъ',							'янкэ',			'',					''),
			(1,					'йсйсп.оюкнвйх%',			'',						'опндсйрш',		'аюйюкеъ',							'укноэъ',		'',					''),
			(1,					'%жхйнпхи%',				'',						'опндсйрш',		'аюйюкеъ',							'жхйнпхи',		'',					''),
			(1,					'яндю%',					'',						'опндсйрш',		'аюйюкеъ',							'яндю',			'',					''),
			(1,					'%ялеяэ%ймнпп%',			'',						'опндсйрш',		'аюйюкеъ',							'ялеяэ дкъ ашярпнцн опхцнрнбкемхъ',	'',	''),
			(1,					'ясьйю%',					'',						'опндсйрш',		'аюйюкеъ',							'ясьйю',			'',					''),
			(1,					'ясуюпхйх%',				'',						'опндсйрш',		'аюйюкеъ',							'ясуюпхйх',			'',					''),
			(1,					'якюияш%',					'',						'опндсйрш',		'аюйюкеъ',							'якюияш',			'',					''),
			(1,					'%вхояш%',					'',						'опндсйрш',		'аюйюкеъ',							'вхояш',			'',					''),
			(1,					'оно%йнпм%',				'',						'опндсйрш',		'аюйюкеъ',							'оно йнпм',			'',					''),

			(2,					'люякн %',					 '%дфнмянмя ащах%',		'опндсйрш',		'охыебше фхпш',						'люякн',			'',					''),			
			(1,					'люпцюпхм %',				'',						'опндсйрш',		'охыебше фхпш',						'люпцюпхм',			'',					''),			
			(1,					'яопед%',					'',						'опндсйрш',		'охыебше фхпш',						'яопед',			'',					''),
			
			(1,					'%онкнремже%',				'',						'рнбюпш',		'рнбюпш дкъ днлю х дювх',			'онкнремже',		'',					''),
			(1,					'%яелемю%',					'',						'рнбюпш',		'рнбюпш дкъ днлю х дювх',			'яелемю',			'',					''),
			(1,					'%янбнй%',					'',						'рнбюпш',		'рнбюпш дкъ днлю х дювх',			'янбнй',			'',					''),
			(1,					'%цпюакх%',				'%мюанп дкъ оеяйю%',		'рнбюпш',		'рнбюпш дкъ днлю х дювх',			'цпюакх',			'',					''),
			(1,					'%псйюб%гюоейюмхъ%',		'',						'рнбюпш',		'рнбюпш дкъ днлю х дювх',			'псйюб дкъ гюоейюмхъ',	'',				''),
			(1,					'%дкъ%упюмемхъ%',			'',						'рнбюпш',		'рнбюпш дкъ днлю х дювх',			'дкъ упюмемхъ',		'',					''),
			(1,					'%яюкт%',					'',						'рнбюпш',		'рнбюпш дкъ днлю х дювх',			'яюктерйх',			'',					''),
			(1,					'%д%хмярпслем%',			'',						'рнбюпш',		'рнбюпш дкъ днлю х дювх',			'упюмемхе',		'',					''),
			(1,					'%бепусьйю%',				'',						'рнбюпш',		'рнбюпш дкъ днлю х дювх',			'екнвмюъ хцпсьйю',	'',					''),
			(1,					'%мюанп дкъ йнмяепбхпнбюмхъ%',	'',					'рнбюпш',		'рнбюпш дкъ днлю х дювх',			'мюанп дкъ йнмяепбхпнбюмхъ',	'',					''),
			(1,					'%д%лшрэъ%',				'',						'рнбюпш',		'рнбюпш дкъ днлю х дювх',			'дкъ лшрэъ онясдш',	'',					''),
			(1,					'%вхяр%яп%',				'',						'рнбюпш',		'рнбюпш дкъ днлю х дювх',			'ашрнбюъ ухлхъ',	'вхяръыее япедярбн',					''),
			(1,					'%д%ярхпйх%',				'',						'рнбюпш',		'рнбюпш дкъ днлю х дювх',			'дкъ ярхпйх',	'',					''),
			(1,					'%ярхп%онпнь%',				'',						'рнбюпш',		'рнбюпш дкъ днлю х дювх',			'дкъ ярхпйх',	'',					''),
			(1,					'%юпнлюрхгюрнп%',			'',						'рнбюпш',		'рнбюпш дкъ днлю х дювх',			'ашрнбюъ ухлхъ',	'юпнлюрхгюрнп',					''),
			(1,					'%репйю%',					'',						'рнбюпш',		'рнбюпш дкъ днлю х дювх',			'репйю',	'',						''),
			(1,					'%яюкюрмхй%',				'',						'рнбюпш',		'рнбюпш дкъ днлю х дювх',			'яюкюрмхй',	'',						''),
			(1,					'%бюгю%',					'',						'рнбюпш',		'рнбюпш дкъ днлю х дювх',			'бюгю',	'',							''),
			(1,					'%цсайю%',					'',						'рнбюпш',		'рнбюпш дкъ днлю х дювх',			'цсайю',	'',						''),
			(1,					'%асршкйю%йпшьй%',			'',						'рнбюпш',		'рнбюпш дкъ днлю х дювх',			'онясдю',	'',						''),
			(1,					'%аюрюпеъ яюкчрнб%',		'',						'рнбюпш',		'рнбюпш дкъ днлю х дювх',			'тсиепбепй',	'',						''),
			(1,					'%сдна%',					'',						'рнбюпш',		'рнбюпш дкъ днлю х дювх',			'сднапемхъ',	'',						''),
			(1,					'%яюуюпмхжю%',				'',						'рнбюпш',		'рнбюпш дкъ днлю х дювх',			'онясдю',	'',						''),
			(1,					'%оюйер%люийю%',			'',						'рнбюпш',		'рнбюпш дкъ днлю х дювх',			'оюйер',	'',						''),
			(1,					'%оюйер%тюянбн%',			'',						'рнбюпш',		'рнбюпш дкъ днлю х дювх',			'оюйер',	'',						''),
			(1,					'%оюйер%д%гюл%',			'',						'рнбюпш',		'рнбюпш дкъ днлю х дювх',			'оюйер',	'',						''),
			(1,					'%оюйеер%д%',				'',						'рнбюпш',		'рнбюпш дкъ днлю х дювх',			'оюйер',	'',						''),
			(1,					'%асршкйю ощр%',			'',						'рнбюпш',		'рнбюпш дкъ днлю х дювх',			'онясдю',	'',						''),
			(1,					'%оепвюрйх%дкъ%',			'',						'рнбюпш',		'рнбюпш дкъ днлю х дювх',			'оепвюрйх',	'',						''),
			(1,					'жберш%',					'',						'рнбюпш',		'рнбюпш дкъ днлю х дювх',			'жберш',	'',						''),
			(1,					'бхкйх%',					'',						'рнбюпш',		'рнбюпш дкъ днлю х дювх',			'бхкйх',	'',						''),
			(1,					'окюрйх%',					'',						'рнбюпш',		'рнбюпш дкъ днлю х дювх',			'окюрйх',	'',						''),
			(1,					'%тнплю д%',				'',						'рнбюпш',		'рнбюпш дкъ днлю х дювх',			'йсунммши хмбемрюпэ',	'',			''),
			(1,					'%леьнй дкъ%',				'',						'рнбюпш',		'рнбюпш дкъ днлю х дювх',			'упюмемхе',	'',					''),
			(1,					'%леьйх дкъ%',				'',						'рнбюпш',		'рнбюпш дкъ днлю х дювх',			'упюмемхе',	'',					''),
			(1,					'%йнмреимеп%',				'',						'рнбюпш',		'рнбюпш дкъ днлю х дювх',			'упюмемхе',	'',					''),
			(1,					'%ы╗рйю%',					'',						'рнбюпш',		'рнбюпш дкъ днлю х дювх',			'ы╗рйю',	'',					''),
			(1,					'%сцнкэ%',					'',						'рнбюпш',		'рнбюпш дкъ днлю х дювх',			'сцнкэ',	'',					''),
			(1,					'%юмрхярюрхй%',				'',						'рнбюпш',		'рнбюпш дкъ днлю х дювх',			'ашрнбюъ ухлхъ',	'юмрхярюрхй',					''),
			(1,					'%оепвюрйх пегхмнбше%',		'',						'рнбюпш',		'рнбюпш дкъ днлю х дювх',			'оепвюрйх',	'',					''),
			(1,					'дхяйх%',					'',						'рнбюпш',		'рнбюпш дкъ днлю х дювх',			'дхяйх',	'',					''),
			(1,					'%кюлою%',					'',						'рнбюпш',		'рнбюпш дкъ днлю х дювх',			'кюлою',	'',					''),
			(1,					'гелкъ%',					'',						'рнбюпш',		'рнбюпш дкъ днлю х дювх',			'гелкъ',	'',					''),
			(1,					'яохвйх%',					'',						'рнбюпш',		'рнбюпш дкъ днлю х дювх',			'яохвйх',	'',					''),
			(1,					'рюпекйю%',					'',						'рнбюпш',		'рнбюпш дкъ днлю х дювх',			'рюпекйю',	'',					''),
			(1,					'%нябефхрекэ%',				'',						'рнбюпш',		'рнбюпш дкъ днлю х дювх',			'ашрнбюъ ухлхъ','',					''),

			(1,					'%лшкн%',					'',						'рнбюпш',		'йнялерхйю/цхцхемю',				'лшкн',			'',					''),
			(1,					'%аюкэгюл%',				'',						'рнбюпш',		'йнялерхйю/цхцхемю',				'аюкэгюл','',					''),	
			(1,					'%ьюлосмэ%',				'',						'рнбюпш',		'йнялерхйю/цхцхемю',				'ьюлосмэ','',					''),	
			(1,					'%цекэ%',					'',						'рнбюпш',		'йнялерхйю/цхцхемю',				'цекэ','',							''),
			(1,					'йпел%д/п%',				'',						'рнбюпш',		'йнялерхйю/цхцхемю',				'йпел дкъ псй','',							''),		
			(1,					'%опнйкюдйх%',				'',						'рнбюпш',		'йнялерхйю/цхцхемю',				'опнйкюдйх','',					''),	
			(1,					'%оюкнвйх бюрмше%',			'',						'рнбюпш',		'йнялерхйю/цхцхемю',				'оюкнвйх бюрмше','',		''),	
			(1,					'%ярюмнй%',					'',						'рнбюпш',		'йнялерхйю/цхцхемю',				'ярюмнй','',						''),	
			(1,					'%дегнднп%',				'',						'рнбюпш',		'йнялерхйю/цхцхемю',				'дегнднпюмр','',				''),	
			(1,					'%рсюк%асл%',				'',						'рнбюпш',		'йнялерхйю/цхцхемю',				'рсюкермюъ аслюцю','',			''),	
			(1,					'%люяйю %',				'%йюпмюб%',					'рнбюпш',		'йнялерхйю/цхцхемю',				'люяйю','',					''),	
			(1,					'%яйпюа%',					'',						'рнбюпш',		'йнялерхйю/цхцхемю',				'йнялерхвеяйне япедярбн','',		''),	
			(1,					'%ткнпеяюм%',				'',						'рнбюпш',		'йнялерхйю/цхцхемю',				'йнялерхвеяйне япедярбн','',	''),	
			(1,					'люякн %д%гюцюпю%',			'',						'рнбюпш',		'йнялерхйю/цхцхемю',				'йнялерхвеяйне япедярбн','',''),	
			(1,					'яп-бн д%',					'',						'рнбюпш',		'йнялерхйю/цхцхемю',				'йнялерхвеяйне япедярбн','',		''),	
			(1,					'%фхдйнярэ%',				'',						'рнбюпш',		'йнялерхйю/цхцхемю',				'фхдйнярэ','',					''),	
			(1,					'г / ы %',					'',						'рнбюпш',		'йнялерхйю/цхцхемю',				'гсамюъ ыерйю','',					''),
			(1,					'г/ы %',					'',						'рнбюпш',		'йнялерхйю/цхцхемю',				'гсамюъ ыерйю','',					''),	
			(1,					'г / о %',					'',						'рнбюпш',		'йнялерхйю/цхцхемю',				'гсамюъ оюярю','',					''),
			(1,					'г/о %',					'',						'рнбюпш',		'йнялерхйю/цхцхемю',				'гсамюъ оюярю','',					''),


			(1,					'%йюпюмдюь%',				'',						'рнбюпш',		'йнмжекъпхъ',						'йюпюмдюь','',					''),
			(1,					'%ймхцю дкъ гюохяеи%',		'',						'рнбюпш',		'йнмжекъпхъ',						'ймхцю дкъ гюохяеи','',					''),
			(1,					'%накнфйю%',				'',						'рнбюпш',		'йнмжекъпхъ',						'накнфйю','',					''),
			(1,					'%рерпюдэ%',				'',						'рнбюпш',		'йнмжекъпхъ',						'рерпюдэ','',					''),
			(1,					'%псвйю%',					'',						'рнбюпш',		'йнмжекъпхъ',						'псвйю','',					''),
			(1,					'%пхянбюмхъ%',				'',						'рнбюпш',		'йнмжекъпхъ',						'дкъ пхянбюмхъ','',					''),
			(1,					'%йюпрнм%',					'',						'рнбюпш',		'йнмжекъпхъ',						'йюпрнм','',					''),
			(1,					'%ймхфйю%',					'',						'рнбюпш',		'йнмжекъпхъ',						'ймхфйю','',					''),
			(1,					'%йкеи%',					'',						'рнбюпш',		'йнмжекъпхъ',						'йкеи','',					''),
			(1,					'%йюпрпхдф%',				'',						'рнбюпш',		'йнмжекъпхъ',						'йюпрпхдф','',					''),
			(1,					'%акнймнр%',				'',						'рнбюпш',		'йнмжекъпхъ',						'акнймнр','',					''),
			(1,					'%юйбюпекэ%',				'',						'рнбюпш',		'йнмжекъпхъ',						'юйбюпекэ','',					''),
			(1,					'%кюярхй%',					'%[ю_ъ]кюярхй%',		'рнбюпш',		'йнмжекъпхъ',						'кюярхй','',					''),
			(1,					'%юкэанл%',					'',						'рнбюпш',		'йнмжекъпхъ',						'юкэанл','',					''),
			(1,					'%аслюцю%',					'',						'рнбюпш',		'йнмжекъпхъ',						'аслюцю','',					''),
			(1,					'%рнвхкйю%',				'',						'рнбюпш',		'йнмжекъпхъ',						'рнвхкйю','',					''),
			(1,					'%нрйпшрйю%',				'',						'рнбюпш',		'йнмжекъпхъ',						'нрйпшрйю','',					''),
			
			(1,					'%цюгерю%',					'',						'рнбюпш',		'ймхцх/фспмюкш',					'цюгерю','',					''),
			(1,					'%фспмюк%',					'',						'рнбюпш',		'ймхцх/фспмюкш',					'фспмюк','',					''),
			(1,					'%ймхцю%',					'',						'рнбюпш',		'ймхцх/фспмюкш',					'ймхцю','',					''),
			(1,					'%яйюмбнпд%',				'',						'рнбюпш',		'ймхцх/фспмюкш',					'яйюмбнпд','',					''),
			
			(1,					'%мюанп дкъ оеяйю%',		'',						'рнбюпш',		'рнбюпш дкъ дереи',					'хцпсьйх','',					''),
			(1,					'%яня%осяршь%',				'',						'рнбюпш',		'рнбюпш дкъ дереи',					'яняйх\осяршьйх','',					''),
			(1,					'%хцпсьйю%',				'',						'рнбюпш',		'рнбюпш дкъ дереи',					'хцпсьйх','',					''),
			(1,					'йпел деряйхи%',			'',						'рнбюпш',		'рнбюпш дкъ дереи',					'сунд гю деряйни йнфеи','',					''),
			(1,					'%дфнмянмя ащах%',			'',						'рнбюпш',		'рнбюпш дкъ дереи',					'сунд гю деряйни йнфеи','',					''),
			(1,					'%ондцсгмхйх%',				'',						'рнбюпш',		'рнбюпш дкъ дереи',					'ондцсгмхйх','',				''),	
			
			(1,					'%мюонкмхрекэ%',			'',						'рнбюпш',		'рнбюпш дкъ фхбнрмшу',				'мюонкмхрекэ','',					''),
			
			(1,					'%ондюпнвмюъ йюпрю%',		'',						'рнбюпш',		'ондюпйх',							'ондюпнвмюъ йюпрю','',					''),
			(1,					'%%оюйер ондюп%',			'',						'рнбюпш',		'ондюпйх',							'ондюпнвмши оюйер','',					''),
			(1,					'%оюйер%ондюп%',			'',						'рнбюпш',		'ондюпйх',							'ондюпнвмши оюйер','',					''),
			(1,					'%мнянй дкъ ондюпйнб%',		'',						'рнбюпш',		'ондюпйх',							'мнбнцндмхе рнбюпш','',					''),
			(1,					'%мнбнцндмхи ондюпнй%',		'',						'рнбюпш',		'ондюпйх',							'мнбнцндмхи ондюпнй','',					''),
			(1,					'%анмсямюъ йюпрю%',			'',						'рнбюпш',		'ондюпйх',							'анмсямюъ йюпрю','',					''),
			(1,					'%ондюпнвмши мюанп%',		'',						'рнбюпш',		'ондюпйх',							'ондюпнвмши мюанп','',					''),
			(1,					'%мюанп ондюпйх%',			'',						'рнбюпш',		'ондюпйх',							'ондюпнвмши мюанп','',					''),

			
			(1,					'%йюпрш%',					'',						'рнбюпш',		'дпсцхе рнбюпш',					'йюпрш','',					''),
			(1,					'%хяоюпхрекэ%',				'',						'рнбюпш',		'дпсцхе рнбюпш',					'хяоюпхрекэ','',					''),
			(1,					'%аюрюпеий%',				'',						'рнбюпш',		'дпсцхе рнбюпш',					'аюрюпеийх','',					''),
			(1,					'%гюфхцюкйю%',				'',						'рнбюпш',		'дпсцхе рнбюпш',					'гюфхцюкйю','',					''),
			(1,					'%опегепбю%',				'',						'рнбюпш',		'дпсцхе рнбюпш',					'опегепбюрхбш','',					''),
			(1,					'%феб%пегхм%',				'',						'рнбюпш',		'дпсцхе рнбюпш',					'фебюрекэмюъ пегхмйю','',			''),
			(1,					'%яхцюпер%',				'',						'рнбюпш',		'дпсцхе рнбюпш',					'яхцюперш','',					'')	


		END
GO


/*
EXTRACT_SUBSTRING
оПНЖЕДСПЮ СДЮКЪЕР ОНДЯРПНЙС @SUBSTRING ХГ product_name Х ГЮОХЯШБЮЕР ЕЕ Б temp_param
гМЮВЕМХЕ ХГ temp_param ОНЯКЕ БШОНКМЕМХЪ ОПНЖЕДСПШ МСФМН ОЕПЕМЕЯРХ Б ОНЯРНЪММШИ ДПСЦНИ ЯРНКАЕЖ
оЮПЮЛЕРПШ
	@SUBSTRING - ОНДЯРПНЙЮ, ЙНРНПСЧ МЮДН МЮИРХ
	(ОНЙЮ МЕ ПЕЮКХГНБЮМ) @remove_measure - ОПХГМЮЙ, СДЮКЪРЭ КХ МЮИДЕММНЕ ЯНВЕРЮМХЕ ХГ product_name (ОНЙЮ МЕ ПЕЮКХГНБЮМ)
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

		/* САХПЮЧ КХЬМХЕ ОПНАЕКШ Х ОПНАЕКШ Б ЙНМЖЕ ЯРПНЙХ*/	  
		UPDATE step0
		SET product_name = rtrim(replace(product_name,'  ',' '))
	
	END
GO

/*
MULTIPLE_UPDATE
оПНЖЕДСПЮ СДЮКЪЕР ОНДЯРПНЙХ, ЯННРБЕРЯРБСЧЫХЕ ЙЮФДНИ ОЕПЕЛЕММНИ , ХГ product_name (ОН СЯКНБХЧ like @SUBSTRING)
Х ГЮОХЯШБЮЕР ГМЮВЕМХЪ ОЕПЕЛЕММШУ Б ЯННРБЕРЯРБСЧЫХЕ ЯРНКАЖШ
гМЮВЕМХЕ ХГ temp_param ОНЯКЕ БШОНКМЕМХЪ ОПНЖЕДСПШ МСФМН ОЕПЕМЕЯРХ Б ОНЯРНЪММШИ ДПСЦНИ ЯРНКАЕЖ

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

		/* САХПЮЧ КХЬМХЕ ОПНАЕКШ Х ОПНАЕКШ Б ЙНМЖЕ ЯРПНЙХ*/	  
		UPDATE step0
		SET product_name = rtrim(replace(product_name,'  ',' '))
	
	END
GO


/*
MULTIPLE_UPDATE_PRODSUBTYPE
оПНЖЕДСПЮ СДЮКЪЕР ОНДЯРПНЙХ, ЯННРБЕРЯРБСЧЫХЕ ЙЮФДНИ ОЕПЕЛЕММНИ , ХГ product_name  (ОН СЯКНБХЧ like @SUBSTRING and like @PROD_TYPE)
Х ГЮОХЯШБЮЕР ГМЮВЕМХЪ ОЕПЕЛЕММШУ Б ЯННРБЕРЯРБСЧЫХЕ ЯРНКАЖШ
гМЮВЕМХЕ ХГ temp_param ОНЯКЕ БШОНКМЕМХЪ ОПНЖЕДСПШ МСФМН ОЕПЕМЕЯРХ Б ОНЯРНЪММШИ ДПСЦНИ ЯРНКАЕЖ

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

		/* САХПЮЧ КХЬМХЕ ОПНАЕКШ Х ОПНАЕКШ Б ЙНМЖЕ ЯРПНЙХ*/	  
		UPDATE step0
		SET product_name = rtrim(replace(product_name,'  ',' '))
	
	END
GO


/*
MULTIPLE_UPDATE_PRODCATEGORY
оПНЖЕДСПЮ СДЮКЪЕР ОНДЯРПНЙХ, ЯННРБЕРЯРБСЧЫХЕ ЙЮФДНИ ОЕПЕЛЕММНИ , ХГ product_name  (ОН СЯКНБХЧ like @SUBSTRING and like @PROD_CATEGORY)
Х ГЮОХЯШБЮЕР ГМЮВЕМХЪ ОЕПЕЛЕММШУ Б ЯННРБЕРЯРБСЧЫХЕ ЯРНКАЖШ
гМЮВЕМХЕ ХГ temp_param ОНЯКЕ БШОНКМЕМХЪ ОПНЖЕДСПШ МСФМН ОЕПЕМЕЯРХ Б ОНЯРНЪММШИ ДПСЦНИ ЯРНКАЕЖ

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

		/* САХПЮЧ КХЬМХЕ ОПНАЕКШ Х ОПНАЕКШ Б ЙНМЖЕ ЯРПНЙХ*/	  
		UPDATE step0
		SET product_name = rtrim(replace(product_name,'  ',' '))
	
	END
GO


/* 
PRODUCT_NAME_PARCING
оПНЖЕДСПЮ ПЮГАХПЮЕР МЮХЛЕМНБЮМХЕ ОПНДСЙРЮ Х ГЮОНКМЪЕР ОНКЪ Я ОЮПЮЛЕРПЮЛХ ОПНДСЙРЮ
*/
DROP PROCEDURE IF EXISTS PRODUCT_NAME_PARCING
GO
CREATE PROCEDURE PRODUCT_NAME_PARCING
	AS
	BEGIN
	--STEP0: яНГДЮЧ РЮАКХЖС Я ОНКЪЛХ ДКЪ МЮОНКМЕМХЪ
		DROP TABLE IF EXISTS step0 
		SELECT	product_id,
				RTRIM(substring(product_name, 1, len(product_name)-1)) as product_name,  --САХПЮЕЛ МЕБХДХЛШИ МЕОНМЪРМШИ ЯХЛБНК Б ЙНМЖЕ ЯРПНЙХ Х ОПНАЕКШ Б ЙНМЖЕ ЯРПНЙХ

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
		

		/* ЯКНФМШЕ ХЯЙКЧВЕМХЪ */
		update step0  set product_name = replace(product_name, 'мюанпняемэ-гхлюъяюлюъйпел-юмрхярпеяя75лк+йпел-люмхйчп75лк/12', 'мюанпняемэ-гхлюъяюлюъйпел-юмрхярпеяя+йпел-люмхйчп 75лк+75лк / 12')
		where ( product_name like '%мюанпняемэ-гхлюъяюлюъйпел-юмрхярпеяя75лк+йпел-люмхйчп75лк/12%' )

		-------------------------------param15_producer-------------------------------------------
		UPDATE step0
		SET product_name=replace(replace(replace(replace(replace(product_name
												,'ой кюлю','кюлю')
												,'ннн ой ""осрхмю""','осрхмю')
												,'"йхгкъп цсо"','йхгкъп')
												,'"юд бхмн гсою.яепаяйне"','"юд бхмн гсою"')
												,'йюуерх"','йюуерх.')

		/* БНР АНКЭЬНИ ЙСЯНЙ, ЙНРНПШЕ МЮДН АШ ОЕПЕМЕЯРХ Б РЮАКХЖС ОПЮБХК product_category_rules. Р.Е. ЩРНР БЮПХЮМР НОПЕДЕКЕМХЪ ОНКЕИ - ЯЙНПЕЕ ЮРЮБХГЛ */
	    /* МЮВЮКН ЮРЮБХЯРХВЕЯЙНЦН СВЮЯРЙЮ */
		EXEC MULTIPLE_UPDATE				@SUBSTRING = 'кюлю'				,@PRODUCER	= 'ой кюлю'					,@PROD_TYPE ='опндсйрш'	,@PROD_SUBTYPE=''					,@PROD_CATEGORY=''					,@PROD_SUBCATEGORY=''
		EXEC MULTIPLE_UPDATE				@SUBSTRING = 'осрхмю'			,@PRODUCER	= 'ннн ой ""осрхмю""'		,@PROD_TYPE ='опндсйрш'	,@PROD_SUBTYPE='пшаю/пшанопндсйрш'	,@PROD_CATEGORY='хйпю'				,@PROD_SUBCATEGORY='хйпю'
		EXEC MULTIPLE_UPDATE				@SUBSTRING = 'рнляйюъ орхжетюапхйю'	,@PRODUCER	= 'рнляйюъ орхжетюапхйю',@PROD_TYPE ='опндсйрш'	,@PROD_SUBTYPE='ъижю'				,@PROD_CATEGORY=''					,@PROD_SUBCATEGORY=''
		EXEC MULTIPLE_UPDATE				@SUBSTRING = 'рот'				,@PRODUCER	= 'рот'						,@PROD_TYPE ='опндсйрш'	,@PROD_SUBTYPE='лъян/орхжю'			,@PROD_CATEGORY='йспхжю'			,@PROD_SUBCATEGORY=''
		EXEC MULTIPLE_UPDATE				@SUBSTRING = '"йхгкъп цсо"'		,@PRODUCER	= '"йхгкъп  цсо"'			,@PROD_TYPE ='юкйнцнкэ'	,@PROD_SUBTYPE='йнмэъй'				,@PROD_CATEGORY='пняяхъ(дюцеярюм)'	,@PROD_SUBCATEGORY=''
		EXEC MULTIPLE_UPDATE				@SUBSTRING = '"юд бхмн гсою"'	,@PRODUCER	= '"юд бхмн гсою.яепаяйне"' ,@PROD_TYPE ='юкйнцнкэ'	,@PROD_SUBTYPE=''					,@PROD_CATEGORY='яепахъ'			,@PROD_SUBCATEGORY=''  -- МЕ СДЮКЪЧ бхмн, ВРНАШ ПЮГНАПЮРЭ РХО бхмн МХФЕ
		EXEC MULTIPLE_UPDATE				@SUBSTRING = '"юярх.люпрхмх"'	,@PRODUCER	= '"юярх.люпрхмх"'          ,@PROD_TYPE ='юкйнцнкэ'	,@PROD_SUBTYPE='бхмн'				,@PROD_CATEGORY='хрюкхъ'			,@PROD_SUBCATEGORY='хцпхярне'
		EXEC MULTIPLE_UPDATE				@SUBSTRING = 'ъьйхмяйхи ой'		,@PRODUCER	= 'ъьйхмяйхи ой'			,@PROD_TYPE ='опндсйрш'	,@PROD_SUBTYPE='йнмдхрепяйхе хгдекхъ/якюднярх',@PROD_CATEGORY='бшоевйю'	,@PROD_SUBCATEGORY='бюткх'
		EXEC MULTIPLE_UPDATE				@SUBSTRING = 'лхпюрнпц'			,@PRODUCER	= 'лхпюрнпц'				,@PROD_TYPE ='опндсйрш'	,@PROD_SUBTYPE='лъян/орхжю'			,@PROD_CATEGORY=''					,@PROD_SUBCATEGORY=''
		EXEC MULTIPLE_UPDATE				@SUBSTRING = 'лхйнъм'			,@PRODUCER	= 'лхйнъм'					,@PROD_TYPE ='опндсйрш'	,@PROD_SUBTYPE='лъян/орхжю'			,@PROD_CATEGORY=''					,@PROD_SUBCATEGORY=''
		EXEC MULTIPLE_UPDATE				@SUBSTRING = 'фхрмхжю'			,@PRODUCER	= 'фхрмхжю'					,@PROD_TYPE ='опндсйрш'	,@PROD_SUBTYPE=''					,@PROD_CATEGORY=''					,@PROD_SUBCATEGORY=''
		EXEC MULTIPLE_UPDATE				@SUBSTRING = 'юпхнм'			,@PRODUCER	= 'юпхнм'					,@PROD_TYPE ='опндсйрш'	,@PROD_SUBTYPE='пшаю/пшанопндсйрш'	,@PROD_CATEGORY=''					,@PROD_SUBCATEGORY=''
		EXEC MULTIPLE_UPDATE				@SUBSTRING = 'кемлнпеопндсйр'	,@PRODUCER	= 'кемлнпеопндсйр'			,@PROD_TYPE ='опндсйрш'	,@PROD_SUBTYPE='пшаю/пшанопндсйрш'	,@PROD_CATEGORY='йерю'				,@PROD_SUBCATEGORY='яреий'
		EXEC MULTIPLE_UPDATE				@SUBSTRING = 'онкюпсятхь'		,@PRODUCER	= 'онкюпсятхь'				,@PROD_TYPE ='опндсйрш'	,@PROD_SUBTYPE='пшаю/пшанопндсйрш'	,@PROD_CATEGORY=''					,@PROD_SUBCATEGORY=''
		EXEC MULTIPLE_UPDATE				@SUBSTRING = 'ебпнопнл'			,@PRODUCER	= 'ебпнопнл'				,@PROD_TYPE ='опндсйрш'	,@PROD_SUBTYPE='пшаю/пшанопндсйрш'	,@PROD_CATEGORY=''					,@PROD_SUBCATEGORY=''
		EXEC MULTIPLE_UPDATE				@SUBSTRING = 'лот'				,@PRODUCER	= 'лот'						,@PROD_TYPE ='опндсйрш'	,@PROD_SUBTYPE='лъян/орхжю'			,@PROD_CATEGORY=''					,@PROD_SUBCATEGORY=''
		EXEC MULTIPLE_UPDATE_PRODCATEGORY	@SUBSTRING = 'опнярнйбюьхмн'	,@PRODUCER	= 'опнярнйбюьхмн'			,@PROD_TYPE ='опндсйрш'	,@PROD_SUBTYPE='охыебше фхпш'		,@PROD_CATEGORY='люякн якхбнвмне'	,@PROD_SUBCATEGORY=''
		EXEC MULTIPLE_UPDATE				@SUBSTRING = 'опнярнйбюьхмн'	,@PRODUCER	= 'опнярнйбюьхмн'			,@PROD_TYPE ='опндсйрш'	,@PROD_SUBTYPE='лнкнвмше опндсйрш'	,@PROD_CATEGORY=''					,@PROD_SUBCATEGORY=''
		EXEC MULTIPLE_UPDATE				@SUBSTRING = 'мюдхм'			,@PRODUCER	= 'мюдхм'					,@PROD_TYPE ='опндсйрш'	,@PROD_SUBTYPE='аюйюкеъ'			,@PROD_CATEGORY='вюи'				,@PROD_SUBCATEGORY=''
		EXEC MULTIPLE_UPDATE				@SUBSTRING = 'яюмрн яретюмн'	,@PRODUCER	= 'гюн ╚мон юЦПНЯЕПБХЯ╩'	,@PROD_TYPE ='юкйнцнкэ'	,@PROD_SUBTYPE='бхммши мюо.'			,@PROD_CATEGORY='пняяхъ'			,@PROD_SUBCATEGORY=''	-- МЕ СДЮКЪЧ бхммши мюо. цюг., ВРНАШ ПЮГНАПЮРЭ РХО бхмн МХФЕ
		EXEC MULTIPLE_UPDATE				@SUBSTRING = 'яхахпяйхи цсплюм'	,@PRODUCER	= 'яхахпяйхи цсплюм'		,@PROD_TYPE ='опндсйрш'	,@PROD_SUBTYPE='йскхмюпхъ/онкстюапхйюрш'	,@PROD_CATEGORY=''			,@PROD_SUBCATEGORY=''
		EXEC MULTIPLE_UPDATE				@SUBSTRING = 'щплюмм'			,@PRODUCER	= 'щплюмм'					,@PROD_TYPE ='опндсйрш'	,@PROD_SUBTYPE='лнкнвмше опндсйрш'	,@PROD_CATEGORY=''					,@PROD_SUBCATEGORY=''
		EXEC MULTIPLE_UPDATE				@SUBSTRING = 'алй'				,@PRODUCER	= 'ннн алй'					,@PROD_TYPE ='опндсйрш'	,@PROD_SUBTYPE='лнкнвмше опндсйрш'	,@PROD_CATEGORY=''					,@PROD_SUBCATEGORY=''
		EXEC MULTIPLE_UPDATE				@SUBSTRING = 'йюуерх.'			,@PRODUCER	= 'йюуерх'					,@PROD_TYPE ='юкйнцнкэ'	,@PROD_SUBTYPE=''					,@PROD_CATEGORY='пняяхъ'			,@PROD_SUBCATEGORY=''  -- МЕ СДЮКЪЧ бхмн, ВРНАШ ПЮГНАПЮРЭ РХО бхмн МХФЕ
		EXEC MULTIPLE_UPDATE_PRODCATEGORY	@SUBSTRING = 'апхг'				,@PRODUCER	= 'апхг'					,@PROD_TYPE ='опндсйрш'	,@PROD_SUBTYPE='пшаю/пшанопндсйрш'	,@PROD_CATEGORY='хйпю вепмюъ хлхрюжхъ'	,@PROD_SUBCATEGORY=''
		EXEC MULTIPLE_UPDATE_PRODCATEGORY	@SUBSTRING = 'апхг'				,@PRODUCER	= 'апхг'					,@PROD_TYPE ='опндсйрш'	,@PROD_SUBTYPE='пшаю/пшанопндсйрш'	,@PROD_CATEGORY='хйпю йпюямюъ хлхрюжхъ'	,@PROD_SUBCATEGORY=''
		EXEC MULTIPLE_UPDATE_PRODCATEGORY	@SUBSTRING = 'апхг'				,@PRODUCER	= 'апхг'					,@PROD_TYPE ='опндсйрш'	,@PROD_SUBTYPE='пшаю/пшанопндсйрш'	,@PROD_CATEGORY='люякн хйнпмне'			,@PROD_SUBCATEGORY=''
		EXEC MULTIPLE_UPDATE_PRODCATEGORY	@SUBSTRING = 'апхг'				,@PRODUCER	= 'апхг'					,@PROD_TYPE ='опндсйрш'	,@PROD_SUBTYPE='пшаю/пшанопндсйрш'	,@PROD_CATEGORY='мюанп й охбс'			,@PROD_SUBCATEGORY=''
		EXEC MULTIPLE_UPDATE_PRODCATEGORY	@SUBSTRING = 'рнляйхи пшагюбнд'	,@PRODUCER	= 'рнляйхи пшагюбнд'		,@PROD_TYPE ='опндсйрш'	,@PROD_SUBTYPE='пшаю/пшанопндсйрш'	,@PROD_CATEGORY='мюанп й охбс'			,@PROD_SUBCATEGORY=''
		EXEC MULTIPLE_UPDATE_PRODCATEGORY	@SUBSTRING = 'рнляйхи пшагюбнд'	,@PRODUCER	= 'рнляйхи пшагюбнд'		,@PROD_TYPE ='опндсйрш'	,@PROD_SUBTYPE='пшаю/пшанопндсйрш'	,@PROD_CATEGORY='тюпь пшамши'			,@PROD_SUBCATEGORY=''
		EXEC MULTIPLE_UPDATE_PRODCATEGORY	@SUBSTRING = 'рнляйхи пшагюбнд'	,@PRODUCER	= 'рнляйхи пшагюбнд'		,@PROD_TYPE ='опндсйрш'	,@PROD_SUBTYPE='пшаю/пшанопндсйрш'	,@PROD_CATEGORY='пшаю'					,@PROD_SUBCATEGORY=''-- НЯРЮБЬЕЕЯЪ ОН РНЛЯЙНЛС ПШАГЮБНДС
		EXEC MULTIPLE_UPDATE				@SUBSTRING = 'рнляйхи пшагюбнд'	,@PRODUCER	= 'рнляйхи пшагюбнд'		,@PROD_TYPE ='опндсйрш'	,@PROD_SUBTYPE='пшаю/пшанопндсйрш'	,@PROD_CATEGORY='онКстюапхйюр'			,@PROD_SUBCATEGORY='' -- СДЮКХРЭ ДСАКЭ пшангюбнд, пшагюбнд
		EXEC MULTIPLE_UPDATE_PRODCATEGORY	@SUBSTRING = 'рнляйхи пшангюбнд',@PRODUCER	= 'рнляйхи пшангюбнд'		,@PROD_TYPE ='опндсйрш'	,@PROD_SUBTYPE='пшаю/пшанопндсйрш'	,@PROD_CATEGORY='тюпь пшамши'			,@PROD_SUBCATEGORY=''
		EXEC MULTIPLE_UPDATE_PRODCATEGORY	@SUBSTRING = 'рнляйхи пшангюбнд',@PRODUCER	= 'рнляйхи пшангюбнд'		,@PROD_TYPE ='опндсйрш'	,@PROD_SUBTYPE='пшаю/пшанопндсйрш'	,@PROD_CATEGORY='пшаю'					,@PROD_SUBCATEGORY=''-- НЯРЮБЬЕЕЯЪ ОН РНЛЯЙНЛС ПШАнГЮБНДС
		EXEC MULTIPLE_UPDATE				@SUBSTRING = 'рнляйхи пшангюбнд',@PRODUCER	= 'рнляйхи пшангюбнд'		,@PROD_TYPE ='опндсйрш'	,@PROD_SUBTYPE='пшаю/пшанопндсйрш'	,@PROD_CATEGORY=''						,@PROD_SUBCATEGORY=''
		EXEC MULTIPLE_UPDATE_PRODCATEGORY	@SUBSTRING = 'йюльюр'			,@PRODUCER	= 'йюльюр'					,@PROD_TYPE ='опндсйрш'	,@PROD_SUBTYPE='пшаю/пшанопндсйрш'	,@PROD_CATEGORY='опеяепбш'				,@PROD_SUBCATEGORY=''
		EXEC MULTIPLE_UPDATE_PRODCATEGORY	@SUBSTRING = 'йюльюр'			,@PRODUCER	= 'йюльюр'					,@PROD_TYPE ='опндсйрш'	,@PROD_SUBTYPE='пшаю/пшанопндсйрш'	,@PROD_CATEGORY='пшаю'					,@PROD_SUBCATEGORY='' --ПЮГНАПЮРЭ ЙНОВЕМХЕ\ ГЮЛНПНФ \ ...?
		EXEC MULTIPLE_UPDATE				@SUBSTRING = 'йюльюр'			,@PRODUCER	= 'йюльюр'					,@PROD_TYPE ='опндсйрш'	,@PROD_SUBTYPE='пшаю/пшанопндсйрш'	,@PROD_CATEGORY=''						,@PROD_SUBCATEGORY='' ---ПЮГНАПЮРЭ
		EXEC MULTIPLE_UPDATE				@SUBSTRING = 'лхяреп уе'		,@PRODUCER	= 'лхяреп уе'				,@PROD_TYPE ='опндсйрш'	,@PROD_SUBTYPE='йскхмюпхъ/онкстюапхйюрш',@PROD_CATEGORY=''						,@PROD_SUBCATEGORY=''   --БНЯРНВМЮЪ ЙЮЙЮЪ-РН ЕДЮ , ЯЮКЮЭШ, ЯСЬР
		EXEC MULTIPLE_UPDATE				@SUBSTRING = 'тепщкэцюл'		,@PRODUCER	= 'тепщкэцюл'				,@PROD_TYPE ='опндсйрш'	,@PROD_SUBTYPE='йнмяепбюжхъ/янсяш'		,@PROD_CATEGORY=''						,@PROD_SUBCATEGORY=''   --  ЛЮПХМНБ, ЯНКЕМЭЪ, УПЕМНДЕП
		EXEC MULTIPLE_UPDATE				@SUBSTRING = 'аскцюпйнмяепб'	,@PRODUCER	= 'аскцюпйнмяепб'			,@PROD_TYPE ='опндсйрш'	,@PROD_SUBTYPE='йнмяепбюжхъ/янсяш'	,@PROD_CATEGORY=''						,@PROD_SUBCATEGORY=''   
		EXEC MULTIPLE_UPDATE				@SUBSTRING = 'гекемюъ сяюдэаю'  ,@PRODUCER	= 'юйбюрнпхъ'				,@PROD_TYPE ='опндсйрш'	,@PROD_SUBTYPE='йнмяепбюжхъ/янсяш'	,@PROD_CATEGORY=''						,@PROD_SUBCATEGORY=''   --  ОПЕЯЕПБШ, ПШАЮ, ЯНКЕМЭЪ, НБНЫХ
		EXEC MULTIPLE_UPDATE_PRODCATEGORY	@SUBSTRING = 'юйбюрнпхъ'		,@PRODUCER	= 'юйбюрнпхъ'				,@PROD_TYPE ='опндсйрш'	,@PROD_SUBTYPE='йскхмюпхъ/онкстюапхйюрш'	,@PROD_CATEGORY='яюкюр'			,@PROD_SUBCATEGORY=''   --  ОПЕЯЕПБШ, ПШАЮ, ЯНКЕМЭЪ, НБНЫХ
		EXEC MULTIPLE_UPDATE				@SUBSTRING = 'юйбюрнпхъ'		,@PRODUCER	= 'юйбюрнпхъ'				,@PROD_TYPE ='опндсйрш'	,@PROD_SUBTYPE='пшаю/пшанопндсйрш'	,@PROD_CATEGORY=''						,@PROD_SUBCATEGORY=''   --  ОПЕЯЕПБШ, ПШАЮ, ЯНКЕМЭЪ, НБНЫХ
		EXEC MULTIPLE_UPDATE_PRODCATEGORY	@SUBSTRING = 'аюкрхияйхи аепец'	,@PRODUCER	= 'аюкрхияйхи аепец'		,@PROD_TYPE ='опндсйрш'	,@PROD_SUBTYPE='пшаю/пшанопндсйрш'	,@PROD_CATEGORY='опеяепбш'				,@PROD_SUBCATEGORY='' 
		EXEC MULTIPLE_UPDATE_PRODCATEGORY	@SUBSTRING = 'аюкрхияйхи аепец'	,@PRODUCER	= 'аюкрхияйхи аепец'		,@PROD_TYPE ='опндсйрш'	,@PROD_SUBTYPE='пшаю/пшанопндсйрш'	,@PROD_CATEGORY='хйпю'					,@PROD_SUBCATEGORY='' 
		EXEC MULTIPLE_UPDATE_PRODCATEGORY	@SUBSTRING = 'аюкрхияйхи аепец'	,@PRODUCER	= 'аюкрхияйхи аепец'		,@PROD_TYPE ='опндсйрш'	,@PROD_SUBTYPE='пшаю/пшанопндсйрш'	,@PROD_CATEGORY='люякн княняебне'		,@PROD_SUBCATEGORY='' 
		EXEC MULTIPLE_UPDATE_PRODCATEGORY	@SUBSTRING = 'аюкрхияйхи аепец'	,@PRODUCER	= 'аюкрхияйхи аепец'		,@PROD_TYPE ='опндсйрш'	,@PROD_SUBTYPE='пшаю/пшанопндсйрш'	,@PROD_CATEGORY='люякн яекеднвмне'		,@PROD_SUBCATEGORY='' 
		EXEC MULTIPLE_UPDATE_PRODCATEGORY	@SUBSTRING = 'аюкрхияйхи аепец'	,@PRODUCER	= 'аюкрхияйхи аепец'		,@PROD_TYPE ='опндсйрш'	,@PROD_SUBTYPE='пшаю/пшанопндсйрш'	,@PROD_CATEGORY='оюьрер пшамши'			,@PROD_SUBCATEGORY='' 
		EXEC MULTIPLE_UPDATE_PRODCATEGORY	@SUBSTRING = 'аюкрхияйхи аепец'	,@PRODUCER	= 'аюкрхияйхи аепец'		,@PROD_TYPE ='опндсйрш'	,@PROD_SUBTYPE='пшаю/пшанопндсйрш'	,@PROD_CATEGORY='пшаю'					,@PROD_SUBCATEGORY='' 
		EXEC MULTIPLE_UPDATE				@SUBSTRING = 'аюкрхияйхи аепец'	,@PRODUCER	= 'аюкрхияйхи аепец'		,@PROD_TYPE ='опндсйрш'	,@PROD_SUBTYPE='пшаю/пшанопндсйрш'	,@PROD_CATEGORY='яюкюр хг лнпяйни йюосярш'	,@PROD_SUBCATEGORY='' 
		EXEC MULTIPLE_UPDATE_PRODCATEGORY	@SUBSTRING = 'лепхдхюм'			,@PRODUCER	= 'лепхдхюм'				,@PROD_TYPE ='опндсйрш'	,@PROD_SUBTYPE='пшаю/пшанопндсйрш'	,@PROD_CATEGORY='опеяепбш'				,@PROD_SUBCATEGORY='' 
		EXEC MULTIPLE_UPDATE_PRODCATEGORY	@SUBSTRING = 'лепхдхюм'			,@PRODUCER	= 'лепхдхюм'				,@PROD_TYPE ='опндсйрш'	,@PROD_SUBTYPE='пшаю/пшанопндсйрш'	,@PROD_CATEGORY='яюкюр хг лнпяйни йюосярш'	,@PROD_SUBCATEGORY='' 
		EXEC MULTIPLE_UPDATE_PRODCATEGORY	@SUBSTRING = 'лепхдхюм'			,@PRODUCER	= 'лепхдхюм'				,@PROD_TYPE ='опндсйрш'	,@PROD_SUBTYPE='пшаю/пшанопндсйрш'	,@PROD_CATEGORY='йпюанбше оюкнвйх'		,@PROD_SUBCATEGORY='' 
		EXEC MULTIPLE_UPDATE_PRODCATEGORY	@SUBSTRING = 'лепхдхюм'			,@PRODUCER	= 'лепхдхюм'				,@PROD_TYPE ='опндсйрш'	,@PROD_SUBTYPE='пшаю/пшанопндсйрш'	,@PROD_CATEGORY='пшаю'					,@PROD_SUBCATEGORY='' -- НЯРЮБЬЕЕЯЪ ОН лепхдхюм
		EXEC MULTIPLE_UPDATE				@SUBSTRING = 'лепхдхюм'			,@PRODUCER	= 'лепхдхюм'				,@PROD_TYPE ='опндсйрш'	,@PROD_SUBTYPE='пшаю/пшанопндсйрш'	,@PROD_CATEGORY='хйпю'					,@PROD_SUBCATEGORY='' 
		EXEC MULTIPLE_UPDATE_PRODCATEGORY	@SUBSTRING = 'тнятнпекэ'		,@PRODUCER	= 'тнятнпекэ'				,@PROD_TYPE ='опндсйрш'	,@PROD_SUBTYPE='пшаю/пшанопндсйрш'	,@PROD_CATEGORY='опеяепбш'				,@PROD_SUBCATEGORY='' 
		EXEC MULTIPLE_UPDATE_PRODCATEGORY	@SUBSTRING = 'тнятнпекэ'		,@PRODUCER	= 'тнятнпекэ'				,@PROD_TYPE ='опндсйрш'	,@PROD_SUBTYPE='пшаю/пшанопндсйрш'	,@PROD_CATEGORY='пшаю'					,@PROD_SUBCATEGORY='' 
		EXEC MULTIPLE_UPDATE_PRODCATEGORY	@SUBSTRING = 'йюохрюм'			,@PRODUCER	= 'йюохрюм'					,@PROD_TYPE ='опндсйрш'	,@PROD_SUBTYPE='пшаю/пшанопндсйрш'	,@PROD_CATEGORY='опеяепбш'				,@PROD_SUBCATEGORY='' 
		EXEC MULTIPLE_UPDATE_PRODCATEGORY	@SUBSTRING = 'йюохрюм'			,@PRODUCER	= 'йюохрюм'					,@PROD_TYPE ='опндсйрш'	,@PROD_SUBTYPE='пшаю/пшанопндсйрш'	,@PROD_CATEGORY='пшаю'					,@PROD_SUBCATEGORY='' 
		EXEC MULTIPLE_UPDATE_PRODCATEGORY	@SUBSTRING = 'рд щйнп'			,@PRODUCER	= 'рд щйнп'					,@PROD_TYPE ='опндсйрш'	,@PROD_SUBTYPE='пшаю/пшанопндсйрш'	,@PROD_CATEGORY='опеяепбш'				,@PROD_SUBCATEGORY='' 
		EXEC MULTIPLE_UPDATE_PRODCATEGORY	@SUBSTRING = 'рд щйнп'			,@PRODUCER	= 'рд щйнп'					,@PROD_TYPE ='опндсйрш'	,@PROD_SUBTYPE='пшаю/пшанопндсйрш'	,@PROD_CATEGORY='мюанп й охбс'			,@PROD_SUBCATEGORY='' 
		EXEC MULTIPLE_UPDATE_PRODCATEGORY	@SUBSTRING = 'рд щйнп'			,@PRODUCER	= 'рд щйнп'					,@PROD_TYPE ='опндсйрш'	,@PROD_SUBTYPE='пшаю/пшанопндсйрш'	,@PROD_CATEGORY='пшаю'					,@PROD_SUBCATEGORY='' 
		EXEC MULTIPLE_UPDATE_PRODCATEGORY	@SUBSTRING = 'гнкнрюъ пшайю'	,@PRODUCER	= 'гнкнрюъ пшайю'			,@PROD_TYPE ='опндсйрш'	,@PROD_SUBTYPE='пшаю/пшанопндсйрш'	,@PROD_CATEGORY='опеяепбш'				,@PROD_SUBCATEGORY='' 
		EXEC MULTIPLE_UPDATE_PRODCATEGORY	@SUBSTRING = 'гнкнрюъ пшайю'	,@PRODUCER	= 'гнкнрюъ пшайю'			,@PROD_TYPE ='опндсйрш'	,@PROD_SUBTYPE='пшаю/пшанопндсйрш'	,@PROD_CATEGORY='пшаю'					,@PROD_SUBCATEGORY='' 
		EXEC MULTIPLE_UPDATE_PRODCATEGORY	@SUBSTRING = 'аюкрхияйхи'		,@PRODUCER	= 'аюкрхияйхи'				,@PROD_TYPE ='опндсйрш'	,@PROD_SUBTYPE='пшаю/пшанопндсйрш'	,@PROD_CATEGORY='опеяепбш'				,@PROD_SUBCATEGORY='' 
		EXEC MULTIPLE_UPDATE				@SUBSTRING = 'яепбхяямюа'		,@PRODUCER	= 'яепбхяямюа'				,@PROD_TYPE ='опндсйрш'	,@PROD_SUBTYPE='йскхмюпхъ/онкстюапхйюрш',@PROD_CATEGORY=''					,@PROD_SUBCATEGORY='яюкюр ябефеярэ хг йюосярш люпхмнбюммни' 
		EXEC MULTIPLE_UPDATE				@SUBSTRING = 'бад'				,@PRODUCER	= 'бад'						,@PROD_TYPE ='опндсйрш'	,@PROD_SUBTYPE='лнкнвмше опндсйрш'	,@PROD_CATEGORY=''						,@PROD_SUBCATEGORY=''
		EXEC MULTIPLE_UPDATE				@SUBSTRING = 'юмрнмнб дбнп'		,@PRODUCER	= 'юмрнмнб дбнп'			,@PROD_TYPE ='опндсйрш'	,@PROD_SUBTYPE='йскхмюпхъ/онкстюапхйюрш',@PROD_CATEGORY=''					,@PROD_SUBCATEGORY='' --АКХМШ, АСКЙХ, ЙНРКЕРШ, БЮПХЕМХЙХ, ДЕЯЕПР, ОХПНФМШЕ, РНПР  -- ЙСКХМЮПХЪ?
		EXEC MULTIPLE_UPDATE				@SUBSTRING = 'хмлюпйн'			,@PRODUCER	= 'хмлюпйн'					,@PROD_TYPE ='опндсйрш'	,@PROD_SUBTYPE='лнкнвмше опндсйрш'	,@PROD_CATEGORY='лнпнфемне'				,@PROD_SUBCATEGORY=''
		EXEC MULTIPLE_UPDATE				@SUBSTRING = 'ямефмши цнпнднй'	,@PRODUCER	= 'ямефмши цнпнднй'			,@PROD_TYPE ='опндсйрш'	,@PROD_SUBTYPE='лнкнвмше опндсйрш'	,@PROD_CATEGORY='лнпнфемне'				,@PROD_SUBCATEGORY=''
		EXEC MULTIPLE_UPDATE				@SUBSTRING = 'цпняохпнм'		,@PRODUCER	= 'анвйюпх'					,@PROD_TYPE ='опндсйрш'	,@PROD_SUBTYPE='лнкнвмше опндсйрш'	,@PROD_CATEGORY='лнпнфемне'				,@PROD_SUBCATEGORY=''
		EXEC MULTIPLE_UPDATE				@SUBSTRING = 'онкъпхя'			,@PRODUCER	= 'онкъпхя'					,@PROD_TYPE ='опндсйрш'	,@PROD_SUBTYPE='лнкнвмше опндсйрш'	,@PROD_CATEGORY='лнпнфемне'				,@PROD_SUBCATEGORY=''
		EXEC MULTIPLE_UPDATE_PRODCATEGORY	@SUBSTRING = 'анвйюпх'			,@PRODUCER	= 'анвйюпх'					,@PROD_TYPE ='опндсйрш'	,@PROD_SUBTYPE='мюохрйх'			,@PROD_CATEGORY='мюохрнй цюг.'			,@PROD_SUBCATEGORY=''
		EXEC MULTIPLE_UPDATE_PRODCATEGORY	@SUBSTRING = 'анвйюпх'			,@PRODUCER	= 'анвйюпх'					,@PROD_TYPE ='опндсйрш'	,@PROD_SUBTYPE='бндю'				,@PROD_CATEGORY='бндю лхмепюк.'			,@PROD_SUBCATEGORY=''
		EXEC MULTIPLE_UPDATE_PRODCATEGORY	@SUBSTRING = 'лхмх TV'			,@PRODUCER	= 'лхмх TV'					,@PROD_TYPE ='опндсйрш'	,@PROD_SUBTYPE='йнмдхрепяйхе хгдекхъ/якюднярх',@PROD_CATEGORY=''			,@PROD_SUBCATEGORY=''  -- ОЕВЕМЭЕ, ЙЕЙЯШ, РНПРШ, ЙПСЮЯЯ, БЮТ
		EXEC MULTIPLE_UPDATE_PRODCATEGORY	@SUBSTRING = 'TV'				,@PRODUCER	= 'TV'						,@PROD_TYPE ='опндсйрш'	,@PROD_SUBTYPE='йнмдхрепяйхе хгдекхъ/якюднярх',@PROD_CATEGORY=''			,@PROD_SUBCATEGORY=''  -- ОЕВЕМЭЕ, ЙЕЙЯШ, РНПРШ, ЙПСЮЯЯ, БЮТ
		EXEC MULTIPLE_UPDATE_PRODCATEGORY	@SUBSTRING = 'псяаеппх кюим'	,@PRODUCER	= 'псяаеппх кюим'			,@PROD_TYPE ='опндсйрш'	,@PROD_SUBTYPE='нпеух/цпхаш'		,@PROD_CATEGORY='цпхаш'					,@PROD_SUBCATEGORY='гюлнпнгйю'  
		EXEC MULTIPLE_UPDATE_PRODCATEGORY	@SUBSTRING = 'псяаеппх кюим'	,@PRODUCER	= 'псяаеппх кюим'			,@PROD_TYPE ='опндсйрш'	,@PROD_SUBTYPE='нпеух/цпхаш'		,@PROD_CATEGORY='ялеяэ цпхамюъ'			,@PROD_SUBCATEGORY=''     
		EXEC MULTIPLE_UPDATE				@SUBSTRING = 'псяаеппх кюим'	,@PRODUCER	= 'псяаеппх кюим'			,@PROD_TYPE ='опндсйрш'	,@PROD_SUBTYPE='тпсйрш/ъцндш'		,@PROD_CATEGORY='ъцндш'					,@PROD_SUBCATEGORY=''   
		EXEC MULTIPLE_UPDATE				@SUBSTRING = 'пшангюбнд аюийюк'	,@PRODUCER	= 'пшангюбнд аюийюкр'		,@PROD_TYPE ='опндсйрш',@PROD_SUBTYPE='пшаю/пшанопндсйрш',@PROD_CATEGORY=''						,@PROD_SUBCATEGORY='' ---АХРНВЙХ ПШАМШ Х ОЕКЭЛЕМХ ПШАМШЕ
		EXEC MULTIPLE_UPDATE_PRODCATEGORY	@SUBSTRING = '4 яегнмю'			,@PRODUCER	= '4 яегнмю'				,@PROD_TYPE ='опндсйрш'	,@PROD_SUBTYPE='нбных/гекемэ'		,@PROD_CATEGORY='йсйспсгю'				,@PROD_SUBCATEGORY=''  
		EXEC MULTIPLE_UPDATE				@SUBSTRING = '4 яегнмю'			,@PRODUCER	= '4 яегнмю'				,@PROD_TYPE ='опндсйрш'	,@PROD_SUBTYPE='тпсйрш/ъцндш'		,@PROD_CATEGORY='ъцндш'					,@PROD_SUBCATEGORY=''   
		EXEC MULTIPLE_UPDATE				@SUBSTRING = 'укеангюбнд ╧4'	,@PRODUCER	= 'укеангюбнд ╧4'			,@PROD_TYPE ='опндсйрш'	,@PROD_SUBTYPE='укеанаскнвмше хгдекхъ/бшоевйю',@PROD_CATEGORY=''			,@PROD_SUBCATEGORY=''   
		EXEC MULTIPLE_UPDATE_PRODCATEGORY	@SUBSTRING = 'лнпнгйн'			,@PRODUCER	= 'лнпнгйн'					,@PROD_TYPE ='рнбюпш'	,@PROD_SUBTYPE='рнбюпш дкъ дереи'	,@PROD_CATEGORY='йпел деряйхи'			,@PROD_SUBCATEGORY=''      
		EXEC MULTIPLE_UPDATE_PRODCATEGORY	@SUBSTRING = 'лнпнгйн'			,@PRODUCER	= 'лнпнгйн'					,@PROD_TYPE ='опндсйрш'	,@PROD_SUBTYPE='тпсйрш/ъцндш'		,@PROD_CATEGORY='цпхм йксамхйю'			,@PROD_SUBCATEGORY='гюлнпнгйю'      -- ЯДЕКЮРЭ update ЯЛЕЯЭ МЮ ЦПХАШ
		EXEC MULTIPLE_UPDATE_PRODCATEGORY	@SUBSTRING = 'лнпнгйн'			,@PRODUCER	= 'лнпнгйн'					,@PROD_TYPE ='опндсйрш'	,@PROD_SUBTYPE='тпсйрш/ъцндш'		,@PROD_CATEGORY='цпхм люкхмю'			,@PROD_SUBCATEGORY='гюлнпнгйю'      -- ЯДЕКЮРЭ update ЯЛЕЯЭ МЮ ЦПХАШ
		EXEC MULTIPLE_UPDATE_PRODCATEGORY	@SUBSTRING = 'лнпнгйн'			,@PRODUCER	= 'лнпнгйн'					,@PROD_TYPE ='опндсйрш'	,@PROD_SUBTYPE='нпеух/цпхаш'		,@PROD_CATEGORY='цпхм ьюлохмэнмш'		,@PROD_SUBCATEGORY='гюлнпнгйю'      -- ЯДЕКЮРЭ update ЯЛЕЯЭ МЮ ЦПХАШ
		EXEC MULTIPLE_UPDATE_PRODCATEGORY	@SUBSTRING = 'лнпнгйн'			,@PRODUCER	= 'лнпнгйн'					,@PROD_TYPE ='опндсйрш'	,@PROD_SUBTYPE='нбных/гекемэ'		,@PROD_CATEGORY='цпхм'					,@PROD_SUBCATEGORY='гюлнпнгйю'      -- ЯДЕКЮРЭ update ЯЛЕЯЭ МЮ ЦПХАШ
		EXEC MULTIPLE_UPDATE				@SUBSTRING = 'лнпнгйн'			,@PRODUCER	= 'лнпнгйн'					,@PROD_TYPE ='опндсйрш'	,@PROD_SUBTYPE='йскхмюпхъ/онкстюапхйюрш',	@PROD_CATEGORY='цпхм йксамхйю'	,@PROD_SUBCATEGORY='гюлнпнгйю'      -- ЯДЕКЮРЭ update ЯЛЕЯЭ МЮ ЦПХАШ
		EXEC MULTIPLE_UPDATE				@SUBSTRING = 'юйрхлекэ'			,@PRODUCER	= 'юйрхлекэ'				,@PROD_TYPE ='опндсйрш'	,@PROD_SUBTYPE='лнкнвмше опндсйрш',	@PROD_CATEGORY='лнкнвмши опндсйр'		,@PROD_SUBCATEGORY=''     
		EXEC MULTIPLE_UPDATE				@SUBSTRING = 'пнккрнм'			,@PRODUCER	= 'пнккрнм'					,@PROD_TYPE ='опндсйрш'	,@PROD_SUBTYPE='аюйюкеъ'			,@PROD_CATEGORY='ялеяэ дкъ ашярпнцн опхцнрнбкемхъ'						,@PROD_SUBCATEGORY=''     
		EXEC MULTIPLE_UPDATE				@SUBSTRING = 'беяекши цмнл'		,@PRODUCER	= 'беяекши цмнл'			,@PROD_TYPE ='опндсйрш'	,@PROD_SUBTYPE='йнмдхрепяйхе хгдекхъ/якюднярх',@PROD_CATEGORY='феке'		,@PROD_SUBCATEGORY=''  

		update step0
		set param19_subcategory = '70 / 90',
		param18_category='йпеберйх',
		product_name = replace( replace(product_name, '70/90',''),'йпеберйх','')
		where product_name like '%70/90%' and product_name like '%йпеберйх%'
		
		EXEC MULTIPLE_UPDATE				@SUBSTRING = 'юцюлю'			,@PRODUCER	= 'юцюлю'					,@PROD_TYPE ='опндсйрш'	,@PROD_SUBTYPE='пшаю/пшанопндсйрш'	,@PROD_CATEGORY=''						,@PROD_SUBCATEGORY='' ---
		EXEC MULTIPLE_UPDATE				@SUBSTRING = 'йпюямныейнбн'		,@PRODUCER	= 'йпюямныейнбн'			,@PROD_TYPE ='опндсйрш'	,@PROD_SUBTYPE='охыебше фхпш'	,@PROD_CATEGORY='люякн'					,@PROD_SUBCATEGORY='якхбнвмне' ---
		EXEC MULTIPLE_UPDATE_PRODCATEGORY	@SUBSTRING = 'тпсйрнбхвх'		,@PRODUCER	= 'йнмдхрепяйхи йнлахмюр нгепяйхи ясбемхп',	@PROD_TYPE ='опндсйрш',@PROD_SUBTYPE='йнмдхрепяйхе хгдекхъ/якюднярх',@PROD_CATEGORY='йнмтерш'	,@PROD_SUBCATEGORY='' 
		EXEC MULTIPLE_UPDATE_PRODCATEGORY	@SUBSTRING = 'нпеунбхвх'		,@PRODUCER	= 'йнмдхрепяйхи йнлахмюр нгепяйхи ясбемхп',	@PROD_TYPE ='опндсйрш',@PROD_SUBTYPE='йнмдхрепяйхе хгдекхъ/якюднярх',@PROD_CATEGORY='йнмтерш'	,@PROD_SUBCATEGORY='' 
		EXEC MULTIPLE_UPDATE_PRODCATEGORY	@SUBSTRING = 'бхвх'				,@PRODUCER	= 'бхвх',					@PROD_TYPE ='опндсйрш'	,@PROD_SUBTYPE='пшаю/пшанопндсйрш'	,@PROD_CATEGORY='йпюанбне лъян'			,@PROD_SUBCATEGORY='' 
		EXEC MULTIPLE_UPDATE_PRODCATEGORY	@SUBSTRING = 'бхвх'				,@PRODUCER	= 'бхвх',					@PROD_TYPE ='опндсйрш'	,@PROD_SUBTYPE='пшаю/пшанопндсйрш'	,@PROD_CATEGORY='йпюанбше оюкнвйх'		,@PROD_SUBCATEGORY=''
		EXEC MULTIPLE_UPDATE_PRODCATEGORY	@SUBSTRING = 'бхвх'				,@PRODUCER	= 'бхвх',					@PROD_TYPE ='опндсйрш'	,@PROD_SUBTYPE='пшаю/пшанопндсйрш'	,@PROD_CATEGORY='йпюанбне оюкнвйх'		,@PROD_SUBCATEGORY=''  -- САПЮРЭ ДСАКЭ  ЙПЮАНБНЕ ЙПЮАНБШЕ
		EXEC MULTIPLE_UPDATE				@SUBSTRING = 'бхвх'				,@PRODUCER	= 'бхвх',					@PROD_TYPE ='опндсйрш'	,@PROD_SUBTYPE='пшаю/пшанопндсйрш'	,@PROD_CATEGORY=''						,@PROD_SUBCATEGORY=''
		EXEC MULTIPLE_UPDATE_PRODCATEGORY	@SUBSTRING = 'меярке'			,@PRODUCER	= 'меярке',					@PROD_TYPE ='опндсйрш'	,@PROD_SUBTYPE='лнкнвмше опндсйрш'	,@PROD_CATEGORY='лнпнфемне'				,@PROD_SUBCATEGORY=''
		EXEC MULTIPLE_UPDATE_PRODCATEGORY	@SUBSTRING = 'меярке'			,@PRODUCER	= 'меярке',					@PROD_TYPE ='опндсйрш'	,@PROD_SUBTYPE='аюйюкеъ'			,@PROD_CATEGORY='цнр. гюбрпюй'			,@PROD_SUBCATEGORY=''
		EXEC MULTIPLE_UPDATE_PRODCATEGORY	@SUBSTRING = 'меярке'			,@PRODUCER	= 'меярке',					@PROD_TYPE ='опндсйрш'	,@PROD_SUBTYPE='аюйюкеъ'			,@PROD_CATEGORY='йюью'					,@PROD_SUBCATEGORY=''
		EXEC MULTIPLE_UPDATE_PRODCATEGORY	@SUBSTRING = 'меярке'			,@PRODUCER	= 'меярке',					@PROD_TYPE ='опндсйрш'	,@PROD_SUBTYPE='йнмдхрепяйхе хгдекхъ/якюднярх'	,@PROD_CATEGORY='йнмтер'	,@PROD_SUBCATEGORY=''
		EXEC MULTIPLE_UPDATE_PRODCATEGORY	@SUBSTRING = 'меярке'			,@PRODUCER	= 'меярке',					@PROD_TYPE ='опндсйрш'	,@PROD_SUBTYPE='деряйне охрюмхе'	,@PROD_CATEGORY='меярнфем'				,@PROD_SUBCATEGORY='ялеяэ'
		EXEC MULTIPLE_UPDATE_PRODCATEGORY	@SUBSTRING = 'меярке'			,@PRODUCER	= 'меярке',					@PROD_TYPE ='опндсйрш'	,@PROD_SUBTYPE='йнмдхрепяйхе хгдекхъ/якюднярх'	,@PROD_CATEGORY='ьнйнкюд'	,@PROD_SUBCATEGORY=''
		EXEC MULTIPLE_UPDATE_PRODCATEGORY	@SUBSTRING = 'меярке'			,@PRODUCER	= 'меярке',					@PROD_TYPE ='опндсйрш'	,@PROD_SUBTYPE='йнмдхрепяйхе хгдекхъ/якюднярх'	,@PROD_CATEGORY='ьнй.аюрнмвхй'	,@PROD_SUBCATEGORY=''
		EXEC MULTIPLE_UPDATE				@SUBSTRING = 'меярке'			,@PRODUCER	= 'меярке',					@PROD_TYPE ='опндсйрш'	,@PROD_SUBTYPE=''					,@PROD_CATEGORY=''						,@PROD_SUBCATEGORY='' ---
		EXEC MULTIPLE_UPDATE_PRODCATEGORY	@SUBSTRING = 'об'				,@PRODUCER	= 'об',						@PROD_TYPE ='опндсйрш'	,@PROD_SUBTYPE='йнмдхрепяйхе хгдекхъ/якюднярх',@PROD_CATEGORY='оевемэе'				,@PROD_SUBCATEGORY=''
		EXEC MULTIPLE_UPDATE_PRODSUBTYPE	@SUBSTRING = 'аекюпсяэ'			,@PRODUCER	= 'аекюпсяэ',				@PROD_TYPE ='юкйнцнкэ'	,@PROD_SUBTYPE='бхммши мюо.'					,@PROD_CATEGORY=''						,@PROD_SUBCATEGORY='' ---
		EXEC MULTIPLE_UPDATE				@SUBSTRING = 'аекюпсяэ'			,@PRODUCER	= 'аекюпсяэ',				@PROD_TYPE ='опндсйрш'	,@PROD_SUBTYPE=''								,@PROD_CATEGORY=''						,@PROD_SUBCATEGORY='' ---
		EXEC MULTIPLE_UPDATE				@SUBSTRING = 'аекеаеи'			,@PRODUCER	= 'аекеаеи',				@PROD_TYPE ='опндсйрш'	,@PROD_SUBTYPE='яшпш'							,@PROD_CATEGORY=''						,@PROD_SUBCATEGORY='' ---
		EXEC MULTIPLE_UPDATE				@SUBSTRING = 'мнбняхахпяйюъ ьт'	,@PRODUCER	= 'мнбняхахпяйюъ ьт',		@PROD_TYPE ='опндсйрш'	,@PROD_SUBTYPE='йнмдхрепяйхе хгдекхъ/якюднярх'	,@PROD_CATEGORY=''						,@PROD_SUBCATEGORY='' ---
		EXEC MULTIPLE_UPDATE				@SUBSTRING = 'рд мнбняхахпяйхи'	,@PRODUCER	= 'рд мнбняхахпяйхи',		@PROD_TYPE ='опндсйрш'	,@PROD_SUBTYPE='нбных/гекемэ'					,@PROD_CATEGORY=''						,@PROD_SUBCATEGORY='' ---
		EXEC MULTIPLE_UPDATE				@SUBSTRING = 'рй мнбняхахпяйхи'	,@PRODUCER	= 'рй мнбняхахпяйхи',		@PROD_TYPE ='опндсйрш'	,@PROD_SUBTYPE='нбных/гекемэ'					,@PROD_CATEGORY=''						,@PROD_SUBCATEGORY='' ---
		EXEC MULTIPLE_UPDATE				@SUBSTRING = 'мнбняхахпяй'		,@PRODUCER	= 'мнбняхахпяй',			@PROD_TYPE ='опндсйрш'	,@PROD_SUBTYPE='лнкнвмше опндсйрш'				,@PROD_CATEGORY=''						,@PROD_SUBCATEGORY='' ---
		EXEC MULTIPLE_UPDATE_PRODCATEGORY	@SUBSTRING = 'депебемяйне лнкнвйн',@PRODUCER= 'депебемяйне лнкнвйн',	@PROD_TYPE ='опндсйрш'	,@PROD_SUBTYPE='охыебше фхпш'					,@PROD_CATEGORY='люякн якхбнвмне'		,@PROD_SUBCATEGORY='' ---
		EXEC MULTIPLE_UPDATE				@SUBSTRING = 'депебемяйне лнкнвйн',@PRODUCER= 'депебемяйне лнкнвйн',	@PROD_TYPE ='опндсйрш'	,@PROD_SUBTYPE='лнкнвмше опндсйрш'				,@PROD_CATEGORY=''						,@PROD_SUBCATEGORY='' ---
		EXEC MULTIPLE_UPDATE_PRODCATEGORY	@SUBSTRING = 'юпцемрхмю'		,@PRODUCER	= 'юпцемрхмю',				@PROD_TYPE ='юкйнцнкэ'	,@PROD_SUBTYPE='бхмн'							,@PROD_CATEGORY=''						,@PROD_SUBCATEGORY='' ---
		EXEC MULTIPLE_UPDATE				@SUBSTRING = 'юпцемрхмю'		,@PRODUCER	= 'юпцемрхмю',				@PROD_TYPE ='опндсйрш'	,@PROD_SUBTYPE=''								,@PROD_CATEGORY=''						,@PROD_SUBCATEGORY='' ---
		EXEC MULTIPLE_UPDATE				@SUBSTRING = 'юбюмцюпд'			,@PRODUCER	= 'юбюмцюпд',				@PROD_TYPE ='опндсйрш'	,@PROD_SUBTYPE='пшаю/пшанопндсйрш'				,@PROD_CATEGORY=''						,@PROD_SUBCATEGORY='' ---
		EXEC MULTIPLE_UPDATE				@SUBSTRING ='унпньее йювеярбн, яопюбедкхбюъ жемю',@PRODUCER ='ой кюлю', @PROD_TYPE ='опндсйрш'	,@PROD_SUBTYPE='йскхмюпхъ/онкстюапхйюрш'		,@PROD_CATEGORY=''						,@PROD_SUBCATEGORY='' ---
		EXEC MULTIPLE_UPDATE				@SUBSTRING = 'щяйхлня'			,@PRODUCER	= 'щяйхлня',				@PROD_TYPE ='опндсйрш'	,@PROD_SUBTYPE='лнкнвмше опндсйрш'				,@PROD_CATEGORY='лнпнфемне'				,@PROD_SUBCATEGORY='' ---
		EXEC MULTIPLE_UPDATE				@SUBSTRING = 'ярхкэ опндсйр'	,@PRODUCER	= 'ярхкэ опндсйр',			@PROD_TYPE ='опндсйрш'	,@PROD_SUBTYPE='пшаю/пшанопндсйрш'				,@PROD_CATEGORY=''						,@PROD_SUBCATEGORY='' ---
		EXEC MULTIPLE_UPDATE				@SUBSTRING = 'яхтсд тпщь'		,@PRODUCER	= 'яхтсд тпщь',				@PROD_TYPE ='опндсйрш'	,@PROD_SUBTYPE='пшаю/пшанопндсйрш'				,@PROD_CATEGORY=''						,@PROD_SUBCATEGORY='' ---
		EXEC MULTIPLE_UPDATE				@SUBSTRING = 'яой'				,@PRODUCER	= 'яой',					@PROD_TYPE ='опндсйрш'	,@PROD_SUBTYPE='лъян/орхжю'						,@PROD_CATEGORY=''						,@PROD_SUBCATEGORY='' ---
		EXEC MULTIPLE_UPDATE				@SUBSTRING = 'хвюкйх'			,@PRODUCER	= 'хвюкйх',					@PROD_TYPE ='опндсйрш'	,@PROD_SUBTYPE='яшпш'							,@PROD_CATEGORY=''						,@PROD_SUBCATEGORY='' ---
		EXEC MULTIPLE_UPDATE				@SUBSTRING = 'слюкюр'			,@PRODUCER	= 'слюкюр',					@PROD_TYPE ='опндсйрш'	,@PROD_SUBTYPE='яшпш'							,@PROD_CATEGORY=''						,@PROD_SUBCATEGORY='' ---
		EXEC MULTIPLE_UPDATE				@SUBSTRING = 'псяяйне лнпе'		,@PRODUCER	= 'псяяйне лнпе',			@PROD_TYPE ='опндсйрш'	,@PROD_SUBTYPE='пшаю/пшанопндсйрш'				,@PROD_CATEGORY=''						,@PROD_SUBCATEGORY='' ---
		EXEC MULTIPLE_UPDATE				@SUBSTRING = 'аюкх'				,@PRODUCER	= 'аюкх',					@PROD_TYPE ='опндсйрш'	,@PROD_SUBTYPE='йскхмюпхъ/онкстюапхйюрш'		,@PROD_CATEGORY=''						,@PROD_SUBCATEGORY='' ---
		EXEC MULTIPLE_UPDATE				@SUBSTRING = 'яйр'				,@PRODUCER	= 'яйр',					@PROD_TYPE ='опндсйрш'	,@PROD_SUBTYPE='лъян/орхжю'						,@PROD_CATEGORY=''						,@PROD_SUBCATEGORY='' ---
		EXEC MULTIPLE_UPDATE				@SUBSTRING = 'юкэцепд'			,@PRODUCER	= 'юкэцепд',				@PROD_TYPE ='опндсйрш'	,@PROD_SUBTYPE='яшпш'							,@PROD_CATEGORY=''						,@PROD_SUBCATEGORY='' ---
		EXEC MULTIPLE_UPDATE_PRODCATEGORY	@SUBSTRING = 'рнляйне лнкнйн'	,@PRODUCER	= 'рнляйне лнкнйн',			@PROD_TYPE ='опндсйрш'	,@PROD_SUBTYPE='охыебше фхпш'					,@PROD_CATEGORY='люякн якхбнвмне'						,@PROD_SUBCATEGORY='' ---
		EXEC MULTIPLE_UPDATE				@SUBSTRING = 'рнляйне лнкнйн'	,@PRODUCER	= 'рнляйне лнкнйн',			@PROD_TYPE ='опндсйрш'	,@PROD_SUBTYPE='лнкнвмше опндсйрш'				,@PROD_CATEGORY=''						,@PROD_SUBCATEGORY='' ---
		EXEC MULTIPLE_UPDATE				@SUBSTRING = 'рнляйхе лекэмхжш'	,@PRODUCER	= 'рнляйхе лекэмхжш',		@PROD_TYPE ='опндсйрш'	,@PROD_SUBTYPE='аюйюкеъ'						,@PROD_CATEGORY='лсйю'					,@PROD_SUBCATEGORY='' ---
		EXEC MULTIPLE_UPDATE				@SUBSTRING = 'рнляйхе оейюпмх'	,@PRODUCER	= 'рнляйхе оейюпмх',		@PROD_TYPE ='опндсйрш'	,@PROD_SUBTYPE='йнмдхрепяйхе хгдекхъ/якюднярх'	,@PROD_CATEGORY='оевемэе'				,@PROD_SUBCATEGORY='' ---
		EXEC MULTIPLE_UPDATE				@SUBSTRING = 'рнляйхи йнмдхреп'	,@PRODUCER	= 'рнляйхи йнмдхреп',		@PROD_TYPE ='опндсйрш'	,@PROD_SUBTYPE='йнмдхрепяйхе хгдекхъ/якюднярх'	,@PROD_CATEGORY='оевемэе'				,@PROD_SUBCATEGORY='' ---
		EXEC MULTIPLE_UPDATE				@SUBSTRING = 'рнляйхи йнмдхреп'	,@PRODUCER	= 'рнляйхи йнмдхреп',		@PROD_TYPE ='опндсйрш'	,@PROD_SUBTYPE='йнмдхрепяйхе хгдекхъ/якюднярх'	,@PROD_CATEGORY='оевемэе'				,@PROD_SUBCATEGORY='' ---
		EXEC MULTIPLE_UPDATE				@SUBSTRING = 'рнляйне ахярпн'	,@PRODUCER	= 'рнляйне ахярпн',			@PROD_TYPE ='опндсйрш'	,@PROD_SUBTYPE='йскхмюпхъ/онкстюапхйюрш'		,@PROD_CATEGORY=''						,@PROD_SUBCATEGORY='' ---
		EXEC MULTIPLE_UPDATE_PRODCATEGORY	@SUBSTRING = 'нью'				,@PRODUCER	= 'нью',					@PROD_TYPE ='юкцнйнкэ'	,@PROD_SUBTYPE='охбн'							,@PROD_CATEGORY='пняяхъ'				,@PROD_SUBCATEGORY='' ---
		EXEC MULTIPLE_UPDATE				@SUBSTRING = 'бнярнвмши йюпюбюм',@PRODUCER	= 'бнярнвмши йюпюбюм',		@PROD_TYPE ='опндсйрш'	,@PROD_SUBTYPE='йскхмюпхъ/онкстюапхйюрш'		,@PROD_CATEGORY=''						,@PROD_SUBCATEGORY='' ---
		EXEC MULTIPLE_UPDATE				@SUBSTRING = 'оп-бн юот фелвсфхмю ярюбпнонкэъ' ,@PRODUCER= 'оп-бн юот фелвсфхмю ярюбпнонкэъ',@PROD_TYPE ='юкйнцнкэ',@PROD_SUBTYPE='йнмэъй'	,@PROD_CATEGORY='пняяхъ'				,@PROD_SUBCATEGORY='' ---
		EXEC MULTIPLE_UPDATE				@SUBSTRING = 'йпюяйх керю'		,@PRODUCER	= 'йпюяйх керю',			@PROD_TYPE ='опндсйрш'	,@PROD_SUBTYPE='нбных/гекемэ'					,@PROD_CATEGORY=''						,@PROD_SUBCATEGORY='' ---
		EXEC MULTIPLE_UPDATE				@SUBSTRING = 'яюмрю апелнп'		,@PRODUCER	= 'яюмрю апелнп',			@PROD_TYPE ='опндсйрш'	,@PROD_SUBTYPE='пшаю/пшанопндсйрш'				,@PROD_CATEGORY=''						,@PROD_SUBCATEGORY='' ---
		EXEC MULTIPLE_UPDATE_PRODCATEGORY	@SUBSTRING = 'юия ярюп'			,@PRODUCER	= 'юия ярюп',				@PROD_TYPE ='опндсйрш'	,@PROD_SUBTYPE='тпсйрш/ъцндш'					,@PROD_CATEGORY='ялнпндхмю'				,@PROD_SUBCATEGORY='гюлнпнгйю' ---
		EXEC MULTIPLE_UPDATE_PRODCATEGORY	@SUBSTRING = 'юия ярюп'			,@PRODUCER	= 'юия ярюп',				@PROD_TYPE ='опндсйрш'	,@PROD_SUBTYPE='нпеух/цпхаш'					,@PROD_CATEGORY='ьюлохмэнмш'			,@PROD_SUBCATEGORY='гюлнпнгйю' ---
		EXEC MULTIPLE_UPDATE				@SUBSTRING = 'юия ярюп'			,@PRODUCER	= 'юия ярюп',				@PROD_TYPE ='опндсйрш'	,@PROD_SUBTYPE='нбных/гекемэ'					,@PROD_CATEGORY=''						,@PROD_SUBCATEGORY='гюлнпнгйю' ---
		EXEC MULTIPLE_UPDATE				@SUBSTRING = 'юбювю'			,@PRODUCER	= 'юбювю',					@PROD_TYPE ='опндсйрш'	,@PROD_SUBTYPE='пшаю/пшанопндсйрш'				,@PROD_CATEGORY='хйпю'					,@PROD_SUBCATEGORY='' ---
		EXEC MULTIPLE_UPDATE				@SUBSTRING = 'опндсйрш нр люякнбни',@PRODUCER= 'опндсйрш нр люякнбни',	@PROD_TYPE ='опндсйрш',@PROD_SUBTYPE='йнмдхрепяйхе хгдекхъ/якюднярх'	,@PROD_CATEGORY=''						,@PROD_SUBCATEGORY='' ---
		EXEC MULTIPLE_UPDATE				@SUBSTRING = 'псяяйхи ункнд'	,@PRODUCER	= 'псяяйхи ункнд',			@PROD_TYPE ='опндсйрш'	,@PROD_SUBTYPE='лнкнвмше опндсйрш'				,@PROD_CATEGORY='лнпнфемне'				,@PROD_SUBCATEGORY='' ---
		EXEC MULTIPLE_UPDATE				@SUBSTRING = 'юмцюпхъ'			,@PRODUCER	= 'юмцюпхъ',				@PROD_TYPE ='опндсйрш'	,@PROD_SUBTYPE='лнкнвмше опндсйрш'				,@PROD_CATEGORY='лнпнфемне'				,@PROD_SUBCATEGORY='' ---
		EXEC MULTIPLE_UPDATE_PRODCATEGORY	@SUBSTRING = 'якюбхжю'			,@PRODUCER	= 'якюбхжю',				@PROD_TYPE ='опндсйрш'	,@PROD_SUBTYPE='лнкнвмше опндсйрш'				,@PROD_CATEGORY='лнпнфемне'				,@PROD_SUBCATEGORY='' ---
		EXEC MULTIPLE_UPDATE				@SUBSTRING = 'якюбхжю'			,@PRODUCER	= 'якюбхжю',				@PROD_TYPE ='опндсйрш'	,@PROD_SUBTYPE='бндю'							,@PROD_CATEGORY=''						,@PROD_SUBCATEGORY='' ---
		EXEC MULTIPLE_UPDATE				@SUBSTRING = 'лхкйнл'			,@PRODUCER	= 'лхкйнл',					@PROD_TYPE ='опндсйрш'	,@PROD_SUBTYPE='лнкнвмше опндсйрш'				,@PROD_CATEGORY='лнпнфемне'				,@PROD_SUBCATEGORY='' ---
		EXEC MULTIPLE_UPDATE_PRODCATEGORY	@SUBSTRING = 'люпя'				,@PRODUCER	= 'люпя',					@PROD_TYPE ='опндсйрш'	,@PROD_SUBTYPE='лнкнвмше опндсйрш'				,@PROD_CATEGORY='лнпнфемне'				,@PROD_SUBCATEGORY='' ---
		EXEC MULTIPLE_UPDATE				@SUBSTRING = 'люпяхюмйю'		,@PRODUCER	= 'люпяхюмйю',				@PROD_TYPE ='опндсйрш'	,@PROD_SUBTYPE='йнмдхрепяйхе хгдекхъ/якюднярх'	,@PROD_CATEGORY='йнмтерш'				,@PROD_SUBCATEGORY='' ---
		EXEC MULTIPLE_UPDATE				@SUBSTRING = 'люпяекеж'			,@PRODUCER	= 'люпяекеж',				@PROD_TYPE ='рнбюпш'	,@PROD_SUBTYPE='йнялерхйю/цхцхемю'				,@PROD_CATEGORY='цекэ дкъ дсью'			,@PROD_SUBCATEGORY='' ---
		EXEC MULTIPLE_UPDATE				@SUBSTRING = ' люпя '			,@PRODUCER	= 'люпя',					@PROD_TYPE ='опндсйрш'	,@PROD_SUBTYPE='йнмдхрепяйхе хгдекхъ/якюднярх'	,@PROD_CATEGORY='ьнй.аюрнмвхй'			,@PROD_SUBCATEGORY='' ---
		EXEC MULTIPLE_UPDATE				@SUBSTRING = 'юкэрепбеяр'		,@PRODUCER	= 'юкэрепбеяр',				@PROD_TYPE ='опндсйрш'	,@PROD_SUBTYPE='лнкнвмше опндсйрш'				,@PROD_CATEGORY='лнпнфемне'				,@PROD_SUBCATEGORY='' ---
		EXEC MULTIPLE_UPDATE				@SUBSTRING = 'йюлохмю'			,@PRODUCER	= 'йюлохмю',				@PROD_TYPE ='опндсйрш'	,@PROD_SUBTYPE='лнкнвмше опндсйрш'				,@PROD_CATEGORY=''						,@PROD_SUBCATEGORY='' ---
		--EXEC MULTIPLE_UPDATE				@SUBSTRING = 'цняохпнм'			,@PRODUCER	= 'цняохпнм',				@PROD_TYPE =''			,@PROD_SUBTYPE=''								,@PROD_CATEGORY=''						,@PROD_SUBCATEGORY='' ---
		--EXEC MULTIPLE_UPDATE				@SUBSTRING = 'яепбхяямюа'		,@PRODUCER	= 'яепбхяямюа',				@PROD_TYPE =''			,@PROD_SUBTYPE=''								,@PROD_CATEGORY=''						,@PROD_SUBCATEGORY='' ---
		EXEC MULTIPLE_UPDATE				@SUBSTRING = 'юркюмрю-яепбхя'	,@PRODUCER	= 'юркюмрю-яепбхя',			@PROD_TYPE ='опндсйрш'	,@PROD_SUBTYPE='лъян/орхжю'						,@PROD_CATEGORY=''						,@PROD_SUBCATEGORY='' ---
		EXEC MULTIPLE_UPDATE				@SUBSTRING = 'ухмрюи'			,@PRODUCER	= 'ухмрюи',					@PROD_TYPE ='опндсйрш'	,@PROD_SUBTYPE='нпеух/цпхаш'					,@PROD_CATEGORY=''						,@PROD_SUBCATEGORY='' ---
		EXEC MULTIPLE_UPDATE				@SUBSTRING = 'днапше рпюдхжхх'	,@PRODUCER	= 'днапше рпюдхжхх',		@PROD_TYPE ='опндсйрш'	,@PROD_SUBTYPE='лнкнвмше опндсйрш'				,@PROD_CATEGORY=''						,@PROD_SUBCATEGORY='гюлнпнгйю' ---
		EXEC MULTIPLE_UPDATE				@SUBSTRING='псяяйюъ пшамюъ йнлоюмхъ',@PRODUCER='псяяйюъ пшамюъ йнлоюмхъ',@PROD_TYPE ='опндсйрш'	,@PROD_SUBTYPE='пшаю/пшанопндсйрш'				,@PROD_CATEGORY=''						,@PROD_SUBCATEGORY='' ---
		EXEC MULTIPLE_UPDATE				@SUBSTRING = 'онкрхьнй'			,@PRODUCER	= 'онкрхьнй',				@PROD_TYPE ='опндсйрш'	,@PROD_SUBTYPE='пшаю/пшанопндсйрш'				,@PROD_CATEGORY=''						,@PROD_SUBCATEGORY='' ---
		EXEC MULTIPLE_UPDATE				@SUBSTRING = 'рд нияреп'		,@PRODUCER	= 'рд нияреп',				@PROD_TYPE ='опндсйрш'	,@PROD_SUBTYPE='пшаю/пшанопндсйрш'				,@PROD_CATEGORY=''						,@PROD_SUBCATEGORY='' ---
		EXEC MULTIPLE_UPDATE				@SUBSTRING = 'длг'				,@PRODUCER	= 'длг',					@PROD_TYPE ='опндсйрш'	,@PROD_SUBTYPE='лнкнвмше опндсйрш'				,@PROD_CATEGORY='яшпнй рбнпнфмши'		,@PROD_SUBCATEGORY='' ---
		EXEC MULTIPLE_UPDATE				@SUBSTRING = 'кхдеп'			,@PRODUCER	= 'кхдеп',					@PROD_TYPE ='опндсйрш'	,@PROD_SUBTYPE='укеанаскнвмше хгдекхъ/бшоевйю'	,@PROD_CATEGORY=''						,@PROD_SUBCATEGORY='' ---
		EXEC MULTIPLE_UPDATE				@SUBSTRING = 'бйся бнярнйю'		,@PRODUCER	= 'бйся бнярнйю',			@PROD_TYPE ='опндсйрш'	,@PROD_SUBTYPE='йскхмюпхъ/онкстюапхйюрш'		,@PROD_CATEGORY=''						,@PROD_SUBCATEGORY='' ---
		EXEC MULTIPLE_UPDATE				@SUBSTRING = 'аекнбефяйхе яшпш'	,@PRODUCER	= 'аекнбефяйхе яшпш',		@PROD_TYPE ='опндсйрш'	,@PROD_SUBTYPE='яшпш'							,@PROD_CATEGORY=''						,@PROD_SUBCATEGORY='' ---
		EXEC MULTIPLE_UPDATE				@SUBSTRING = 'псяяйхиункнд'		,@PRODUCER	= 'псяяйхи ункнд',			@PROD_TYPE ='опндсйрш'	,@PROD_SUBTYPE='лнкнвмше опндсйрш'				,@PROD_CATEGORY='лнпнфемне'				,@PROD_SUBCATEGORY='щяйхлн' ---
		EXEC MULTIPLE_UPDATE				@SUBSTRING = 'хо охбхйнб'		,@PRODUCER	= 'хо охбхйнб',				@PROD_TYPE ='опндсйрш'	,@PROD_SUBTYPE='пшаю/пшанопндсйрш'				,@PROD_CATEGORY='онкстюапхйюр хг ысйх'	,@PROD_SUBCATEGORY='гюлнпнгйю' ---
		EXEC MULTIPLE_UPDATE				@SUBSTRING='ме рнкэйн дкъ ябнху',@PRODUCER	= 'ме рнкэйн дкъ ябнху',	@PROD_TYPE =''			,@PROD_SUBTYPE=''								,@PROD_CATEGORY=''						,@PROD_SUBCATEGORY='' ---
		EXEC MULTIPLE_UPDATE				@SUBSTRING = 'онкъпх'			,@PRODUCER	= 'онкъпхя',				@PROD_TYPE ='опндсйрш'	,@PROD_SUBTYPE='лнкнвмше опндсйрш'				,@PROD_CATEGORY='лнпнфемне'				,@PROD_SUBCATEGORY='' ---
		EXEC MULTIPLE_UPDATE_PRODCATEGORY	@SUBSTRING = 'екюмэ'			,@PRODUCER	= 'екюмэ',					@PROD_TYPE ='опндсйрш'	,@PROD_SUBTYPE='яшпш'							,@PROD_CATEGORY='яшп'					,@PROD_SUBCATEGORY='яшп окюбкеммши' ---
		EXEC MULTIPLE_UPDATE_PRODCATEGORY	@SUBSTRING = 'хмяйне'			,@PRODUCER	= 'хмяйне',					@PROD_TYPE ='опндсйрш'	,@PROD_SUBTYPE='ъижю'							,@PROD_CATEGORY='ъижн'						,@PROD_SUBCATEGORY='' ---
		EXEC MULTIPLE_UPDATE				@SUBSTRING = 'мюжхнмюкэмши ярюмдюпр',@PRODUCER= 'мюжхнмюкэмши ярюмдюпр',@PROD_TYPE ='опндсйрш'	,@PROD_SUBTYPE='ъижю'						,@PROD_CATEGORY=''						,@PROD_SUBCATEGORY='' ---
		EXEC MULTIPLE_UPDATE				@SUBSTRING = 'MLEKARA SABAC'	,@PRODUCER	= 'MLEKARA SABAC',			@PROD_TYPE ='опндсйрш'	,@PROD_SUBTYPE='яшпш'							,@PROD_CATEGORY=''						,@PROD_SUBCATEGORY='' ---
		EXEC MULTIPLE_UPDATE				@SUBSTRING = 'ъпйне српн'		,@PRODUCER	= 'ъпйне српн',				@PROD_TYPE ='опндсйрш'	,@PROD_SUBTYPE='ъижю'							,@PROD_CATEGORY=''						,@PROD_SUBCATEGORY='' ---
		EXEC MULTIPLE_UPDATE				@SUBSTRING = 'кхаепн'			,@PRODUCER	= 'кхаепн',					@PROD_TYPE ='рнбюпш'	,@PROD_SUBTYPE='рнбюпш дкъ дереи'				,@PROD_CATEGORY='ондцсгмхйх'	,@PROD_SUBCATEGORY='' ---
		EXEC MULTIPLE_UPDATE				@SUBSTRING = 'оюлоепя'			,@PRODUCER	= 'оюлоепя',				@PROD_TYPE ='рнбюпш'	,@PROD_SUBTYPE='рнбюпш дкъ дереи'				,@PROD_CATEGORY='ондцсгмхйх'	,@PROD_SUBCATEGORY='' ---
		EXEC MULTIPLE_UPDATE				@SUBSTRING = 'фхккер'			,@PRODUCER	= 'фхккер',					@PROD_TYPE ='рнбюпш'	,@PROD_SUBTYPE='йнялерхйю/цхцхемю'				,@PROD_CATEGORY=''				,@PROD_SUBCATEGORY='' ---
		EXEC MULTIPLE_UPDATE				@SUBSTRING ='лепжючыхи йюпюмдюь',@PRODUCER	= 'лепжючыхи йюпюмдюь',		@PROD_TYPE ='опндсйрш'	,@PROD_SUBTYPE='аюйюкеъ'						,@PROD_CATEGORY='охыебни йпюяхрекэ'	,@PROD_SUBCATEGORY='' ---
		EXEC MULTIPLE_UPDATE				@SUBSTRING = ' релю'			,@PRODUCER	= 'р╗лю'			,		@PROD_TYPE ='опндсйрш'	,@PROD_SUBTYPE='деряйне охрюмхе'				,@PROD_CATEGORY=''	,@PROD_SUBCATEGORY='' ---
		EXEC MULTIPLE_UPDATE				@SUBSTRING = ' р╗лю'			,@PRODUCER	= 'р╗лю'			,		@PROD_TYPE ='опндсйрш'	,@PROD_SUBTYPE='деряйне охрюмхе'				,@PROD_CATEGORY=''	,@PROD_SUBCATEGORY='' ---
		EXEC MULTIPLE_UPDATE				@SUBSTRING = 'ахн-аюкюмя'		,@PRODUCER	= 'чмхоюй'			,		@PROD_TYPE ='опндсйрш'	,@PROD_SUBTYPE='лнкнвмше опндсйрш'				,@PROD_CATEGORY=''	,@PROD_SUBCATEGORY='' ---
		EXEC MULTIPLE_UPDATE				@SUBSTRING = 'юйрхбхъ'			,@PRODUCER	= 'дюмнм',					@PROD_TYPE ='опндсйрш'	,@PROD_SUBTYPE='лнкнвмше опндсйрш'				,@PROD_CATEGORY=''	,@PROD_SUBCATEGORY='' ---
		EXEC MULTIPLE_UPDATE				@SUBSTRING = 'юйрхбхю'			,@PRODUCER	= 'дюмнм',					@PROD_TYPE ='опндсйрш'	,@PROD_SUBTYPE='лнкнвмше опндсйрш'				,@PROD_CATEGORY=''	,@PROD_SUBCATEGORY='' ---
		EXEC MULTIPLE_UPDATE				@SUBSTRING = 'юкоем цнкд'		,@PRODUCER	= 'юкэоем цнкд',			@PROD_TYPE ='опндсйрш'	,@PROD_SUBTYPE='йнмдхрепяйхе хгдекхъ/якюднярх'	,@PROD_CATEGORY=''	,@PROD_SUBCATEGORY='' ---
		EXEC MULTIPLE_UPDATE				@SUBSTRING = 'юкэоем цнкд'		,@PRODUCER	= 'юкэоем цнкд',			@PROD_TYPE ='опндсйрш'	,@PROD_SUBTYPE='йнмдхрепяйхе хгдекхъ/якюднярх'	,@PROD_CATEGORY=''	,@PROD_SUBCATEGORY='' ---
		EXEC MULTIPLE_UPDATE				@SUBSTRING = 'тпсррхя'			,@PRODUCER	= 'тпсррхя'	,				@PROD_TYPE ='опндсйрш'	,@PROD_SUBTYPE='лнкнвмше опндсйрш'				,@PROD_CATEGORY=''	,@PROD_SUBCATEGORY='' ---
		EXEC MULTIPLE_UPDATE				@SUBSTRING = 'дюпопндсйр'		,@PRODUCER	= 'дюпопндсйр'	,			@PROD_TYPE ='опндсйрш'	,@PROD_SUBTYPE='лнкнвмше опндсйрш'				,@PROD_CATEGORY='цкюгхпнбюмши яшпнй'	,@PROD_SUBCATEGORY='' ---
		EXEC MULTIPLE_UPDATE				@SUBSTRING = 'псяэукеа'			,@PRODUCER	= 'псяэукеа'			,	@PROD_TYPE ='опндсйрш'	,@PROD_SUBTYPE='укеанаскнвмше хгдекхъ/бшоевйю'	,@PROD_CATEGORY='янкнлйю'	,@PROD_SUBCATEGORY=''
		EXEC MULTIPLE_UPDATE				@SUBSTRING = 'укеамши яоюя'		,@PRODUCER	= 'укеамши яоюя'		,	@PROD_TYPE ='опндсйрш'	,@PROD_SUBTYPE='укеанаскнвмше хгдекхъ/бшоевйю'	,@PROD_CATEGORY='оевемэе'	,@PROD_SUBCATEGORY=''
		EXEC MULTIPLE_UPDATE_PRODCATEGORY	@SUBSTRING = 'рюилкх'			,@PRODUCER	= 'рюилкх',					@PROD_TYPE ='опндсйрш'	,@PROD_SUBTYPE='охрюмхе дкъ фхбнрмшу'	,@PROD_CATEGORY='йнпл'			,@PROD_SUBCATEGORY=''
		EXEC MULTIPLE_UPDATE_PRODCATEGORY	@SUBSTRING = 'рюилкх'			,@PRODUCER	= 'рюилкх',					@PROD_TYPE ='рнбюпш'	,@PROD_SUBTYPE='рнбюпш дкъ фхбнрмшу'	,@PROD_CATEGORY='мюонкмхрекэ'	,@PROD_SUBCATEGORY=''
		EXEC MULTIPLE_UPDATE				@SUBSTRING = ' рюиля'			,@PRODUCER	= 'рюиля'			,		@PROD_TYPE ='юкйнцнкэ'	,@PROD_SUBTYPE='бхяйх'					,@PROD_CATEGORY='яью'	,@PROD_SUBCATEGORY='аспанм' ---
		EXEC MULTIPLE_UPDATE				@SUBSTRING = ' рюил'			,@PRODUCER	= 'рюил'			,		@PROD_TYPE ='опндсйрш'	,@PROD_SUBTYPE='йскхмюпхъ/онкстюапхйюрш',@PROD_CATEGORY=''	,@PROD_SUBCATEGORY='' ---
		EXEC MULTIPLE_UPDATE				@SUBSTRING = '/рюил'			,@PRODUCER	= 'рюил'			,		@PROD_TYPE ='опндсйрш'	,@PROD_SUBTYPE='йскхмюпхъ/онкстюапхйюрш',@PROD_CATEGORY=''	,@PROD_SUBCATEGORY='' ---
		EXEC MULTIPLE_UPDATE				@SUBSTRING = 'CHAMMY'			,@PRODUCER	= 'CHAMMY',					@PROD_TYPE ='опндсйрш'	,@PROD_SUBTYPE='охрюмхе дкъ фхбнрмшу'	,@PROD_CATEGORY='йнпл'			,@PROD_SUBCATEGORY=''
		EXEC MULTIPLE_UPDATE				@SUBSTRING = 'бхяйюя'			,@PRODUCER	= 'бхяйюя',					@PROD_TYPE ='опндсйрш'	,@PROD_SUBTYPE='охрюмхе дкъ фхбнрмшу'	,@PROD_CATEGORY='йнпл'			,@PROD_SUBCATEGORY=''
		EXEC MULTIPLE_UPDATE				@SUBSTRING = 'йхрейер'			,@PRODUCER	= 'йхрейер',				@PROD_TYPE ='опндсйрш'	,@PROD_SUBTYPE='охрюмхе дкъ фхбнрмшу'	,@PROD_CATEGORY='йнпл'			,@PROD_SUBCATEGORY=''
		EXEC MULTIPLE_UPDATE				@SUBSTRING = 'йхрхйщр'			,@PRODUCER	= 'йхрейер',				@PROD_TYPE ='опндсйрш'	,@PROD_SUBTYPE='охрюмхе дкъ фхбнрмшу'	,@PROD_CATEGORY='йнпл'			,@PROD_SUBCATEGORY=''
		EXEC MULTIPLE_UPDATE				@SUBSTRING = 'йхрейщр'			,@PRODUCER	= 'йхрейер',				@PROD_TYPE ='опндсйрш'	,@PROD_SUBTYPE='охрюмхе дкъ фхбнрмшу'	,@PROD_CATEGORY='йнпл'			,@PROD_SUBCATEGORY=''
		EXEC MULTIPLE_UPDATE				@SUBSTRING = 'текхйя'			,@PRODUCER	= 'текхйя',					@PROD_TYPE ='опндсйрш'	,@PROD_SUBTYPE='охрюмхе дкъ фхбнрмшу'	,@PROD_CATEGORY='йнпл'			,@PROD_SUBCATEGORY=''
		EXEC MULTIPLE_UPDATE				@SUBSTRING = 'тпхяйхя'			,@PRODUCER	= 'тпхяйхя',				@PROD_TYPE ='опндсйрш'	,@PROD_SUBTYPE='охрюмхе дкъ фхбнрмшу'	,@PROD_CATEGORY='йнпл'			,@PROD_SUBCATEGORY=''
		EXEC MULTIPLE_UPDATE				@SUBSTRING = 'оспхмю бюм'		,@PRODUCER	= 'оспхмю бюм',				@PROD_TYPE ='опндсйрш'	,@PROD_SUBTYPE='охрюмхе дкъ фхбнрмшу'	,@PROD_CATEGORY='йнпл'			,@PROD_SUBCATEGORY=''
		EXEC MULTIPLE_UPDATE				@SUBSTRING = 'оедхцпх'			,@PRODUCER	= 'оедхцпх',				@PROD_TYPE ='опндсйрш'	,@PROD_SUBTYPE='охрюмхе дкъ фхбнрмшу'	,@PROD_CATEGORY='йнпл'			,@PROD_SUBCATEGORY=''
		EXEC MULTIPLE_UPDATE				@SUBSTRING = 'оедхцх'			,@PRODUCER	= 'оедхцх',					@PROD_TYPE ='опндсйрш'	,@PROD_SUBTYPE='охрюмхе дкъ фхбнрмшу'	,@PROD_CATEGORY='йнпл'			,@PROD_SUBCATEGORY=''
		EXEC MULTIPLE_UPDATE				@SUBSTRING = 'ахц-анм'			,@PRODUCER	= 'ахц-анм'			,		@PROD_TYPE ='опндсйрш'	,@PROD_SUBTYPE='йскхмюпхъ/онкстюапхйюрш',@PROD_CATEGORY=''	,@PROD_SUBCATEGORY='' ---
		EXEC MULTIPLE_UPDATE				@SUBSTRING = 'ялюий'			,@PRODUCER	= 'ялюий'			,		@PROD_TYPE ='опндсйрш'	,@PROD_SUBTYPE='йскхмюпхъ/онкстюапхйюрш',@PROD_CATEGORY=''	,@PROD_SUBCATEGORY='' ---
		EXEC MULTIPLE_UPDATE				@SUBSTRING = 'пюднярх йсумх'	,@PRODUCER	= 'пюднярх йсумх'	,		@PROD_TYPE ='опндсйрш'	,@PROD_SUBTYPE='йскхмюпхъ/онкстюапхйюрш',@PROD_CATEGORY=''	,@PROD_SUBCATEGORY='' ---
		EXEC MULTIPLE_UPDATE_PRODCATEGORY	@SUBSTRING = 'ахпйю'			,@PRODUCER	= 'ахпйю',					@PROD_TYPE ='опндсйрш'	,@PROD_SUBTYPE='нпеух/цпхаш'					,@PROD_CATEGORY='тхярюьйх'			,@PROD_SUBCATEGORY=''
		EXEC MULTIPLE_UPDATE_PRODCATEGORY	@SUBSTRING = 'ахпйю'			,@PRODUCER	= 'ахпйю',					@PROD_TYPE ='опндсйрш'	,@PROD_SUBTYPE='нпеух/цпхаш'					,@PROD_CATEGORY='юпюухя'			,@PROD_SUBCATEGORY=''
		EXEC MULTIPLE_UPDATE_PRODCATEGORY	@SUBSTRING = 'ахпйю'			,@PRODUCER	= 'ахпйю',					@PROD_TYPE ='опндсйрш'	,@PROD_SUBTYPE='йскхмюпхъ/онкстюапхйюрш'		,@PROD_CATEGORY='гюйсяйю'	,@PROD_SUBCATEGORY=''
		EXEC MULTIPLE_UPDATE_PRODCATEGORY	@SUBSTRING = 'ахпйю'			,@PRODUCER	= 'ахпйю',					@PROD_TYPE ='опндсйрш'	,@PROD_SUBTYPE='йскхмюпхъ/онкстюапхйюрш'		,@PROD_CATEGORY='цпемйх'	,@PROD_SUBCATEGORY=''
		EXEC MULTIPLE_UPDATE_PRODCATEGORY	@SUBSTRING = 'лняйхрнк'			,@PRODUCER	= 'лняйхрнк',				@PROD_TYPE ='рнбюпш'	,@PROD_SUBTYPE='рнбюпш дкъ днлю х дювх'			,@PROD_CATEGORY=''			,@PROD_SUBCATEGORY=''
		EXEC MULTIPLE_UPDATE_PRODCATEGORY	@SUBSTRING = 'пеид'				,@PRODUCER	= 'пеид',					@PROD_TYPE ='рнбюпш'	,@PROD_SUBTYPE='рнбюпш дкъ днлю х дювх'			,@PROD_CATEGORY=''			,@PROD_SUBCATEGORY=''
		EXEC MULTIPLE_UPDATE				@SUBSTRING = 'йдб'				,@PRODUCER	= 'йдб',					@PROD_TYPE ='опндсйрш'	,@PROD_SUBTYPE='йнмдхрепяйхе хгдекхъ/якюднярх'	,@PROD_CATEGORY=''	,@PROD_SUBCATEGORY='' ---
		EXEC MULTIPLE_UPDATE				@SUBSTRING = '7фекюмхи'			,@PRODUCER	= '7фекюмхи',				@PROD_TYPE ='опндсйрш'	,@PROD_SUBTYPE='йнмдхрепяйхе хгдекхъ/якюднярх'	,@PROD_CATEGORY=''	,@PROD_SUBCATEGORY='' ---
		EXEC MULTIPLE_UPDATE				@SUBSTRING = 'дюмнм'			,@PRODUCER	= 'дюмнм'	,				@PROD_TYPE ='опндсйрш'	,@PROD_SUBTYPE='лнкнвмше опндсйрш'				,@PROD_CATEGORY=''	,@PROD_SUBCATEGORY='' ---
		EXEC MULTIPLE_UPDATE				@SUBSTRING = 'тпсрнмъмъ'		,@PRODUCER	= 'тпсрнмъмъ'	,			@PROD_TYPE ='опндсйрш'	,@PROD_SUBTYPE='деряйне охрюмхе'				,@PROD_CATEGORY=''	,@PROD_SUBCATEGORY='' ---
		EXEC MULTIPLE_UPDATE				@SUBSTRING = 'бйсямнтт'			,@PRODUCER	= 'бйсямнтт'			,	@PROD_TYPE ='опндсйрш'	,@PROD_SUBTYPE='йнмдхрепяйхе хгдекхъ/якюднярх'	,@PROD_CATEGORY='оевемэе'	,@PROD_SUBCATEGORY=''
		EXEC MULTIPLE_UPDATE				@SUBSTRING = 'псяяйюпр'			,@PRODUCER	= 'псяяйюпр'		   ,	@PROD_TYPE ='опндсйрш'	,@PROD_SUBTYPE='йнмдхрепяйхе хгдекхъ/якюднярх'	,@PROD_CATEGORY='йейя'		,@PROD_SUBCATEGORY=''
		EXEC MULTIPLE_UPDATE				@SUBSTRING = 'уай'				,@PRODUCER	= 'уай'					,	@PROD_TYPE ='опндсйрш'	,@PROD_SUBTYPE='йнмдхрепяйхе хгдекхъ/якюднярх'	,@PROD_CATEGORY=''			,@PROD_SUBCATEGORY=''
		EXEC MULTIPLE_UPDATE				@SUBSTRING = 'яекн оюярсьйхмн'	,@PRODUCER	= 'яекн оюярсьйхмн'		,	@PROD_TYPE ='опндсйрш'	,@PROD_SUBTYPE='йнмдхрепяйхе хгдекхъ/якюднярх'	,@PROD_CATEGORY='бюткх'		,@PROD_SUBCATEGORY=''
		EXEC MULTIPLE_UPDATE				@SUBSTRING = 'яоек╗мнй'			,@PRODUCER	= 'яоек╗мнй'	,			@PROD_TYPE ='опндсйрш'	,@PROD_SUBTYPE='деряйне охрюмхе'				,@PROD_CATEGORY=''			,@PROD_SUBCATEGORY='' ---
		EXEC MULTIPLE_UPDATE				@SUBSTRING = 'люрхйю'			,@PRODUCER	= 'люрхйю'		   ,		@PROD_TYPE ='опндсйрш'	,@PROD_SUBTYPE='йнмдхрепяйхе хгдекхъ/якюднярх'	,@PROD_CATEGORY='оевемэе'	,@PROD_SUBCATEGORY=''
		EXEC MULTIPLE_UPDATE				@SUBSTRING = 'лхкйю'			,@PRODUCER	= 'лхкйю',					@PROD_TYPE ='опндсйрш'	,@PROD_SUBTYPE='йнмдхрепяйхе хгдекхъ/якюднярх'	,@PROD_CATEGORY=''	,@PROD_SUBCATEGORY='' ---
		EXEC MULTIPLE_UPDATE				@SUBSTRING = 'юцсью'			,@PRODUCER	= 'юцсью'	,				@PROD_TYPE ='опндсйрш'	,@PROD_SUBTYPE='деряйне охрюмхе'				,@PROD_CATEGORY=''			,@PROD_SUBCATEGORY='' ---
		EXEC MULTIPLE_UPDATE				@SUBSTRING = 'аюасьйхмн ксйньйн',@PRODUCER	= 'аюасьйхмн ксйньйн',		@PROD_TYPE ='опндсйрш'	,@PROD_SUBTYPE='деряйне охрюмхе'				,@PROD_CATEGORY=''			,@PROD_SUBCATEGORY='' ---
		EXEC MULTIPLE_UPDATE				@SUBSTRING = 'яюлх я сяюлх'		,@PRODUCER	= 'яюлх я сяюлх',			@PROD_TYPE ='опндсйрш'	,@PROD_SUBTYPE='деряйне охрюмхе'				,@PROD_CATEGORY=''						,@PROD_SUBCATEGORY='' ---
		EXEC MULTIPLE_UPDATE				@SUBSTRING = 'дюмхяяхлн'		,@PRODUCER	= 'дюмхяяхлн'	,			@PROD_TYPE ='опндсйрш'	,@PROD_SUBTYPE='лнкнвмше опндсйрш'				,@PROD_CATEGORY=''						,@PROD_SUBCATEGORY='' ---
		EXEC MULTIPLE_UPDATE				@SUBSTRING = 'яйнлнпньйю'		,@PRODUCER	= 'яйнлнпньйю'	,			@PROD_TYPE ='опндсйрш'	,@PROD_SUBTYPE='лнкнвмше опндсйрш'				,@PROD_CATEGORY=''						,@PROD_SUBCATEGORY='' ---
		EXEC MULTIPLE_UPDATE				@SUBSTRING = 'йкхмнй'			,@PRODUCER	= 'йкхмнй'	,				@PROD_TYPE ='опндсйрш'	,@PROD_SUBTYPE='лнкнвмше опндсйрш'				,@PROD_CATEGORY='рбнпнц'				,@PROD_SUBCATEGORY='' ---
		EXEC MULTIPLE_UPDATE				@SUBSTRING = 'унукюмд'			,@PRODUCER	= 'унукюмд',				@PROD_TYPE ='опндсйрш'	,@PROD_SUBTYPE='яшпш'							,@PROD_CATEGORY=''						,@PROD_SUBCATEGORY='' ---
		EXEC MULTIPLE_UPDATE				@SUBSTRING = 'бхнкеррю'			,@PRODUCER	= 'бхнкеррю',				@PROD_TYPE ='опндсйрш'	,@PROD_SUBTYPE='яшпш'							,@PROD_CATEGORY=''						,@PROD_SUBCATEGORY='' ---
		EXEC MULTIPLE_UPDATE				@SUBSTRING = 'CHEESE'			,@PRODUCER	= 'CHEESE',					@PROD_TYPE ='опндсйрш'	,@PROD_SUBTYPE='яшпш'							,@PROD_CATEGORY=''						,@PROD_SUBCATEGORY='' ---
		EXEC MULTIPLE_UPDATE				@SUBSTRING = 'MAG'				,@PRODUCER	= 'MAG',					@PROD_TYPE ='опндсйрш'	,@PROD_SUBTYPE='яшпш'							,@PROD_CATEGORY=''						,@PROD_SUBCATEGORY='' ---
		EXEC MULTIPLE_UPDATE				@SUBSTRING = 'реумноюпй'		,@PRODUCER	= 'реумноюпй',				@PROD_TYPE ='рнбюпш'	,@PROD_SUBTYPE='рнбюпш дкъ дереи'				,@PROD_CATEGORY=''						,@PROD_SUBCATEGORY='' ---
		EXEC MULTIPLE_UPDATE				@SUBSTRING = 'йхмдеп'			,@PRODUCER	= 'йхмдеп',					@PROD_TYPE ='опндсйрш'	,@PROD_SUBTYPE='йнмдхрепяйхе хгдекхъ/якюднярх'	,@PROD_CATEGORY=''						,@PROD_SUBCATEGORY='' ---
		EXEC MULTIPLE_UPDATE				@SUBSTRING = 'щбпхйю'			,@PRODUCER	= 'щбпхйю',					@PROD_TYPE ='опндсйрш'	,@PROD_SUBTYPE='йнмдхрепяйхе хгдекхъ/якюднярх'	,@PROD_CATEGORY=''						,@PROD_SUBCATEGORY='' ---
		EXEC MULTIPLE_UPDATE				@SUBSTRING = 'люццх'			,@PRODUCER	= 'люццх',					@PROD_TYPE ='опндсйрш'	,@PROD_SUBTYPE='йскхмюпхъ/онкстюапхйюрш'		,@PROD_CATEGORY=''						,@PROD_SUBCATEGORY=''
		EXEC MULTIPLE_UPDATE				@SUBSTRING ='пндмне ондбнпэе'	,@PRODUCER	= 'пндмне ондбнпэе',		@PROD_TYPE ='опндсйрш'	,@PROD_SUBTYPE='лъян/орхжю'						,@PROD_CATEGORY='йспхжю'				,@PROD_SUBCATEGORY=''
	
		--select * from step0 where product_name like '%ахпйю%'
		--АЕГ СЙЮГЮМХЪ ОПНХГБНДХРЕКЪ, РХО ОПНДСЙРЮ
		EXEC MULTIPLE_UPDATE			   @SUBSTRING = 'ьнйнкюдмне ъижн'	,@PRODUCER	= '',						@PROD_TYPE ='опндсйрш'	,@PROD_SUBTYPE='йнмдхрепяйхе хгдекхъ/якюднярх'	,@PROD_CATEGORY='ьнйнкюдмне ъижн'	,@PROD_SUBCATEGORY='' ---
		EXEC MULTIPLE_UPDATE			   @SUBSTRING = 'LOVELY DOGS'		,@PRODUCER	= '',						@PROD_TYPE ='рнбюпш'	,@PROD_SUBTYPE='рнбюпш дкъ дереи'				,@PROD_CATEGORY='ъижн я хцпсьйни'	,@PROD_SUBCATEGORY='' ---
		EXEC MULTIPLE_UPDATE			   @SUBSTRING = 'хцпсьйю-ячпопхг'	,@PRODUCER	= '',						@PROD_TYPE ='рнбюпш'	,@PROD_SUBTYPE='рнбюпш дкъ дереи'				,@PROD_CATEGORY=''	,@PROD_SUBCATEGORY='' ---
		EXEC MULTIPLE_UPDATE			   @SUBSTRING = 'йпсфйю '			,@PRODUCER	= '',						@PROD_TYPE ='рнбюпш'	,@PROD_SUBTYPE='рнбюпш дкъ днлю х дювх'				,@PROD_CATEGORY='онясдю'	,@PROD_SUBCATEGORY='йпсфйю' ---
		
		/* ЙНМЕЖ ЮРЮБХЯРХВЕЯЙНЦН СВЮЯРЙЮ */
		-------------------------param1_pricefor----------------------------------------------------------
		-------------------------param1_pricefor----------------------------------------------------------
		-------------------------param1_pricefor----------------------------------------------------------
		UPDATE step0
		SET product_name=replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(product_name,	
										'жемш гю йц' , 'жемю гю йц' ),'жемю гю 1йц' , 'жемю гю йц' ),', йц','жемю гю йц'),', жемю гю йц','жемю гю йц'),
										'жемю гю 1ьр','жемю гю ьр'),'крд/ткнпюк/ьр','крд/ткнпюк/жемю гю ьр'),', жемю гю ьр','жемю гю ьр'),
										'жемю гю союйнбйс','жемю гю со'),'жемю гю союй','жемю гю со'),
										'жемю гю 1к','жемю гю к')

		EXEC EXTRACT_SUBSTRING  @SUBSTRING='жемю гю со'
		UPDATE step0 SET param1_pricefor=temp_param where temp_param!=''

		EXEC EXTRACT_SUBSTRING  @SUBSTRING='жемю гю йц'
		UPDATE step0 SET param1_pricefor=temp_param where temp_param!=''

		EXEC EXTRACT_SUBSTRING  @SUBSTRING='жемю гю ьр'
		UPDATE step0 SET param1_pricefor=temp_param where temp_param!=''

		EXEC EXTRACT_SUBSTRING  @SUBSTRING='жемю гю к'
		UPDATE step0 SET param1_pricefor=temp_param where temp_param!=''

		

		UPDATE step0
		SET product_name = replace(replace(replace(replace(replace(product_name,'(',' ( '),')',' ) '),'/',' / '),',бея',', бея') ,'бея,','бея ,')

		UPDATE step0
		SET product_name = replace(product_name,'бея /',' бея /')
		WHERE product_name like '%[0-9]бея /%'

		/* САХПЮЧ КХЬМХЕ ОПНАЕКШ Х ОПНАЕКШ Б ЙНМЖЕ ЯРПНЙХ*/	
		UPDATE step0
		SET product_name = rtrim(replace(product_name,'  ',' '))

		EXEC EXTRACT_SUBSTRING  @SUBSTRING=' бея / яоя'		UPDATE step0 SET param1_ves=temp_param where temp_param!=''
		EXEC EXTRACT_SUBSTRING  @SUBSTRING=' бея / яос'		UPDATE step0 SET param1_ves=temp_param where temp_param!=''
		EXEC EXTRACT_SUBSTRING  @SUBSTRING=' бея / яо'		UPDATE step0 SET param1_ves=temp_param where temp_param!=''
		EXEC EXTRACT_SUBSTRING  @SUBSTRING=' бея '			UPDATE step0 SET param1_ves=temp_param where temp_param!=''
		EXEC EXTRACT_SUBSTRING  @SUBSTRING=' ясу / бея'		UPDATE step0 SET param1_ves=temp_param where temp_param!=''
		EXEC EXTRACT_SUBSTRING  @SUBSTRING=' бея / яоя'		UPDATE step0 SET param1_ves=temp_param where temp_param!=''

		------------------------------param3_gramm------------------------------------------------
		------------------------------param3_gramm------------------------------------------------
		------------------------------param3_gramm------------------------------------------------

		-- НАПЮАНРЙЮ ХЯЙКЧВЕМХИ ДКЪ ЦПЮЛЛ
		UPDATE step0 
		set product_name=replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(product_name,
				'-гекемэ100ц','-гекемэ 100ц'),'йнйреикэ14ц','йнйреикэ 14ц'),'йнте меяйюте оюй.2ц','йнте меяйюте оюй. 2ц'),'ьнйнкюд80ц','ьнйнкюд 80ц'),
				'йеьэч170ц','йеьэч 170ц'),'ьнйнкюд80ц','ьнйнкюд 80ц'),'гкюй.йнйреикэ100ц','гкюй.йнйреикэ 100ц'),'оерхр6ц','оерхр 6ц'),'ъакнйн180ц','ъакнйн 180ц'),
				'юпр.40а7B1ц','юпр.40а7B1_ц'),'юпр.80387ц','юпр.80387_ц'),'бйсянб 4,2ц*5','бйсянб 5*4,2ц'),'хг═якхбш═38ц','хг═якхбш 38ц'),
				'хг═ъакнйю═38ц','хг═ъакнйю 38ц'),'юпр.40гй7б1ц','юпр.40гй7б1_ц'),'якнимши170ц','якнимши 170ц'),'бюп.яцсы.32ц','бюп.яцсы. 32ц'),
				'пнфнй75ц','пнфнй 75ц'),'ьнй.цкюг.116ц','ьнй.цкюг. 116ц'),'ъцндш80ц','ъцндш 80ц'),'всдн100ц','всдн 100ц'),'овек*0,5ц','овек *0,5ц'),
				'432-318 цк','432-318 _цк'),'48 цняр','48 _цняр'),'вепмнякхб-гкюйх370ц','вепмнякхб-гкюйх 370ц'),'4 ьр*65','4ьр*65')				
				,'оевемэе═пнцюкхй═270ц /','оевемэе═пнцюкхй 270ц /')

		UPDATE step0
		SET param6_tara = 'со',
			product_name = replace(replace(product_name,'ц б союйнбйе','ц'),'цп б союйнбйе','ц')
		where	product_name like '%[0-9]ц б союйнбйе%' 
			or product_name like '%[0-9] ц б союйнбйе%'  
			or product_name like '%[0-9]цп б союйнбйе%'  
			or product_name like '%[0-9] цп б союйнбйе%'  

		EXEC REPLACE_MEASURE_SIMBOLS  @measure_simbol= 'ц' , @variant1 = 'цп'
		EXEC EXTRACT_MEASURE @measure_simbol= 'ц' 
		UPDATE step0 SET param3_gramm=temp_param

		-- Б param3_special ГЮОХЬЕЛ НЯРЮРЙХ (НЯНАЕММШЕ ЯКСВЮХ, ЙНРНПШЕ МЮДН ОПНБЕПХРЭ Х ОН ПЕГСКЭРЮРЮЛ ОПНБЕПЙХ НАПЮАНРЮРЭ ЙЮЙ ХЯЙКЧВЕМХЪ ХКХ НЯРЮБХРЭ ЙЮЙ ЕЯРЭ Х МЕ НАПЮЫЮРЭ БМХЛЮМХЪ)
		EXEC EXTRACT_MEASURE @measure_simbol= 'ц' 
		UPDATE step0 SET param3_special=temp_param where temp_param!=''

		
		------------------------------param3_kg------------------------------------------------
		------------------------------param3_kg------------------------------------------------
		------------------------------param3_kg------------------------------------------------
		-- НАПЮАНРЙЮ ХЯЙКЧВЕМХИ ДКЪ йц
		UPDATE step0 
		set product_name=replace(replace(replace(replace(product_name,	'0,3,йц','0,3йц'),'дн 90йц','дн_90йц'),'1,2йц*4','4*1,2йц'),'1,1йц*4','4*1,1йц')

		EXEC EXTRACT_MEASURE @measure_simbol= 'йц' 
		UPDATE step0 SET param3_kg=temp_param
		where param3_kg not like '%[0-9]-[0-9][0-9]%'   and product_name not like '%дн_90йц%'

		EXEC EXTRACT_MEASURE @measure_simbol= 'йц' 
		UPDATE step0 SET param19_subcategory=temp_param
		where param3_kg  like '%[0-9]-[0-9][0-9]%'   and product_name like '%дн_90йц%'

		-- Б param3_special ГЮОХЬЕЛ НЯРЮРЙХ (НЯНАЕММШЕ ЯКСВЮХ, ЙНРНПШЕ МЮДН ОПНБЕПХРЭ Х ОН ПЕГСКЭРЮРЮЛ ОПНБЕПЙХ НАПЮАНРЮРЭ ЙЮЙ ХЯЙКЧВЕМХЪ ХКХ НЯРЮБХРЭ ЙЮЙ ЕЯРЭ Х МЕ НАПЮЫЮРЭ БМХЛЮМХЪ)
		EXEC EXTRACT_MEASURE @measure_simbol= 'йц' 
		UPDATE step0 SET param3_special=temp_param where temp_param!=''

	
		------------------------------param7_percent------------------------------------------------
		------------------------------param7_percent------------------------------------------------
		------------------------------param7_percent------------------------------------------------

		UPDATE step0 set product_name = replace(product_name, '%','percent')

		EXEC EXTRACT_MEASURE @measure_simbol= 'percent' 
		UPDATE step0 SET param7_percent=replace(temp_param,'percent','%')

		--select * from step0 where param7_percent !=''

		-- Б param5_special ГЮОХЬЕЛ НЯРЮРЙХ (НЯНАЕММШЕ ЯКСВЮХ, ЙНРНПШЕ МЮДН ОПНБЕПХРЭ Х ОН ПЕГСКЭРЮРЮЛ ОПНБЕПЙХ НАПЮАНРЮРЭ ЙЮЙ ХЯЙКЧВЕМХЪ ХКХ НЯРЮБХРЭ ЙЮЙ ЕЯРЭ Х МЕ НАПЮЫЮРЭ БМХЛЮМХЪ)
		--EXEC EXTRACT_MEASURE @measure_simbol= '%' 

		--select * from step0 where  param7_percent !=''
		------------------------------param8_sm------------------------------------------------
		------------------------------param8_sm------------------------------------------------
		------------------------------param8_sm------------------------------------------------

		UPDATE step0
		set product_name = replace(replace(product_name, ' X ', 'X'), ' у ', 'у')

		where product_name like '%[0-9] X [0-9]%'
		OR product_name like '%[0-9] у [0-9]%'

		EXEC EXTRACT_MEASURE @measure_simbol= 'ял' 
		UPDATE step0 SET param8_sm=temp_param

		--select * from step0 where param8_sm !=''

		-- Б param8_special ГЮОХЬЕЛ НЯРЮРЙХ (НЯНАЕММШЕ ЯКСВЮХ, ЙНРНПШЕ МЮДН ОПНБЕПХРЭ Х ОН ПЕГСКЭРЮРЮЛ ОПНБЕПЙХ НАПЮАНРЮРЭ ЙЮЙ ХЯЙКЧВЕМХЪ ХКХ НЯРЮБХРЭ ЙЮЙ ЕЯРЭ Х МЕ НАПЮЫЮРЭ БМХЛЮМХЪ)
		EXEC EXTRACT_MEASURE @measure_simbol= 'ял' 
		UPDATE step0 SET param8_special=temp_param where temp_param!=''

		EXEC EXTRACT_MEASURE @measure_simbol= 'л' 
		UPDATE step0 SET param8_m=temp_param

		-- Б param8_special ГЮОХЬЕЛ НЯРЮРЙХ (НЯНАЕММШЕ ЯКСВЮХ, ЙНРНПШЕ МЮДН ОПНБЕПХРЭ Х ОН ПЕГСКЭРЮРЮЛ ОПНБЕПЙХ НАПЮАНРЮРЭ ЙЮЙ ХЯЙКЧВЕМХЪ ХКХ НЯРЮБХРЭ ЙЮЙ ЕЯРЭ Х МЕ НАПЮЫЮРЭ БМХЛЮМХЪ)
		EXEC EXTRACT_MEASURE @measure_simbol= 'л' 
		UPDATE step0 SET param8_special=temp_param where temp_param!=''

		--select * from step0 where param8_special !=''
		
		------------------------------param5_items------------------------------------------------
		------------------------------param5_items------------------------------------------------
		------------------------------param5_items------------------------------------------------
		UPDATE step0
		SET product_name = replace(replace(replace(replace(replace(product_name,'ьр б союйнбйе','ьр'),'ьр.б со-йе','ьр'),'ьр. б со','ьр'),'ьр б со','ьр'),'б союй. 100 ьрсй','100ьр')
		, param6_tara = 'со'
		WHERE	product_name like '%ьр б союйнбйе%'
			or product_name  like '%ьр.б со-йе%'
			or product_name  like '%ьр. б со%'
			or product_name  like '%ьр б со%'
			or product_name  like '%б союй. 100 ьрсй%'

		EXEC EXTRACT_MEASURE @measure_simbol= 'ьр' 
		UPDATE step0 SET param5_items=temp_param

		EXEC EXTRACT_MEASURE @measure_simbol= 'оюпш' 
		UPDATE step0 SET param5_items=temp_param

		--!! ОПНБЕПХРЭ, ВРН Б ЮЯЯНПРХЛЕМР, ВРН Б КХРПШ Х ЯЛ. НЯНАЕММН ЛЕЬЙХ

		-- Б param5_special ГЮОХЬЕЛ НЯРЮРЙХ (НЯНАЕММШЕ ЯКСВЮХ, ЙНРНПШЕ МЮДН ОПНБЕПХРЭ Х ОН ПЕГСКЭРЮРЮЛ ОПНБЕПЙХ НАПЮАНРЮРЭ ЙЮЙ ХЯЙКЧВЕМХЪ ХКХ НЯРЮБХРЭ ЙЮЙ ЕЯРЭ Х МЕ НАПЮЫЮРЭ БМХЛЮМХЪ)
		EXEC EXTRACT_MEASURE @measure_simbol= 'ьр' 
		UPDATE step0 SET param5_special=temp_param where temp_param!=''
		
		------------------------------param6_tara------------------------------------------------
		------------------------------param6_tara------------------------------------------------
		------------------------------param6_tara------------------------------------------------
		update step0
		set product_name = replace(product_name,'оюйер д','оюйеер д')
		where product_name like '%оюйер д%'

		update step0
		set product_name = replace(product_name,' б / с',' б / со')
		where product_name like '% б / с %'

		update step0
		set product_name = replace(product_name,'йсбьхм','йсбьххм')
		where product_name like '%йсбьхм%' and product_name like '%йпшь%'

		update step0
		set product_name = replace(product_name,'асршкйю','асршшкйю')
		where product_name like '%асршкйю%' and product_name like '%опна%'

		UPDATE step0  SET product_name = replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(product_name
														,'б оюйере','оюйер')
														,'бюйссл со-йю','б / со')
														,'б онкхщрхк.союй','о / со'),'окюярхй со-йю','о / со'),'окюяр / со','о / со'),'ок / союй','о / со'),'ок / со','о / со')
														,'аслюф со-йю','а / со'),'асл / союй','а / со'),'асл / со','а / со')
														,'йюпрнм со-йю','й / со'),'йюпр / со','й / со'),'йюпр со','й / со')
														,'цюг / со','ц / со')												
														,'лъцйюъ со-йю','л / со')
														,'б ондюп / союйнбйе','онд / со'),'онд.со.','онд / со ')
														,'окюярхй аюмйю','ок / а')	,'о / а','ок / а')
														,'о / оюй','ок / оюй')

		EXEC EXTRACT_SUBSTRING  @SUBSTRING='ткнсоюй'		UPDATE step0 SET param6_tara=temp_param where temp_param!=''
		EXEC EXTRACT_SUBSTRING  @SUBSTRING='оюйер'			UPDATE step0 SET param6_tara=temp_param where temp_param!=''									
		EXEC EXTRACT_SUBSTRING  @SUBSTRING=' б / со'		UPDATE step0 SET param6_tara=temp_param where temp_param!=''
		EXEC EXTRACT_SUBSTRING  @SUBSTRING=' о / со'		UPDATE step0 SET param6_tara=temp_param where temp_param!=''
		EXEC EXTRACT_SUBSTRING  @SUBSTRING=' а / со'		UPDATE step0 SET param6_tara=temp_param where temp_param!=''
		EXEC EXTRACT_SUBSTRING  @SUBSTRING=' й / со'		UPDATE step0 SET param6_tara=temp_param where temp_param!=''
		EXEC EXTRACT_SUBSTRING  @SUBSTRING=' ц / со'		UPDATE step0 SET param6_tara=temp_param where temp_param!=''
		EXEC EXTRACT_SUBSTRING  @SUBSTRING=' л / со'		UPDATE step0 SET param6_tara=temp_param where temp_param!=''
		EXEC EXTRACT_SUBSTRING  @SUBSTRING=' реплн / со'	UPDATE step0 SET param6_tara=temp_param where temp_param!=''
		EXEC EXTRACT_SUBSTRING  @SUBSTRING=' онд / со'		UPDATE step0 SET param6_tara=temp_param where temp_param!=''
		EXEC EXTRACT_SUBSTRING  @SUBSTRING=' д / оюй'		UPDATE step0 SET param6_tara=temp_param where temp_param!=''
		EXEC EXTRACT_SUBSTRING  @SUBSTRING=' оп / о'		UPDATE step0 SET param6_tara=temp_param where temp_param!=''
		UPDATE step0  SET product_name = replace(replace(replace(replace(replace(product_name,'лхйпн союйнбйю',' со '),'союйнбйю',' со '),' союй',' со '),' со',' со '),'1со','жемю гю со')
		--EXEC EXTRACT_SUBSTRING  @SUBSTRING=' со'			UPDATE step0 SET param6_tara=temp_param where temp_param!=''
		-- !!лхйпн союйнбйс днаюбхрэ б юяянпрхлемр хкх рхо союйнбйх \ назел
		-- !!ондцсгмхйх оюлоепя юйрхб ащах дпюи JUNIOR лхйпн союйнбйю / 12
		EXEC EXTRACT_SUBSTRING  @SUBSTRING='онпж / со'		UPDATE step0 SET param6_tara=temp_param where temp_param!=''
		EXEC EXTRACT_SUBSTRING  @SUBSTRING='жемю гю со'		UPDATE step0 SET param1_pricefor=temp_param where temp_param!=''
		EXEC EXTRACT_SUBSTRING  @SUBSTRING=' со '			UPDATE step0 SET param6_tara=temp_param where temp_param!=''
		EXEC EXTRACT_SUBSTRING  @SUBSTRING='р / о'			UPDATE step0 SET param6_tara=temp_param where temp_param!=''
		EXEC EXTRACT_SUBSTRING  @SUBSTRING='ф / а'			UPDATE step0 SET param6_tara=temp_param where temp_param!=''
		EXEC EXTRACT_SUBSTRING  @SUBSTRING='ок / аср'		UPDATE step0 SET param6_tara=temp_param where temp_param!=''
		EXEC EXTRACT_SUBSTRING  @SUBSTRING='ок / яр'		UPDATE step0 SET param6_tara=temp_param where temp_param!=''
		EXEC EXTRACT_SUBSTRING  @SUBSTRING='ок / оюй'		UPDATE step0 SET param6_tara=temp_param where temp_param!=''
		EXEC EXTRACT_SUBSTRING  @SUBSTRING='ок / а'			UPDATE step0 SET param6_tara=temp_param where temp_param!=''
		-- явхрюч, ВРН ок / а ==окюярхй аюмйю
		EXEC EXTRACT_SUBSTRING  @SUBSTRING='йсбьхм'			UPDATE step0 SET param6_tara=temp_param where temp_param!=''
		EXEC EXTRACT_SUBSTRING  @SUBSTRING='йкхмнй'			UPDATE step0 SET param6_tara=temp_param where temp_param!=''
		EXEC EXTRACT_SUBSTRING  @SUBSTRING='й / йнп'		UPDATE step0 SET param6_tara=temp_param where temp_param!=''
		EXEC EXTRACT_SUBSTRING  @SUBSTRING='яр / аср'		UPDATE step0 SET param6_tara=temp_param where temp_param!=''
		EXEC EXTRACT_SUBSTRING  @SUBSTRING='яр / а '		UPDATE step0 SET param6_tara=temp_param where temp_param!=''

		--- напюанрюрэ\ пюгнапюрэяъ яр / а == яр / аср ?

		UPDATE step0
		SET param6_tara = 'ярюйюм',
			product_name=replace(product_name, 'ярюйюм' , '' )
		WHERE	product_name		like '%ярюйюм%'	-- ЙНКХВЕЯРБН БУНФДЕМХИ ЯХЛБНКНБ 'ярюйюм'
			and	product_name	not	like '%ярюйюмвхй%'
			and	product_name	not like '%ярюйюм д/йнйреикъ%'
			and param6_tara =	''

		EXEC EXTRACT_SUBSTRING  @SUBSTRING='ощр'			UPDATE step0 SET param6_tara=temp_param where temp_param!=''
		EXEC EXTRACT_SUBSTRING  @SUBSTRING='кнрнй'			UPDATE step0 SET param6_tara=temp_param where temp_param!=''
		EXEC EXTRACT_SUBSTRING  @SUBSTRING='ок / бедпн'		UPDATE step0 SET param6_tara=temp_param where temp_param!=''
		EXEC EXTRACT_SUBSTRING  @SUBSTRING='ок / йюмхярпю'  UPDATE step0 SET param6_tara=temp_param where temp_param!=''
		EXEC EXTRACT_SUBSTRING  @SUBSTRING='ок / йнп'		UPDATE step0 SET param6_tara=temp_param where temp_param!=''
		EXEC EXTRACT_SUBSTRING  @SUBSTRING='бедпн'			UPDATE step0 SET param6_tara=temp_param where temp_param!=''
		EXEC EXTRACT_SUBSTRING  @SUBSTRING='тнкэцю'			UPDATE step0 SET param6_tara=temp_param where temp_param!=''
		EXEC EXTRACT_SUBSTRING  @SUBSTRING='ондкнфйю'		UPDATE step0 SET param6_tara=temp_param where temp_param!=''

		UPDATE step0
		SET param6_tara =''
		where	param6_tara ='оюйер'  
		and (product_name_origin like  '%оюйер ондюп%' or product_name_origin like '%оюйер%люийю%' or product_name_origin like  '%оюйер%тюянбн%')

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
		where product_name like '%йц%'
		*/
		
		------param11_CCC=(яяя) ------------------------------------------------------------------------------------
		------param11_CCC=(яяя) ------------------------------------------------------------------------------------
		------param11_CCC=(яяя) ------------------------------------------------------------------------------------

		update step0 --53
		set product_name = rtrim(replace(replace(product_name,'п ( яяя )','( яяя )'),'п ( яяя )','( яяя )')),
		param12='п'
		where product_name like '%( яяя )%' 

		update step0 --53
		set product_name = rtrim(replace(product_name,'( яяя )','')),
		param11_CCC='(яяя)'
		where product_name like '%( яяя )%' 
		
		------param14_pch_part =ов / оюпр---------------------------------------------------------------------------------------
		------param14_pch_part =ов / оюпр---------------------------------------------------------------------------------------
		------param14_pch_part =ов / оюпр---------------------------------------------------------------------------------------

		update step0 --3
		set param14_pch_part='ов',
		product_name = rtrim(substring(product_name, 1, len(product_name)-2))
		where product_name like '% ов'

		update step0 --2
		set param14_pch_part='оюпр',
		product_name = rtrim(substring(product_name, 1, len(product_name)-4))
		where product_name like '% оюпр'

		update step0 --6
		set param14_pch_part='акнй',
		product_name = rtrim(replace(product_name, '( акнй',''))
		where product_name like '%( акнй%'

		
		------------------------ПЮГАНП бхмю (type,subtype, category,subcategory)---------------------------
		------------------------ПЮГАНП бхмю (type,subtype, category,subcategory)---------------------------
		------------------------ПЮГАНП бхмю (type,subtype, category,subcategory)---------------------------
		UPDATE step0
		SET 
			product_name =  replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace( replace(replace(replace(replace(replace(replace(replace(replace(replace(product_name, 
												'аек ясуне ( апчр )','аек апчр'),
												'аек / ясу','аек ясу') 
												,'тпсрйрнбне','тпсйрнбне'),'о / як тпсйрнбне ( окнднбне )','тпсйрнбне о / як')
												, 'аекне онксякюдйне','аек о / як'),'аек онксякюдйне','аек о / як'),'онксякюдйне аекне','аек о / як'),'аекши ярху онксякюдйхи','аек о / як')
												,'онксякюдйне йпюямне','йп о / як')
												, 'йп якюдйне','йп як')
												,'аек якюдйне','аек як'),'аек якюд','аек як'),'аекне якюдйне','аек як'),'аекши ярху якюдйхи','аек як')
												,'пнгнбне якюдйне','пнг як'),'пнг якюдйне','пнг як')
												,'аекне','аек')
												,'йпюямне','йп')
												,'онпрбхм','онпрбеим')
												,'SANGRIA','яюмцпхъ')

		UPDATE step0
		SET product_name =  replace(replace(replace(product_name, 'тпсйрнбне ( окнднбне )',''),'тпсйрнбне',''),'о / як','тпсйрнбне о / як')
		where product_name like '%тпсйрнбне%'
		and product_name like '%бхмн%'
		and product_name like '%о / як%'

		/* БНР ЕЫЕ АНКЭЬНИ ЙСЯНЙ, ЙНРНПШИ МЮДН АШ ОЕПЕМЕЯРХ Б РЮАКХЖС ОПЮБХК product_category_rules. Р.Е. ЩРНР БЮПХЮМР НОПЕДЕКЕМХЪ ОНКЕИ - РНФЕЮРЮБХГЛ */
	    /* МЮВЮКН ЮРЮБХЯРХВЕЯЙНЦН СВЮЯРЙЮ */		
		EXEC MULTIPLE_UPDATE_PRODSUBTYPE	@SUBSTRING = 'йп ясу'		,@PRODUCER	= ''	,@PROD_TYPE ='юкйнцнкэ'	,@PROD_SUBTYPE='бхмн '				,@PROD_CATEGORY=''	,@PROD_SUBCATEGORY='йп ясу'
		EXEC MULTIPLE_UPDATE_PRODSUBTYPE	@SUBSTRING = 'йп ясу'		,@PRODUCER	= ''	,@PROD_TYPE ='юкйнцнкэ'	,@PROD_SUBTYPE='бхммши мюо.'		,@PROD_CATEGORY=''	,@PROD_SUBCATEGORY='йп ясу'
		EXEC MULTIPLE_UPDATE_PRODSUBTYPE	@SUBSTRING = 'аек апчр'		,@PRODUCER	= ''	,@PROD_TYPE ='юкйнцнкэ'	,@PROD_SUBTYPE='ьюлоюмяйне'			,@PROD_CATEGORY=''	,@PROD_SUBCATEGORY='аек апчр'
		EXEC MULTIPLE_UPDATE				@SUBSTRING = 'аек апчр'		,@PRODUCER	= ''	,@PROD_TYPE ='юкйнцнкэ'	,@PROD_SUBTYPE='бхмн'				,@PROD_CATEGORY=''	,@PROD_SUBCATEGORY='аек апчр'
		EXEC MULTIPLE_UPDATE_PRODSUBTYPE	@SUBSTRING = 'аек ясу'		,@PRODUCER	= ''	,@PROD_TYPE ='юкйнцнкэ'	,@PROD_SUBTYPE='бхммши мюо.'		,@PROD_CATEGORY=''	,@PROD_SUBCATEGORY='аек ясу'
		EXEC MULTIPLE_UPDATE				@SUBSTRING = 'аек ясу'		,@PRODUCER	= ''	,@PROD_TYPE ='юкйнцнкэ'	,@PROD_SUBTYPE='бхмн'				,@PROD_CATEGORY=''	,@PROD_SUBCATEGORY='аек ясу'
		EXEC MULTIPLE_UPDATE_PRODSUBTYPE	@SUBSTRING = 'тпсйрнбне о / як'	,@PRODUCER	= '',@PROD_TYPE ='юкйнцнкэ'	,@PROD_SUBTYPE='бхмн'				,@PROD_CATEGORY=''	,@PROD_SUBCATEGORY='тпсйрнбне о / як'
		EXEC MULTIPLE_UPDATE_PRODSUBTYPE	@SUBSTRING='тпсйрнбне ( окнднбне )',@PRODUCER='',@PROD_TYPE ='юкйнцнкэ'	,@PROD_SUBTYPE='бхмн'				,@PROD_CATEGORY=''	,@PROD_SUBCATEGORY='тпсйрнбне'
		EXEC MULTIPLE_UPDATE_PRODSUBTYPE	@SUBSTRING = 'йп о / як'	,@PRODUCER	= ''	,@PROD_TYPE ='юкйнцнкэ'	,@PROD_SUBTYPE='бхммши мюо. цюг.'	,@PROD_CATEGORY=''	,@PROD_SUBCATEGORY='йп о / як'
		EXEC MULTIPLE_UPDATE_PRODSUBTYPE	@SUBSTRING = 'йп о / як'	,@PRODUCER	= ''	,@PROD_TYPE ='юкйнцнкэ'	,@PROD_SUBTYPE='бхммши мюо.'		,@PROD_CATEGORY=''	,@PROD_SUBCATEGORY='йп о / як'
		EXEC MULTIPLE_UPDATE_PRODSUBTYPE	@SUBSTRING = 'йп о / як'	,@PRODUCER	= ''	,@PROD_TYPE ='юкйнцнкэ'	,@PROD_SUBTYPE='бхмн'				,@PROD_CATEGORY=''	,@PROD_SUBCATEGORY='йп о / як'
		EXEC MULTIPLE_UPDATE_PRODSUBTYPE	@SUBSTRING = 'аек о / як'	,@PRODUCER	= ''	,@PROD_TYPE ='юкйнцнкэ'	,@PROD_SUBTYPE='бхммши мюо. цюг.'	,@PROD_CATEGORY=''	,@PROD_SUBCATEGORY='аек о / як'
		EXEC MULTIPLE_UPDATE_PRODSUBTYPE	@SUBSTRING = 'аек о / як'	,@PRODUCER	= ''	,@PROD_TYPE ='юкйнцнкэ'	,@PROD_SUBTYPE='бхммши мюо.'		,@PROD_CATEGORY=''	,@PROD_SUBCATEGORY='аек о / як'
		EXEC MULTIPLE_UPDATE_PRODSUBTYPE	@SUBSTRING = 'аек о / як'	,@PRODUCER	= ''	,@PROD_TYPE ='юкйнцнкэ'	,@PROD_SUBTYPE='бхмн'				,@PROD_CATEGORY=''	,@PROD_SUBCATEGORY='аек о / як'
		EXEC MULTIPLE_UPDATE_PRODSUBTYPE	@SUBSTRING = 'аек о / як'	,@PRODUCER	= ''	,@PROD_TYPE ='юкйнцнкэ'	,@PROD_SUBTYPE='ьюлоюмяйне'			,@PROD_CATEGORY=''	,@PROD_SUBCATEGORY='аек о / як'
		EXEC MULTIPLE_UPDATE_PRODSUBTYPE	@SUBSTRING = 'аек о / як'	,@PRODUCER	= ''	,@PROD_TYPE ='юкйнцнкэ'	,@PROD_SUBTYPE='бхммши мюо. цюг.'	,@PROD_CATEGORY=''	,@PROD_SUBCATEGORY='аек о / як'
		EXEC MULTIPLE_UPDATE_PRODSUBTYPE	@SUBSTRING = 'аек о / як'	,@PRODUCER	= ''	,@PROD_TYPE ='юкйнцнкэ'	,@PROD_SUBTYPE='бхммши мюо.'		,@PROD_CATEGORY=''	,@PROD_SUBCATEGORY='аек о / як'
		EXEC MULTIPLE_UPDATE_PRODSUBTYPE	@SUBSTRING = 'аек о / як'	,@PRODUCER	= ''	,@PROD_TYPE ='юкйнцнкэ'	,@PROD_SUBTYPE='бхмн'				,@PROD_CATEGORY=''	,@PROD_SUBCATEGORY='аек о / як'
		EXEC MULTIPLE_UPDATE_PRODSUBTYPE	@SUBSTRING = 'аек о / як'	,@PRODUCER	= ''	,@PROD_TYPE ='юкйнцнкэ'	,@PROD_SUBTYPE='ьюлоюмяйне'			,@PROD_CATEGORY=''	,@PROD_SUBCATEGORY='аек о / як'
		EXEC MULTIPLE_UPDATE_PRODSUBTYPE	@SUBSTRING = 'йп як'		,@PRODUCER	= ''	,@PROD_TYPE ='юкйнцнкэ'	,@PROD_SUBTYPE='бхмн'				,@PROD_CATEGORY=''	,@PROD_SUBCATEGORY='йп як'
		EXEC MULTIPLE_UPDATE_PRODSUBTYPE	@SUBSTRING = 'аек як'		,@PRODUCER	= ''	,@PROD_TYPE ='юкйнцнкэ'	,@PROD_SUBTYPE='бхмн'				,@PROD_CATEGORY=''	,@PROD_SUBCATEGORY='аек як'
		EXEC MULTIPLE_UPDATE_PRODSUBTYPE	@SUBSTRING = 'пнг о / як'	,@PRODUCER	= ''	,@PROD_TYPE ='юкйнцнкэ'	,@PROD_SUBTYPE='бхммши мюо. цюг.'	,@PROD_CATEGORY=''	,@PROD_SUBCATEGORY='пнг о / як'
		EXEC MULTIPLE_UPDATE_PRODSUBTYPE	@SUBSTRING = 'пнг о / як'	,@PRODUCER	= ''	,@PROD_TYPE ='юкйнцнкэ'	,@PROD_SUBTYPE='бхмн'				,@PROD_CATEGORY=''	,@PROD_SUBCATEGORY='пнг о / як'
		EXEC MULTIPLE_UPDATE_PRODSUBTYPE	@SUBSTRING = 'йп о / ясу'	,@PRODUCER	= ''	,@PROD_TYPE ='юкйнцнкэ'	,@PROD_SUBTYPE='бхмн'				,@PROD_CATEGORY=''	,@PROD_SUBCATEGORY='йп о / ясу'
		EXEC MULTIPLE_UPDATE_PRODSUBTYPE	@SUBSTRING = 'аек о / ясу'	,@PRODUCER	= ''	,@PROD_TYPE ='юкйнцнкэ'	,@PROD_SUBTYPE='ьюлоюмяйне'			,@PROD_CATEGORY=''	,@PROD_SUBCATEGORY='ьюлоюмяйне'
		EXEC MULTIPLE_UPDATE_PRODSUBTYPE	@SUBSTRING = 'аек о / ясу'	,@PRODUCER	= ''	,@PROD_TYPE ='юкйнцнкэ'	,@PROD_SUBTYPE='бхмн'				,@PROD_CATEGORY=''	,@PROD_SUBCATEGORY='аек о / ясу'
		EXEC MULTIPLE_UPDATE_PRODSUBTYPE	@SUBSTRING = 'пнг ясу'		,@PRODUCER	= ''	,@PROD_TYPE ='юкйнцнкэ'	,@PROD_SUBTYPE='бхмн'				,@PROD_CATEGORY=''	,@PROD_SUBCATEGORY='пнг ясу'
		EXEC MULTIPLE_UPDATE_PRODSUBTYPE	@SUBSTRING = 'пнг'			,@PRODUCER	= ''	,@PROD_TYPE ='юкйнцнкэ'	,@PROD_SUBTYPE='бхмн'				,@PROD_CATEGORY=''	,@PROD_SUBCATEGORY='пнг'
		EXEC MULTIPLE_UPDATE_PRODSUBTYPE	@SUBSTRING = 'йп деяепрмне'	,@PRODUCER	= ''	,@PROD_TYPE ='юкйнцнкэ'	,@PROD_SUBTYPE='бхмн'				,@PROD_CATEGORY=''	,@PROD_SUBCATEGORY='йп деяепрмне'
		EXEC MULTIPLE_UPDATE_PRODSUBTYPE	@SUBSTRING = 'пнг як'		,@PRODUCER	= ''	,@PROD_TYPE ='юкйнцнкэ'	,@PROD_SUBTYPE='бхммши мюо. цюг.'	,@PROD_CATEGORY=''	,@PROD_SUBCATEGORY='пнг як'
		EXEC MULTIPLE_UPDATE_PRODSUBTYPE	@SUBSTRING = 'аек як'		,@PRODUCER	= ''	,@PROD_TYPE ='юкйнцнкэ'	,@PROD_SUBTYPE='бхммши мюо. цюг.'	,@PROD_CATEGORY=''	,@PROD_SUBCATEGORY='аек як'
		EXEC MULTIPLE_UPDATE_PRODSUBTYPE	@SUBSTRING = 'аек'			,@PRODUCER	= ''	,@PROD_TYPE ='юкйнцнкэ'	,@PROD_SUBTYPE='бхммши мюо. цюг.'	,@PROD_CATEGORY=''	,@PROD_SUBCATEGORY='аек'
		EXEC MULTIPLE_UPDATE_PRODSUBTYPE	@SUBSTRING = 'о / як'		,@PRODUCER	= ''	,@PROD_TYPE ='юкйнцнкэ'	,@PROD_SUBTYPE='бхммши мюо. цюг.'	,@PROD_CATEGORY=''	,@PROD_SUBCATEGORY='о / як'
		EXEC MULTIPLE_UPDATE				@SUBSTRING='бхммши мюо. цюг.',@PRODUCER	= ''	,@PROD_TYPE ='юкйнцнкэ'	,@PROD_SUBTYPE='бхммши мюо. цюг.'	,@PROD_CATEGORY=''	,@PROD_SUBCATEGORY=''
		EXEC MULTIPLE_UPDATE_PRODCATEGORY	@SUBSTRING ='аегюкйнцнкэмши',@PRODUCER	= ''	,@PROD_TYPE ='опндсйрш'	,@PROD_SUBTYPE='мюохрйх'			,@PROD_CATEGORY='бхммши мюо.'	,@PROD_SUBCATEGORY='аегюкйнцнкэмши'
		EXEC MULTIPLE_UPDATE_PRODSUBTYPE	@SUBSTRING = 'беплср'		,@PRODUCER	= ''	,@PROD_TYPE ='юкйнцнкэ'	,@PROD_SUBTYPE='бхммши мюо.'		,@PROD_CATEGORY=''	,@PROD_SUBCATEGORY='беплср'
		EXEC MULTIPLE_UPDATE_PRODSUBTYPE	@SUBSTRING = 'яюмцпхъ'		,@PRODUCER	= ''	,@PROD_TYPE ='юкйнцнкэ'	,@PROD_SUBTYPE='бхммши мюо.'		,@PROD_CATEGORY=''	,@PROD_SUBCATEGORY='яюмцпхъ'
		EXEC MULTIPLE_UPDATE_PRODSUBTYPE	@SUBSTRING = 'лнухрн'		,@PRODUCER	= ''	,@PROD_TYPE ='юкйнцнкэ'	,@PROD_SUBTYPE='бхммши мюо.'		,@PROD_CATEGORY=''	,@PROD_SUBCATEGORY='лнухрн'
		EXEC MULTIPLE_UPDATE_PRODSUBTYPE	@SUBSTRING = 'цкхмрбеим'	,@PRODUCER	= ''	,@PROD_TYPE ='юкйнцнкэ'	,@PROD_SUBTYPE='бхммши мюо.'		,@PROD_CATEGORY=''	,@PROD_SUBCATEGORY='цкхмрбеим'
		EXEC MULTIPLE_UPDATE_PRODSUBTYPE	@SUBSTRING = 'онпрбеим'		,@PRODUCER	= ''	,@PROD_TYPE ='юкйнцнкэ'	,@PROD_SUBTYPE='бхммши мюо.'		,@PROD_CATEGORY=''	,@PROD_SUBCATEGORY='онпрбеим'
		EXEC MULTIPLE_UPDATE_PRODSUBTYPE	@SUBSTRING = 'йюцнп'		,@PRODUCER	= ''	,@PROD_TYPE ='юкйнцнкэ'	,@PROD_SUBTYPE='бхммши мюо.'		,@PROD_CATEGORY=''	,@PROD_SUBCATEGORY='йюцнп'
		--EXEC MULTIPLE_UPDATE_PRODSUBTYPE  @SUBSTRING = 'аек'			,@PRODUCER	= ''	,@PROD_TYPE ='юкйнцнкэ'	,@PROD_SUBTYPE='бхммши мюо.'		,@PROD_CATEGORY=''	,@PROD_SUBCATEGORY='аек'
		EXEC MULTIPLE_UPDATE_PRODSUBTYPE	@SUBSTRING = 'йп як'		,@PRODUCER	= ''	,@PROD_TYPE ='юкйнцнкэ'	,@PROD_SUBTYPE='бхммши мюо.'		,@PROD_CATEGORY=''	,@PROD_SUBCATEGORY='йп як'
		EXEC MULTIPLE_UPDATE_PRODSUBTYPE	@SUBSTRING = 'йп дея'		,@PRODUCER	= ''	,@PROD_TYPE ='юкйнцнкэ'	,@PROD_SUBTYPE='бхммши мюо.'		,@PROD_CATEGORY=''	,@PROD_SUBCATEGORY='йп дея'
		--EXEC MULTIPLE_UPDATE_PRODSUBTYPE  @SUBSTRING = 'йп'			,@PRODUCER	= ''	,@PROD_TYPE ='юкйнцнкэ'	,@PROD_SUBTYPE='бхммши мюо.'		,@PROD_CATEGORY=''	,@PROD_SUBCATEGORY='беплср'
		EXEC MULTIPLE_UPDATE				@SUBSTRING = 'бхммши мюо.'	,@PRODUCER	= ''	,@PROD_TYPE ='юкйнцнкэ'	,@PROD_SUBTYPE='бхммши мюо.'		,@PROD_CATEGORY=''	,@PROD_SUBCATEGORY=''
		---!!дндекюрэ он бхмс : опнхгбндхрекъ, ярпюмс
		--ДЮКЕЕ ОНЙЮ ОПНЯРН НОПЕДЕКЪЧ РХО Х ОНДРХО юкцнйнкъ
		EXEC MULTIPLE_UPDATE				@SUBSTRING = 'беплср'		,@PRODUCER	= ''	,@PROD_TYPE ='юкйнцнкэ'		,@PROD_SUBTYPE='беплср'			,@PROD_CATEGORY=''	,@PROD_SUBCATEGORY=''
		EXEC MULTIPLE_UPDATE				@SUBSTRING = 'йнмэъй'		,@PRODUCER	= ''	,@PROD_TYPE ='юкйнцнкэ'		,@PROD_SUBTYPE='йнмэъй'			,@PROD_CATEGORY=''	,@PROD_SUBCATEGORY=''
		EXEC MULTIPLE_UPDATE				@SUBSTRING = 'бндйю'		,@PRODUCER	= ''	,@PROD_TYPE ='юкйнцнкэ'		,@PROD_SUBTYPE='бндйю'			,@PROD_CATEGORY=''	,@PROD_SUBCATEGORY=''
		EXEC MULTIPLE_UPDATE				@SUBSTRING = 'мюярнийю'		,@PRODUCER	= ''	,@PROD_TYPE ='юкйнцнкэ'		,@PROD_SUBTYPE='мюярнийю'		,@PROD_CATEGORY=''	,@PROD_SUBCATEGORY=''
		EXEC MULTIPLE_UPDATE				@SUBSTRING = 'мюо.яохпр.'	,@PRODUCER	= ''	,@PROD_TYPE ='юкйнцнкэ'		,@PROD_SUBTYPE='мюо.яохпр.'		,@PROD_CATEGORY=''	,@PROD_SUBCATEGORY=''
		EXEC MULTIPLE_UPDATE				@SUBSTRING = 'бхяйх'		,@PRODUCER	= ''	,@PROD_TYPE ='юкйнцнкэ'		,@PROD_SUBTYPE='бхяйх'			,@PROD_CATEGORY=''	,@PROD_SUBCATEGORY=''
		EXEC MULTIPLE_UPDATE				@SUBSTRING = 'охбн'			,@PRODUCER	= ''	,@PROD_TYPE ='юкйнцнкэ'		,@PROD_SUBTYPE='охбн'			,@PROD_CATEGORY=''	,@PROD_SUBCATEGORY=''
		EXEC MULTIPLE_UPDATE				@SUBSTRING = 'охбмни мюо.'	,@PRODUCER	= ''	,@PROD_TYPE ='юкйнцнкэ'		,@PROD_SUBTYPE='охбн'			,@PROD_CATEGORY=''	,@PROD_SUBCATEGORY=''
		EXEC MULTIPLE_UPDATE				@SUBSTRING = 'сяяспхияйхи'	,@PRODUCER	= ''	,@PROD_TYPE ='юкйнцнкэ'		,@PROD_SUBTYPE='сяяспхияйхи аюкэгюл' ,@PROD_CATEGORY=''	,@PROD_SUBCATEGORY=''
		EXEC MULTIPLE_UPDATE				@SUBSTRING = 'асцскэлю'		,@PRODUCER	= ''	,@PROD_TYPE ='юкйнцнкэ'		,@PROD_SUBTYPE='асцскэлю'		,@PROD_CATEGORY=''	,@PROD_SUBCATEGORY=''
		EXEC MULTIPLE_UPDATE				@SUBSTRING = 'я / юкй.мюо.'	,@PRODUCER	= ''	,@PROD_TYPE ='юкйнцнкэ'		,@PROD_SUBTYPE='я / юкй.мюо.'	,@PROD_CATEGORY=''	,@PROD_SUBCATEGORY=''
		EXEC MULTIPLE_UPDATE				@SUBSTRING = 'яхдп'			,@PRODUCER	= ''	,@PROD_TYPE ='юкйнцнкэ'		,@PROD_SUBTYPE='яхдп'			,@PROD_CATEGORY=''	,@PROD_SUBCATEGORY=''
		/* ЙНМЕЖ ЮРЮБХЯРХВЕЯЙНЦН СВЮЯРЙЮ */
		
		--select * from step0 where product_name like '%охбн%' and param17_subtype ='' and product_name like '%йпсфйю%'
		-- ДКЪ 'йпсфйю дкъ охбю' ЯЛЕМХРЭ ЙЮРЕЦНПХЧ МЮ рнбюп онясдю ХКХ рнбюпш дкъ днлю

		------------------------------param4_litr------------------------------------------------
		------------------------------param4_litr------------------------------------------------
		------------------------------param4_litr------------------------------------------------

		update step0  set product_name = replace(replace(replace(replace(replace(product_name, 'к ', 'кхяр '), '18к,', '18кхярнб'), '18к', '18кхярнб'), 'к,', 'кхяр '), 'к.', 'кхяр ')
		where	( product_name like '%[0-9]к%' or product_name like '%[0-9] к%' )
				and (product_name like '%аслюц%' or product_name like '%рерпюдэ%' or product_name like '%йюпрнм%' or product_name like '%юкэанл%' or product_name like '%яюктерй%' or product_name like '%йюкемдюпэ%' 
					 or product_name like '%ефедмебмхй%' or product_name like '%акнймнр%' or product_name like '%акнй дкъ гюо%' or product_name like '%ймхфйю%' or product_name like '%ймхцю%')

		update step0  set product_name = replace(product_name, 'к ', 'кхрпнб ')
		where product_name like '%[0-9]к %' 	and ( product_name like '%йюмхярпю%'  
													or (product_name like '%йюярпчкъ%' and product_name not like '%оекэлемх%') 
													or product_name like '%лхяйю-яюкюрмхжю%'
													or product_name like '%йнмреимеп%'
													or product_name like '%цнпьнй дкъ пюяремхи%'
													or product_name like '%вюимхй%'
													or product_name like '%кеийю%')

		update step0  set product_name = replace(replace(product_name, 'к ', 'кхрпнб '), 'к+', 'кхрпнб ')
		where ( product_name like '%[0-9]к%' or product_name like '%[0-9] к%' ) 	and (product_name like '%аюяяеим%'  or product_name like '%йсбьхм-ондярюбйю%' or product_name like '%йсбьхм я йпшьйни%' or product_name like '%оюйеер д / гюлнпюфхбюмхъ%')

		update step0  set product_name = replace(product_name, 'к.', 'кхярнб  ')
		where product_name like '%[0-9] к %' 	and product_name like '%гюйкюдйх-ъпкшвйх%' 

		update step0  set product_name = replace(product_name, '100к', '100кюло')
		where  product_name like '%цхпкъмдю%'  and product_name like '%100к%' 

		update step0
		set product_name  =replace(replace(replace(replace(product_name,'3кер','3_кер'),'5кер','5_кер '),'3 кер','3_кер'),'5 кер','5_кер ')

		update step0
		set product_name =replace(replace(replace(replace(product_name,'0,75','0,75к'),'1,5','1,5к '),'0,25','0,25к'),'0,5','0,5к')
		where 
		(product_name like '%0,75%'  and product_name not like '%0,75к%' and param16_type = 'юкйнцнкэ')
		or 
		(product_name like '%1,5%'	 and product_name not like '%1,5к%'  and param16_type = 'юкйнцнкэ')
		or 
		(product_name like '%0,25%'  and product_name not like '%0,25к%' and param16_type = 'юкйнцнкэ')
		or 
		(product_name like '%0,5%'   and product_name not like '%0,5к%'  and param16_type = 'юкйнцнкэ')


		UPDATE step0 
		set product_name=replace(replace(product_name,	'р / о1к','р / о 1к'),'0,75 / 6','0,75к / 6')

		--опнбепхрэ, БЕГДЕ КХ ГЮОНКМЕМШ КХРПШ Б юкйнцнке

		EXEC EXTRACT_MEASURE @measure_simbol= 'к' 
		UPDATE step0 SET param4_litr=temp_param

		-- Б param5_special ГЮОХЬЕЛ НЯРЮРЙХ (НЯНАЕММШЕ ЯКСВЮХ, ЙНРНПШЕ МЮДН ОПНБЕПХРЭ Х ОН ПЕГСКЭРЮРЮЛ ОПНБЕПЙХ НАПЮАНРЮРЭ ЙЮЙ ХЯЙКЧВЕМХЪ ХКХ НЯРЮБХРЭ ЙЮЙ ЕЯРЭ Х МЕ НАПЮЫЮРЭ БМХЛЮМХЪ)
		EXEC EXTRACT_MEASURE @measure_simbol= 'к' 
		UPDATE step0 SET param4_special=temp_param where temp_param!=''
		
		------------------------------param4_ml------------------------------------------------
		------------------------------param4_ml------------------------------------------------
		------------------------------param4_ml------------------------------------------------

		EXEC EXTRACT_MEASURE @measure_simbol= 'лк' 
		UPDATE step0 SET param4_ml=temp_param

		-- Б param5_special ГЮОХЬЕЛ НЯРЮРЙХ (НЯНАЕММШЕ ЯКСВЮХ, ЙНРНПШЕ МЮДН ОПНБЕПХРЭ Х ОН ПЕГСКЭРЮРЮЛ ОПНБЕПЙХ НАПЮАНРЮРЭ ЙЮЙ ХЯЙКЧВЕМХЪ ХКХ НЯРЮБХРЭ ЙЮЙ ЕЯРЭ Х МЕ НАПЮЫЮРЭ БМХЛЮМХЪ)
		UPDATE STEP0 set product_name = replace(product_name,'+200лк+250лк','+450лк') where product_name like '%+200лк+250лк%'
		UPDATE STEP0 set product_name = replace(product_name,'+',' +') where param4_ml !=''

		EXEC EXTRACT_MEASURE @measure_simbol= 'лк' 
		UPDATE step0 SET param4_special=temp_param where temp_param!=''
		

		------------------------------param10------------------------------------------------
		------------------------------param10------------------------------------------------
		------------------------------param10------------------------------------------------
		---------------- БШВКЕМХЛ ВХЯКЮ ОНЯКЕ '/'  ------------------------------------------
		update step0  --1033
		set product_name = substring(product_name,1,len(product_name)-1)
		where product_name like '%[0-9]"' 

		UPDATE step0  --8973
		SET param10_num1 = substring(product_name,len(product_name)- CHARINDEX('/ ',reverse(product_name))+2 ,len(product_name)),
			product_name = substring(product_name, 1, len(product_name)- CHARINDEX('/ ',reverse(product_name)) )
		where	 product_name like '%/ '  -- 2 ЯРП select * from step0 where product_id in (3487,7031)
				OR  product_name like '%/'  -- 2 ЯРП select * from step0 where product_id in (3487,7031)
				OR product_name like '%/ [0-9]' 
				OR product_name like '%/ [0-9][0-9]'  
				OR product_name like '%/ [0-9][0-9][0-9]'  
				OR product_name like '%/ [0-9][0-9][0-9][0-9]'   

		UPDATE step0 SET product_name = rtrim(product_name)
		UPDATE step0   -- 380
		SET param10_num2 = substring(product_name,len(product_name)- CHARINDEX('/ ',reverse(product_name))+2 ,len(product_name)),
			product_name = substring(product_name, 1, len(product_name)- CHARINDEX('/ ',reverse(product_name)) )
		where	 ( product_name like '%/' and product_id not in (8754) )  -- 6 ЯРП      SELECT * FROM products WHERE product_id in (2039,3339,5040,8013, 8754, 9293, 9416)
				OR product_name like '%/ [0-9]' 
				OR product_name like '%/ [0-9][0-9]' 
				OR product_name like '%/ [0-9][0-9][0-9]'  
				OR product_name like '%/ [0-9][0-9][0-9][0-9]'   
				OR product_name like '%/ [0-9][0-9][0-9][0-9][0-9][0-9]'  

		UPDATE step0 SET product_name = rtrim(product_name)
		UPDATE step0   -- 6
		SET param10_num3 = substring(product_name,len(product_name)- CHARINDEX('/ ',reverse(product_name))+2 ,len(product_name)),
			product_name = substring(product_name, 1, len(product_name)- CHARINDEX('/ ',reverse(product_name)) )
		where	( product_name like '%/' and product_id not in (8754) )  -- 3 ЯРП      SELECT * FROM products WHERE product_id in (7865,8754,9150,10893)
				OR  product_name like '%/ [0-9][0-9][0-9]'  -- 3 ЯРПНЙ 
		
		UPDATE step0 SET product_name = rtrim(product_name)
		
		--select * from step0 where param4_litr = '' and product_name like '%[0-9]к%'
		
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
		update step0 set product_name = replace(product_name, '/ 36леф','/ 36 леф') where product_id in (12138,12140,12147,12151)
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

		---------------------------йкюяяхтхйюжхъ опндсйрш / рнбюпш -----------------------
		---------------------------йкюяяхтхйюжхъ опндсйрш / рнбюпш -----------------------
		---------------------------йкюяяхтхйюжхъ опндсйрш / рнбюпш -----------------------
		
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
		--select * from step0 where param17_subtype='' and product_name_origin like '%йбюя%'

	
		---------------------------оПЕНАПЮГНБЮМХЕ (ЦП Х ЙЦ) Х (лк Х к ) Й ЕДХМНЛС БХДС, НАПЮАНРЙЮ ДНОНКМХРЕКЭМШУ ХЯЙКЧВЕМХИ-----------------------
		---------------------------оПЕНАПЮГНБЮМХЕ (ЦП Х ЙЦ) Х (лк Х к ) Й ЕДХМНЛС БХДС, НАПЮАНРЙЮ ДНОНКМХРЕКЭМШУ ХЯЙКЧВЕМХИ-----------------------
		---------------------------оПЕНАПЮГНБЮМХЕ (ЦП Х ЙЦ) Х (лк Х к ) Й ЕДХМНЛС БХДС, НАПЮАНРЙЮ ДНОНКМХРЕКЭМШУ ХЯЙКЧВЕМХИ-----------------------

		UPDATE step0
		SET param3_gramm= '2*85ц',
			param4_ml = '250лк'
		WHERE param3_gramm = '250лк+2у85ц'

		DROP TABLE IF EXISTS step1
		SELECT 
			replace(replace(param3_kg,'йц',''),',','.') as param3_prepared,
			replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(param3_gramm
											,'ц',''),',','.'),'у','*'),'ьр','')
											,'230-250','250'),'240-250','250'),'200-250','250'),'200-210','210'),'230-240','240'),'140-185','185'),'150-160','160')
											,'160-250','250'),'220-240','240'),'350-400','400'),'15-21','21'),'90-100','100'),'200-220','220'),'260-330','330')
											,'6оюй*5','6*5')  as param3G_prepared,
			replace(replace(replace(replace(param4_litr	
											,'к',''),',','.'),'075','0.75')
											,'1,5-1,75','1,75к') as param4L_prepared,
			replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(param4_ml	
											,'лк',''),',','.'),'лърю500','500'),'2у2','4'),'бняярюмнб.250','250'),'45+72','117'),'бюя+75','75')
											,'100-120','120'),'370-400','400') ,'240-250','250'),'230-250','250')  as param4ML_prepared,
			replace(replace(param4_special ,'лк',''),'+','')as param4S_prepared,
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
				WHEN	param7_percent != ''		THEN trim(replace(replace(replace(replace(param7_percent,'%',''),'юкй.',''),'юкй',''),',','.'))
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
			TRY_CONVERT(float, coef) as try_convert_coef   --- ЯКЕДЮРЭ СЯКНБХЪ, ВРНАШ МЕ ОНКСВЮРЭ МСКХ РЮЛ, ЦДЕ МЕР ГМЮВЕМХИ
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
		--НАПЮАНРЙЮ ДНОНКМХРЕКЭМШУ ХЯЙКЧВЕМХИ
		UPDATE step4		SET gramms = gramms+75	WHERE param3_special like  '%+75%'    
		UPDATE step4		SET gramms = gramms+45	WHERE param3_special like  '%+45%'
		UPDATE step4		SET mls = mls+TRY_CONVERT(float, param4S_prepared)     WHERE product_name like  '%мюанп%'     and param4_special !='' 
		UPDATE step4		SET gramms = 13.6	    WHERE product_name_origin like '%феб%пегхмйю%нпахр%30%' and gramms is null
		UPDATE step4		SET gramms = 13			WHERE product_name_origin like '%феб%пегхмйю%пхцкх%20%' and gramms is null
		UPDATE step4		SET mls = 500			WHERE param17_subtype like 'охбн' and param6_tara like 'яр / аср'	and param10_num1 = '20' and mls is null
		UPDATE step4		SET mls = 500			WHERE param17_subtype like 'охбн' and param6_tara like 'ф / а'		and param10_num1 in ('24','6')	and mls is null
		UPDATE step4		SET mls = 1420			WHERE param17_subtype like 'охбн' and param6_tara like 'ощр'			and param10_num1 = '9'	and mls is null and product_name like '%1,42%'
		UPDATE step4		SET mls = 1000			WHERE param17_subtype like 'охбн' and param6_tara like 'ощр'		and param10_num1 = '9'	and mls is null and product_name like '% к%'
		UPDATE step4		SET param16_type = 'опндсйрш',
								param17_subtype = 'мюохрйх',
								param18_category = 'охбн',
								param19_subcategory = 'аегюкйнцнкэмши'
							WHERE param16_type like 'юкйнцнкэ'  and param17_subtype = 'охбн' and product_name like '%а / юкй%'

		UPDATE step4		SET gramms =20		WHERE product_name_origin like '%йхмдеп ячпопхг%' and  param10_num1='36' and gramms is null
		UPDATE step4		SET gramms =40		WHERE product_name_origin like '%яшпнй дюпопндсйр%' and  param10_num1='20'  and gramms is null
		UPDATE step4		SET gramms =26		WHERE product_name_origin like '%ялеяэ ймнпп%' and  param10_num1='24' and gramms is null
		UPDATE step4		SET gramms =16		WHERE product_id in ('4549','4568','4567')  -- %3б1 
		UPDATE step4		SET gramms =18		WHERE product_id ='4548' -- %3б1 
		UPDATE step4		SET gramms =20		WHERE product_id ='4550' -- %3б1 

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

---пЮГНАПЮРЭ:
---1." йпшл ""йбд йпшляйне аек"" юкй.9-11percentю, ярнкнбне
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
ОПНЖЕДСПЮ ДНАЮБКЪЕР ВЮЯРЭ ПЮЯОЮПЯЕММШУ ОЮПЮЛЕРПНБ Б РЮАКХЖС products
АСДЕР ДНОНКМЪРЭЯЪ ОН ЛЕПЕ ДНОНКМЕМХЪ ПЮГАНПЮ
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
										WHEN param16_type = 'юкйнцнкэ' THEN 'опндсйрш'
										ELSE param16_type 
									END as product_type 
							from step4 
							where step4.product_id=products.product_id),
			product_subtype = (select 	CASE 
											WHEN param16_type = 'юкйнцнкэ' THEN 'юкйнцнкэ'
											ELSE param17_subtype 
										END as product_subtype 
							from step4 
							where step4.product_id=products.product_id),
			product_category = (select 
									CASE 
										WHEN  param16_type = 'юкйнцнкэ' THEN param17_subtype
										ELSE  param18_category
									END as 	product_category 
								from step4 
								where step4.product_id=products.product_id),
			peacemeal = (select CASE 
									WHEN param1_pricefor	= 'жемю гю йц'	THEN 'пюгбея'
									WHEN param1_ves			like  '%бея%'	THEN 'пюгбея'
									WHEN param1_pricefor	= 'жемю гю к'	THEN 'пнгкхб'
								END as peacemeal 
							from step4 
							where step4.product_id=products.product_id)

	END
GO


/*
CALCULATE_STATISTICS  (product_id+store_id)
оПНЖЕДСПЮ ПЮЯЯВХРШБЮЕР ЯРЮРХЯРХЙХ (P)ОН НРДЕКЭМНЛС РНБЮПС (product_id+store_id) Х (S)ОН ЙЮРЕЦНПХХ РНБЮПНБ (product_subtype+store_id):
1. яРЮРХЯРХЙХ ХГЛЕМЕМХЪ ЖЕМШ: 
	1.1.ЯПЕДМЕЕ ЮАЯНКЧРМНЕ ХГЛЕМЕМХЕ ЖЕМШ Б ПСА НР ЯПЕДМЕЦН ГЮ БЯЧ ХЯРНПХЧ ОПНДЮФ (abs_dec) 
	1.2.ЯПЕДМЕЕ НРМНЯХРЕКЭМНЕ ХГЛЕМЕМХЕ ЖЕМШ Б % НР ЯПЕДМЕЦН ГЮ БЯЧ ХЯРНПХЧ ОПНДЮФ (rel_dev) 
	(НЦПЮМХВЕМХЪ: ЯРЮРХЯРХЙХ ЯВХРЮЧРЯЪ ОН price1)
2.цКСАХМЮ ХЯРНПХХ ОПНДЮФ  (history_depth)
3.оКНРМНЯРЭ ОПНДЮФ (ЯЙНКЭЙН ЕДХМХЖ РНБЮПЮ ОПНДЮЕРЯЪ Б ЙЮФДНЛ ЛЮЦЮГХМЕ Б ДЕМЭ) 
					(s_count. Б РЮАКХЖЕ sales_statistics - Б ПЮГПЕГЕ ОПНДСЙРНБ, 
					Б РЮАКХЖЕ subtype_statistics_bydate - Б ПЮГПЕГЕ subtype  ) 
	3.b - ЯПЕДМЕЕ ЙНКХВЕЯРБН РНБЮПЮ\subtype Б ЙЮФДНЛ ЛЮЦЮГХМЕ Б ДЕМЭ (avg_s_count)
4.бЮФМНЯРЭ РНБЮПЮ 
	4.1.ОПНЖЕМР НР БШПСВЙХ НАНПНРЮ ЯПЕДХ ОПНДЮФ БЯЕУ РНБЮПНБ (ЙЮФДНЛС ЛЮЦЮГХМЮ) (part_of_proceeds) PS
	4.2.ОПНЖЕМР ЕДХМХЖ НР НАЗЕЛЮ ОПНДЮФ БЯЕУ РНБЮПНБ (ОН ЙЮФДНЛС ЛЮЦЮГХМЮ)	(part_of_sales) PS

5. вЮЯРНРС ХГЛЕМЕМХЪ ЖЕМШ ДКЪ ЙЮФДНЦН РНБЮПЮ. (вХЯКН СМХЙЮКЭМШУ ЖЕМ)
	(count_unique_fact_price)	- Б ПЮЯВЕР БЙКЧВЕМШ РНКЭЙН ЖЕМШ ХГ ТЮЙРХВЕЯЙХ ГЮЦПСФЕММШУ ГЮОХЯЕИ 
										Я ОНКНФХРЕКЭМШЛ s_count

яНГДЮЕРЯЪ 4 РЮАКХЖШ:
sales_statistics  
product_id_statistics  
product_subtype_statistics
product_type_statistics
-- (СДЮКЕМН) subtype_statistics_bydate
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
оПНЖЕДСПЮ ЯНГДЮЕР РЮАКХЖС ОЮПЮЛЕРПНБ ЯЙПХМХМЦЮ dbo.ScreeningParameters, ЙСДЮ ГЮМНЯЪРЯЪ ОНПНЦХ ТХКЭРПЮЖХХ
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
		('avg_abs_dev',				@min_avg_abs_dev,				@max_avg_abs_dev ,				'CПЕДМЕЕ ЮАЯНКЧРМНЕ ХГЛЕМЕМХЕ ЖЕМШ Б ПСА НР ЯПЕДМЕЦН ГЮ БЯЧ ХЯРНПХЧ ОПНДЮФ'),
		('avg_rel_dev',				@min_avg_rel_dev,				@max_avg_rel_dev ,				'CПЕДМЕЕ НРМНЯХРЕКЭМНЕ ХГЛЕМЕМХЕ ЖЕМШ Б % НР ЯПЕДМЕЦН ГЮ БЯЧ ХЯРНПХЧ ОПНДЮФ'),
		('history_depth',			@min_history_depth,				@max_history_depth ,			'цКСАХМЮ ХЯРНПХХ ОПНДЮФ'),
		('avg_s_count',				@min_avg_s_count,				@max_avg_s_count ,				'оКНРМНЯРЭ ОПНДЮФ (ЯПЕДМЕЕ ЙНК-БН Б ДЕМЭ Б ЙЮФДНЛ ЛЮЦЮГХМЕ)'),
		('percent_of_sales',		@min_percent_of_sales,			@max_percent_of_sales ,			'бЮФМНЯРЭ, ОПНЖЕМР НР БШПСВЙХ НАНПНРЮ ОПНДЮФ ЛЮЦЮГХМЮ'),
		('percent_of_proceeds',		@min_percent_of_proceeds,		@max_percent_of_proceeds ,		'бЮФМНЯРЭ, ОПНЖЕМР НР НАЗЕЛЮ ОПНДЮФ ЛЮЦЮГХМЮ)')	,	
		('count_unique_fact_price', @min_count_unique_fact_price,	@max_count_unique_fact_price ,	'вЮЯРНРЮ ХГЛЕМЕМХЪ ЖЕМ')

	END
GO


/*
CREATE_ScreeningParameters
б ОПНЖЕДСПЕ ЦЕМЕПХРЯЪ ДХМЮЛХВЕЯЙХИ ГЮОПНЯ ДКЪ ОПНЯРЮБКЕМХЪ ОПХГМЮЙЮ screened_attr = 1, ДКЪ ГЮОХЯЕИ, 
СДНБКЕРБНПЪЧЫХУ СЯКНБХЪЛ ХГ РЮАКХЖШ ScreeningParameters Б РЮАКХЖЕ, СЙЮГЮММНИ Б ОЮПЮЛЕРПЕ @screened_table

рЮЙФЕ ЯНГДЮЕРЯЪ РЮАКХЖЮ 'screened_' + @screened_table  (-_statistics) РНКЭЙН Я ГЮОХЯЪЛХ, ДКЪ ЙНРНПШУ screened_attr = 1
	мЮОПХЛЕП, ЕЯКХ screened_table = 'product_id_statistics', РН
	ЯНГДЮЕРЯЪ РЮАКХЖЮ screened_product_id

рЮЙФЕ ЯНГДЮЕРЯЪ РЮАКХЖЮ 'screened_' + @screened_table  - '_statistics)' + '_agg ' 
c ЙНЛСКЪРХБМШЛХ ОНЙЮГЮРЕКЪЛХ ЯРЮРХЯРХЙЮЛХ ОКНРМНЯРХ Х БЮФМНЯРХ 
	мЮОПХЛЕП, ЕЯКХ screened_table = 'product_id_statistics', РН
	ЯНГДЮЕРЯЪ РЮАКХЖЮ screened_product_id_agg
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
дНАЮБКЪЕЛ ЖЕМС ОН s_date+store_id+product_id,  ОПХБЕДЕММСЧ Й НАЫЕЛС НАЗЕЛС/БЕЯС
яНГДЮЕРЯЪ РЮАКХЖa sales_prepared (МЮ АЮГЕ РЮАКХЖШ sales_statistics)
б sales_prepared ОНОЮДЮЧР ГЮОХЯХ РНКЭЙН ОН ОПНДСЙРЮЛ, ОПНЬЕДЬХЛ ЯЙПХМХЦ, 
РНКЭЙН ГЮОХЯХ БМСРПХ БПЕЛЕММНЦН ДХЮОЮГНМЮ ДКЪ опндсйр + люцюгхм 
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
					WHEN ss.product_type = 'рнбюпш'					THEN ss.price1
					WHEN p.gramms	is not null and p.gramms !=0	THEN (ss.price1/p.gramms)*@gramms
					WHEN p.mls		is not null and p.mls	 !=0	THEN (ss.price1/p.mls)*@mls
					WHEN p.peacemeal	is not null					THEN ss.price1
					WHEN ss.product_subtype in ('ъижю')				THEN ss.price1
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
		/* ЯНГДЮРЭ ДПСЦХЕ ХМДЕЙЯЪ, ЕЯКХ АСДЕР СБЕКХВХБЮРЭЯЪ ЙНКХВЕЯРБН ЦПСОО ЖЕМНБНЦН ЙНМРЕЙЯРЮ */

	END
GO

/* ЕЯКХ МСФМН ОНКСВХРЭ ТХМЮКЭМШЕ РЮАКХЖШ МЕ ОН БЯЕЛ ДЮММШЛ, Ю РНКЭЙН ОН ВЮЯРХ, РН МСФМН ГДЕЯЭ ОНЯРЮБХРЭ СЯКНБХЕ
оПНЖЕДСПЮ SALES_PREPARED_RESTRICT - НЦПЮМХВХБЮЕР ЯНЯРЮБ РЮАКХЖШ sales_prepared, ОН СЯКНБХЧ product_subtype=@product_subtype
оЮПЮЛЕРП @product_subtype
ЕЯКХ @product_subtype = None, РН РЮАКХЖШ МЕ ХГЛЕМЪЕРЯЪ,
ХМЮВЕ Б РЮАКХЖЕ НЯРЮЧРЯЪ РНКЭЙН ГЮОХЯХ Я СЯКНБХЕЛ WHERE product_subtype='лнкнвмше опндсйрш'

ОНГБНКЪЕР НЯРЮБХРЭ Б РЮАКХЖС РНКЭЙН НДМН ГМЮВЕМХЕ  product_subtype.
яДЕКЮМЮ, ВРНАШ РЕЯРХПНБЮРЭ МЮ ЛЕМЭЬЕЛ НАЗЕЛЕ ХКХ ОН ЦПСООЕ.
бОНЯКЕДЯРБХХ, ЕЯКХ Б ОПХМЖХОЕ МСФМН АСДЕР ЯВХРЮРЭ ОН ЦПСООЮЛ, РН ЯДЕКЮЧ ЖХЙК, Б ЙНРНПНЛ АСДС ГЮОСЯЙЮРЭ ОНЯКЕДМХЕ РПХ ОПНЖЕДСПШ ДКЪ ЙЮФДНИ ЦПСООШ

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
яНГДЮЕЛ Х ГЮОНКМЪЕЛ РЮАКХЖС GroupParameters
*/

/* CREATE_GroupParameters
1. оПНЖЕДСПЮ ЦЕМЕПХПСЕР РЮАКХЖС AttributeToContect, Б ЙНРНПСЧ ГЮОХЯШБЮЕР ОНКЪ ХГ products

рЮАКХЖЮ AttributeToContect РЮЙФЕ ЯНДЕПФХР РЮЙФЕ
-ОПХГМЮЙХ,ВРН ЮРПХАСР СВЮЯРБСЕР Б СЯКНБХЪУ НОПЕДЕКЕМХЪ ЦПСОО, 
	ОПХГМЮЙ СВЮЯРХЪ ЮРПХАСРЮ Б СЯКНБХЪУ ЦПСООХПНБЙХ (=1, ЕЯКХ ЮРРПХАСР СВЮЯРБСЕР Б ЦПСООХПНБЙЕ)
	ОПХГМЮЙ ЙЮРЕЦНПХЮКЭМНЯРХ\БЕЫЕЯРБЕММНЯРХ ЮРПХАСРЮ
	ОПХГМЮЙ , Б ЙНРНПНЛ СЙЮГЮМ НОЕПЮРНП ДКЪ ЯПЮБМЕМХЪ
дЮКЕЕ ЩРХ РЮАКХЖШ join'ЪРЯЪ ОН product_id

рЮЙФЕ Б AttributeToContect ДНАЮБКЪЧРЯЪ ОНКЪ ЯН ЯРЮРХЯРХЙЮЛХ

2. оПНЖЕДСПЮ ЯНГДЮЕР Х ГЮОНКМЪЕР РЮАКХЖС GroupParameters Я ОПЮБХКЮЛХ НРМЕЯЕМХЪ ОПНДСЙРЮ Б ЦПСООС (МЮ АЮГЕ sales_prepared)
Х GroupParameters_unique (ХДЕР Б ЯКЕДСЧЫСЧ ОПНЖЕДСПС)

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



		--МЮВМЕЛ ГЮОНКМЪРЭ РЮАКХЖС ОПЮБХКЮЛХ НРМЕЯЕМХЪ Б ЦПСООШ
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
яНГДЮЕЛ Х ГЮОНКМЪЕЛ РЮАКХЖС sales_price_context Я ЖЕМНБШЛ ЙНМРЕЙЯРНЛ
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
			BEGIN /* РСР ГЮЦНРНБЙЮ МЮ ПЮЯЬХПЕМХЕ ЙНКХВЕЯРБЮ ОЮПЮЛЕРПНБ */ /*

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


/* оПНЖЕДСПШ  CREATE_PLAN_FOR_PRODUCT_SUBTYPE Х CREATE_PLAN_FOR_PRODUCT ЯНГДЮЧР 
 РЮАКХЖШ НЦПЮМХВЕМХИ, ЙНРНПШЕ ЛНФМН ХЯОНКЭГНБЮРЭ ДКЪ ОНЯКЕДСЧЫЕИ НОРХЛХГЮЖХХ nlopt */
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