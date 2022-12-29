/* Хотлянник Елена. Глоубайт, 2020 */

/* 
Это файл, в котором выполняются процедуры. 
Все что нужно сделать перед выполнением, это указать в строке 18 директорию, в которой лежат файлы. 
ОБЩЕЕ ВРЕМЯ ВЫПОЛНЕНИЯ ~ 50 мин (зависит от компа)
*/


/*
step0. Cоздаю базу данных PRICE_FORMATION  и процедуры в ней
	Для этого единожды выполняем файл launch_me_first_and_once (create db and procedures).sql, время выполнения  ~ 2 сек	
	Если необходимо запустить launch_me_first_and_once повторно, 
						то рекомендую предварительно удалить базу PRICE_FORMATION через обозревательно объектов,
						поставив галочку в check-box внизу окна "Закрыть существующие соединения"
*/


/* step1. Определяю базу данных */
USE PRICE_FORMATION 
GO


-- step2. Создаю таблицы и загружаю данные
--время выполнения  ~ 2 мин
--!!!!УКАЖИТЕ ПУТЬ К ФАЙЛАМ с \
EXEC LOAD_FROM_FILES	@DIRECTORY		= 'C:\Users\elena.khotlyannik\Documents\AA\Выгрузка 01.04.2016-01.04.2018_\',
						@SALES_FILE1	= 'Выгрузка продаж с 01.04.2016 - 31.12.2016.csv',								  
						@SALES_FILE2	= 'Выгрузка продаж с 01.01.2017 - 01.10.2017.csv',
						@SALES_FILE3	= 'Выгрузка продаж с 02.10.2017 - 01.04.2018.csv',
						@PRODUCTS_FILE	= 'Справочник товаров(по которым были продажи с 01.04.2016-01.04.2018).csv',
						@STORES_FILE	= 'Магазины.csv'
GO


-- step3. Анонимизация данных
--время выполнения  ~ 1 мин 
EXEC ANONIMISING
GO


-- step4. Дополняю временной ряд и расчитываю цену для таблицы sales
--время выполнения  ~  20 мин!!
EXEC ALTER_SALES --добавляем поля в таблицу
GO
EXEC TIME_SERIES_AND_PRICES
GO


-- step5. Разбираю наименование продуктов, выделяя из него параметры продукта
--время выполнения  ~ 2мин
EXEC CREATE_product_category_rules
GO
EXEC FILL_product_category_rules
GO

EXEC PRODUCT_NAME_PARCING
GO


-- step6. Добавляю параметры продуктов к таблице products
--время выполнения  ~ неск сек
EXEC ALTER_PRODUCTS  -- добавляем поля в таблицу
GO
EXEC ADD_PARAMETERS_TO_PRODUCTS
GO


/*
step7. Рассчитываю статистики. Создаются таблицы 
	sales_statistics
	product_statistics
	subtype_statistics
*/
--время выполнения  ~ 2мин
EXEC CALCULATE_STATISTICS
GO


/*
step8. Задаю параметры для скриннига и делаю скриннинг продуктов
*/
--время выполнения  ~ неск секунд
EXEC CREATE_ScreeningParameters
	@min_avg_abs_dev				= NULL,		@max_avg_abs_dev				= NULL, 
	@min_avg_rel_dev				= NULL,		@max_avg_rel_dev				= 200, 
	@min_history_depth				= 30,		@max_history_depth				= NULL, 
	@min_avg_s_count				= NULL,		@max_avg_s_count				= NULL, 
	@min_percent_of_sales			= 0.01,		@max_percent_of_sales			= NULL, 
	@min_percent_of_proceeds		= 0.01,		@max_percent_of_proceeds		= NULL, 
	@min_count_unique_fact_price	= 5,		@max_count_unique_fact_price	= NULL
GO

EXEC ScreenProducts @screened_table = 'product_id_statistics'
GO
--EXEC ScreenProducts @screened_table = 'product_type_statistics'
--GO
--EXEC ScreenProducts @screened_table = 'product_subtype_statistics'
--GO


/* step9. 
Добавляю цену по s_date+store_id+product_id,  приведенную к общему объему/весу
Создается таблицa sales_prepared (на базе таблицы sales_statistics)
В sales_prepared попадают записи только по продуктам, прошедшим скриниг, 
только записи внутри временного диапазона для ПРОДУКТ + МАГАЗИН 
*/
--время выполнения  ~ 1мин
 EXEC PRICE_CONVERTING @gramms = 1000, @mls = 1000
 GO


 /* step10. optional
 Если нужно получить финальные таблицы не по всем данным, а только по части, то нужно здесь поставить значение для  @product_subtype, 
 например @product_subtype = 'МОЛОЧНЫЕ ПРОДУКТЫ'. 
 Если запустить без указания значения, (по умолчанию @product_subtype = Null), то таблица не изменится и будет содержать весь объем данных
 */
 --время выполнения  ~ неск секунд
 EXEC SALES_PREPARED_RESTRICT -- @product_subtype ='БАКАЛЕЯ'--  @product_subtype = 'МОЛОЧНЫЕ ПРОДУКТЫ'
 GO


 /* step11.
Создаю таблицу GroupParameters с правилами группировок по каждому параметру, для генерации ценового контекста
*/
--время выполнения  ~ 15 сек
 EXEC CREATE_GroupParameters
 GO


/* 
PRICE_CONTEXT
Создаю и заполняю таблицу sales_price_context с ценовым контекстом
*/
--время выполнения  ~ 20мин
EXEC PRICE_CONTEXT
GO
 

 /* stepN-1. optional. Создаю таблицы с ограничениями для последующей оптимизации nlopt */
--время выполнения  ~ неск секунд
EXEC CREATE_PLAN_FOR_PRODUCT_SUBTYPE
GO

EXEC CREATE_PLAN_FOR_PRODUCT
GO

/*
stepN. Удаляю временные таблицы и столбцы
-- время выполнения  ~  можно сделать процедуру, которая удаляет временные таблицы и столбцы
-- EXEC  сделать процедуру, которая удаляет временные таблицы и столбцы
*/
DROP TABLE IF EXISTS step01, step02, step03, step04, step1, step2, step3, prep1, time_product_series, temp1
DROP TABLE IF EXISTS step0
DROP TABLE IF EXISTS step4

GO