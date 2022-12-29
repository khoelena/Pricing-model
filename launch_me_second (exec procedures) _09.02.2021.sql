/* ��������� �����. ��������, 2020 */

/* 
��� ����, � ������� ����������� ���������. 
��� ��� ����� ������� ����� �����������, ��� ������� � ������ 18 ����������, � ������� ����� �����. 
����� ����� ���������� ~ 50 ��� (������� �� �����)
*/


/*
step0. C����� ���� ������ PRICE_FORMATION  � ��������� � ���
	��� ����� �������� ��������� ���� launch_me_first_and_once (create db and procedures).sql, ����� ����������  ~ 2 ���	
	���� ���������� ��������� launch_me_first_and_once ��������, 
						�� ���������� �������������� ������� ���� PRICE_FORMATION ����� �������������� ��������,
						�������� ������� � check-box ����� ���� "������� ������������ ����������"
*/


/* step1. ��������� ���� ������ */
USE PRICE_FORMATION 
GO


-- step2. ������ ������� � �������� ������
--����� ����������  ~ 2 ���
--!!!!������� ���� � ������ � \
EXEC LOAD_FROM_FILES	@DIRECTORY		= 'C:\Users\elena.khotlyannik\Documents\AA\�������� 01.04.2016-01.04.2018_\',
						@SALES_FILE1	= '�������� ������ � 01.04.2016 - 31.12.2016.csv',								  
						@SALES_FILE2	= '�������� ������ � 01.01.2017 - 01.10.2017.csv',
						@SALES_FILE3	= '�������� ������ � 02.10.2017 - 01.04.2018.csv',
						@PRODUCTS_FILE	= '���������� �������(�� ������� ���� ������� � 01.04.2016-01.04.2018).csv',
						@STORES_FILE	= '��������.csv'
GO


-- step3. ������������ ������
--����� ����������  ~ 1 ��� 
EXEC ANONIMISING
GO


-- step4. �������� ��������� ��� � ���������� ���� ��� ������� sales
--����� ����������  ~  20 ���!!
EXEC ALTER_SALES --��������� ���� � �������
GO
EXEC TIME_SERIES_AND_PRICES
GO


-- step5. �������� ������������ ���������, ������� �� ���� ��������� ��������
--����� ����������  ~ 2���
EXEC CREATE_product_category_rules
GO
EXEC FILL_product_category_rules
GO

EXEC PRODUCT_NAME_PARCING
GO


-- step6. �������� ��������� ��������� � ������� products
--����� ����������  ~ ���� ���
EXEC ALTER_PRODUCTS  -- ��������� ���� � �������
GO
EXEC ADD_PARAMETERS_TO_PRODUCTS
GO


/*
step7. ����������� ����������. ��������� ������� 
	sales_statistics
	product_statistics
	subtype_statistics
*/
--����� ����������  ~ 2���
EXEC CALCULATE_STATISTICS
GO


/*
step8. ����� ��������� ��� ��������� � ����� ��������� ���������
*/
--����� ����������  ~ ���� ������
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
�������� ���� �� s_date+store_id+product_id,  ����������� � ������ ������/����
��������� ������a sales_prepared (�� ���� ������� sales_statistics)
� sales_prepared �������� ������ ������ �� ���������, ��������� �������, 
������ ������ ������ ���������� ��������� ��� ������� + ������� 
*/
--����� ����������  ~ 1���
 EXEC PRICE_CONVERTING @gramms = 1000, @mls = 1000
 GO


 /* step10. optional
 ���� ����� �������� ��������� ������� �� �� ���� ������, � ������ �� �����, �� ����� ����� ��������� �������� ���  @product_subtype, 
 �������� @product_subtype = '�������� ��������'. 
 ���� ��������� ��� �������� ��������, (�� ��������� @product_subtype = Null), �� ������� �� ��������� � ����� ��������� ���� ����� ������
 */
 --����� ����������  ~ ���� ������
 EXEC SALES_PREPARED_RESTRICT -- @product_subtype ='�������'--  @product_subtype = '�������� ��������'
 GO


 /* step11.
������ ������� GroupParameters � ��������� ����������� �� ������� ���������, ��� ��������� �������� ���������
*/
--����� ����������  ~ 15 ���
 EXEC CREATE_GroupParameters
 GO


/* 
PRICE_CONTEXT
������ � �������� ������� sales_price_context � ������� ����������
*/
--����� ����������  ~ 20���
EXEC PRICE_CONTEXT
GO
 

 /* stepN-1. optional. ������ ������� � ������������� ��� ����������� ����������� nlopt */
--����� ����������  ~ ���� ������
EXEC CREATE_PLAN_FOR_PRODUCT_SUBTYPE
GO

EXEC CREATE_PLAN_FOR_PRODUCT
GO

/*
stepN. ������ ��������� ������� � �������
-- ����� ����������  ~  ����� ������� ���������, ������� ������� ��������� ������� � �������
-- EXEC  ������� ���������, ������� ������� ��������� ������� � �������
*/
DROP TABLE IF EXISTS step01, step02, step03, step04, step1, step2, step3, prep1, time_product_series, temp1
DROP TABLE IF EXISTS step0
DROP TABLE IF EXISTS step4

GO