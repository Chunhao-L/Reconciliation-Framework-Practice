IF OBJECT_ID('STG_LAYER.CUSTOMERS', 'U') IS NOT NULL
    DROP TABLE STG_LAYER.CUSTOMERS;
GO

CREATE TABLE STG_LAYER.CUSTOMERS (
    batch_id INT,
    entity_event_id INT,
    customer_id INT,
    fname NVARCHAR(50),
    lname NVARCHAR(50)
);


IF OBJECT_ID('STG_LAYER.PRODUCTS', 'U') IS NOT NULL
    DROP TABLE STG_LAYER.PRODUCTS;
GO

CREATE TABLE STG_LAYER.PRODUCTS (
    batch_id INT,
    entity_event_id INT,
    product_id INT,
    product_name NVARCHAR(50),
    unit_price INT
);

INSERT INTO STG_LAYER.CUSTOMERS (
    batch_id, entity_event_id, customer_id, fname, lname
)
VALUES
(1, 1, 1001, N'Alice',  N'Brown'),
(1, 2, 1005, N'Ethan',  N'Wong'),
(2, 5, 1002, N'Bob',    N'Smith'),
(3, 9, 1003, N'Cathy',  N'Lee'),
(3,10, 1006, N'Nathan', N'Chan');

INSERT INTO STG_LAYER.PRODUCTS (
    batch_id, entity_event_id, product_id, product_name, unit_price
)
VALUES
(1, 3, 2001, N'apple',  2),
(1, 4, 2002, N'banana', 1),
(2, 6, 2003, N'orange', 3),
(2, 7, 2001, N'apple',  2),
(3,11, 2002, N'banana', 1);

select * from STG_LAYER.CUSTOMERS;
select * from STG_LAYER.PRODUCTS;