IF OBJECT_ID('RAW_LAYER.BATCH_EXTRACT', 'U') IS NOT NULL
    DROP TABLE RAW_LAYER.BATCH_EXTRACT;
GO

CREATE TABLE RAW_LAYER.BATCH_EXTRACT (
    batch_id INT,
    extraction_date DATE,
    json_data NVARCHAR(MAX)
);

IF OBJECT_ID('RAW_LAYER.ENTITY_EXTRACT', 'U') IS NOT NULL
    DROP TABLE RAW_LAYER.ENTITY_EXTRACT;
GO

CREATE TABLE RAW_LAYER.ENTITY_EXTRACT (
    batch_id INT,
    entity_event_id INT,
    entity_id INT,
    extraction_date DATE,
    entity_type NVARCHAR(50),
    entity_json_data NVARCHAR(MAX)
);

INSERT INTO RAW_LAYER.BATCH_EXTRACT (batch_id, extraction_date, json_data)
VALUES 
-- Batch 1
(1, '2025-07-07', 
N'{
  "batch_id": 1,
  "batch_data": [
    { "entity_id": 1001, "entity_type": "customer", "entity": { "first_name": "Alice", "last_name": "Brown", "gender": "F" } },
    { "entity_id": 1005, "entity_type": "customer", "entity": { "first_name": "Ethan", "last_name": "Wong", "gender": "M" } },
    { "entity_id": 2001, "entity_type": "product",  "entity": { "product_name": "apple",  "unit_price": 2 } },
    { "entity_id": 2002, "entity_type": "product",  "entity": { "product_name": "banana", "unit_price": 1 } }
  ]
}'),

-- Batch 2
(2, '2025-07-07', 
N'{
  "batch_id": 2,
  "batch_data": [
    { "entity_id": 1002, "entity_type": "customer", "entity": { "first_name": "Bob", "last_name": "Smith", "gender": "M" } },
    { "entity_id": 2003, "entity_type": "product",  "entity": { "product_name": "orange", "unit_price": 3 } },
    { "entity_id": 2001, "entity_type": "product",  "entity": { "product_name": "apple",  "unit_price": 2 } },
    { "entity_id": 2004, "entity_type": "product",  "entity": { "product_name": "peach",  "unit_price": 4 } }
  ]
}'),

-- Batch 3
(3, '2025-07-07', 
N'{
  "batch_id": 3,
  "batch_data": [
    { "entity_id": 1003, "entity_type": "customer", "entity": { "first_name": "Cathy", "last_name": "Lee", "gender": "F" } },
    { "entity_id": 1006, "entity_type": "customer", "entity": { "first_name": "Nathan", "last_name": "Chan", "gender": "M" } },
    { "entity_id": 2002, "entity_type": "product",  "entity": { "product_name": "banana", "unit_price": 1 } }
  ]
}'),

-- Batch 4
(4, '2025-07-07', 
N'{
  "batch_id": 4,
  "batch_data": [
    { "entity_id": 1004, "entity_type": "customer", "entity": { "first_name": "David", "last_name": "Chen", "gender": "M" } },
    { "entity_id": 2001, "entity_type": "product",  "entity": { "product_name": "apple",  "unit_price": 2 } },
    { "entity_id": 2002, "entity_type": "product",  "entity": { "product_name": "banana", "unit_price": 1 } },
    { "entity_id": 2005, "entity_type": "product",  "entity": { "product_name": "grape",  "unit_price": 5 } },
    { "entity_id": 2003, "entity_type": "product",  "entity": { "product_name": "orange", "unit_price": 3 } },
    { "entity_id": 2004, "entity_type": "product",  "entity": { "product_name": "peach",  "unit_price": 4 } }
  ]
}');


INSERT INTO RAW_LAYER.ENTITY_EXTRACT (
    batch_id, entity_event_id, entity_id, extraction_date, entity_type, entity_json_data
)
VALUES
-- Batch 1
(1, 1, 1001, '2025-07-07', 'customer', N'{"first_name":"Alice","last_name":"Brown","gender":"F"}'),
(1, 2, 1005, '2025-07-07', 'customer', N'{"first_name":"Ethan","last_name":"Wong","gender":"M"}'),
(1, 3, 2001, '2025-07-07', 'product',  N'{"product_name":"apple","unit_price":2}'),
(1, 4, 2002, '2025-07-07', 'product',  N'{"product_name":"banana","unit_price":1}'),

-- Batch 2
(2, 5, 1002, '2025-07-07', 'customer', N'{"first_name":"Bob","last_name":"Smith","gender":"M"}'),
(2, 6, 2003, '2025-07-07', 'product',  N'{"product_name":"orange","unit_price":3}'),
(2, 7, 2001, '2025-07-07', 'product',  N'{"product_name":"apple","unit_price":2}'),
(2, 8, 2004, '2025-07-07', 'product',  N'{"product_name":"peach","unit_price":4}'),

-- Batch 3
(3, 9, 1003, '2025-07-07', 'customer', N'{"first_name":"Cathy","last_name":"Lee","gender":"F"}'),
(3,10, 1006, '2025-07-07', 'customer', N'{"first_name":"Nathan","last_name":"Chan","gender":"M"}'),
(3,11, 2002, '2025-07-07', 'product',  N'{"product_name":"banana","unit_price":1}'),

-- Batch 4
(4,12, 1004, '2025-07-07', 'customer', N'{"first_name":"David","last_name":"Chen","gender":"M"}'),
(4,13, 2001, '2025-07-07', 'product',  N'{"product_name":"apple","unit_price":2}'),
(4,14, 2002, '2025-07-07', 'product',  N'{"product_name":"banana","unit_price":1}'),
(4,15, 2005, '2025-07-07', 'product',  N'{"product_name":"grape","unit_price":5}'),
(4,16, 2003, '2025-07-07', 'product',  N'{"product_name":"orange","unit_price":3}'),
(4,17, 2004, '2025-07-07', 'product',  N'{"product_name":"peach","unit_price":4}');


select * from RAW_LAYER.BATCH_EXTRACT;
select * from RAW_LAYER.ENTITY_EXTRACT;
