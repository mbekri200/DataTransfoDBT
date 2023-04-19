
WITH shipsdyndatarow as (
    select * from {{ source('sources', 'SHIPDYNAMICROWDATA') }} 
)

select * from shipsdyndatarow 