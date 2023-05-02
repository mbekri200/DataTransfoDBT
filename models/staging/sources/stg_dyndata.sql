
WITH shipsdyndatarow as (
    select * from {{ source('sourcesais', 'SHIPDYNAMICROWDATA') }} 
)

select * from shipsdyndatarow 