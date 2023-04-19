WITH stg_ports as (
    select * from {{ source('sources', 'Ports') }} 
)

select * from stg_ports