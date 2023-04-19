WITH stg_stat_data as (
    select * from {{ source('sources', 'globalSHIPSTATICROWDATA') }} 
)

select * from stg_stat_data