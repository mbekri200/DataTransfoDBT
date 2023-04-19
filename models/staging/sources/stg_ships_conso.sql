WITH stg_datashipsconso as (
    select * from {{ source('sources', 'RawShipsConsoOr') }} 
    WHERE FuelConsoKgPermile <> 'Division by zero!'AND CO2emissionKgPermile <> 'Division by zero!' 
)

select * from stg_datashipsconso