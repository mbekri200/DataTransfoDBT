{{
    config(
        materialized='incremental',
        "unique_key='MMSI'"

    )
}}
with StaticShipData as 
 (
     SELECT DISTINCT SHIPSTATICROWDATA.MMSI,
            SHIPSTATICROWDATA.ImoNumber, 
            
            SHIPSTATICROWDATA.CallSign, 
            SHIPSTATICROWDATA.Name,
            SHIPSTATICROWDATA.Typeofvessel,
            SHIPSTATICROWDATA.longueur, 
            SHIPSTATICROWDATA.largeur, 
            RawShipsConso.FuelConsoKgPermile,
            RawShipsConso.CO2emissionKgPermile
    from {{ ref ('stg_staticdata')}} SHIPSTATICROWDATA
    LEFT JOIN {{ ref ('stg_ships_conso')}} RawShipsConso
    ON 
        SHIPSTATICROWDATA.IMONUMBER = RawShipsConso.ImoNumber or SHIPSTATICROWDATA.Name = RawShipsConso.Name
 )

 select * from StaticShipData