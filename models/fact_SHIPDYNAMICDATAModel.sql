with PreDataDynSuccessive as 
(SELECT SHIPDYNAMICROWDATA.MMSI, LAG(SHIPDYNAMICROWDATA.EventEnqueuedUtcTime,1,0) OVER (PARTITION BY SHIPDYNAMICROWDATA.MMSI ORDER BY SHIPDYNAMICROWDATA.EventEnqueuedUtcTime) AS horodotage_precedent ,
       SHIPDYNAMICROWDATA.EventEnqueuedUtcTime as horodotage_actuel,
       ST_MAKEPOINT(LAG(SHIPDYNAMICROWDATA.Latitude,1,0) OVER (PARTITION BY SHIPDYNAMICROWDATA.MMSI ORDER BY SHIPDYNAMICROWDATA.EventEnqueuedUtcTime),LAG(SHIPDYNAMICROWDATA.longitude,1,0) OVER (PARTITION BY SHIPDYNAMICROWDATA.MMSI ORDER BY SHIPDYNAMICROWDATA.EventEnqueuedUtcTime))AS position_precedente,
       ST_MAKEPOINT(SHIPDYNAMICROWDATA.Latitude,SHIPDYNAMICROWDATA.longitude) as position_actuelle,
       
       LAG(SHIPDYNAMICROWDATA.Sog,1,0) OVER (PARTITION BY SHIPDYNAMICROWDATA.MMSI ORDER BY SHIPDYNAMICROWDATA.EventEnqueuedUtcTime) AS Sog_precedente,
       ST_DISTANCE(position_precedente,position_actuelle)/1609 as distance_parcourue_miles,
       SHIPDYNAMICROWDATA.Sog ,
       SHIPDYNAMICROWDATA.NavigationalStatus,
       StaticShipData.CO2emissionKgPermile * distance_parcourue_miles as QtteCO2emise ,
       StaticShipData.FuelConsoKgPermile * distance_parcourue_miles as QttFuelConsomme
FROM {{ ref ('stg_dyndata')}} SHIPDYNAMICROWDATA
LEFT join {{ ref ('dim_staticdataships')}} StaticShipData    -----------------revoir
ON SHIPDYNAMICROWDATA.MMSI=StaticShipData.MMSI),


DataDynSuccessive as     
(
    SELECT * from PreDataDynSuccessive
    WHERE horodotage_precedent <> 0
)

select * from DataDynSuccessive  
 order by QTTECO2EMISE DESC