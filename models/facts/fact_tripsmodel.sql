{{
    config(
        materialized='incremental'
    )
}}

with datedeparture as (
SELECT MMSI , MIN(HORODOTAGE_PRECEDENT) AS DepartureDate
from {{ ref ('fact_SHIPDYNAMICDATAModel')}}
where QTTFUELCONSOMME IS NOT null 
AND ((SOG_PRECEDENTE = 0 AND SOG <> 0) OR (SOG_PRECEDENTE <> 0 AND SOG <> 0) )
group by MMSI),

--SELECT * from datedeparture

DateAndPosDeparture as (
SELECT datedeparture.MMSI
       ,datedeparture.DepartureDate
       ,position_precedente as PosDepart
FROM datedeparture 
join {{ ref ('fact_SHIPDYNAMICDATAModel')}} DataDynSuccessive
on datedeparture.MMSI=DataDynSuccessive.MMSI AND datedeparture.DepartureDate = horodotage_precedent
),

--select * from DateAndPosDeparture

 possuccessivetrips as (
                    select DataDynSuccessive.MMSI 
                    ,DataDynSuccessive.horodotage_precedent
                    ,DataDynSuccessive.horodotage_actuel
                    ,globalSHIPSTATICROWDATA.datearrivee
                    ,DataDynSuccessive.QtteCO2emise
                    --,globalSHIPSTATICROWDATA.datearrivee
                    from {{ ref ('fact_SHIPDYNAMICDATAModel')}} DataDynSuccessive
                    left join SHIPS_DATA.ROWDATASCHEMA.globalSHIPSTATICROWDATA globalSHIPSTATICROWDATA
                    ON DataDynSuccessive.MMSI=globalSHIPSTATICROWDATA.MMSI
                    WHERE globalSHIPSTATICROWDATA.datearrivee >= DataDynSuccessive.horodotage_actuel 
                    --group by MMSI
                    ),

SumCo2Instantanne as(
        select       MMSI
                     ,datearrivee
                    ,sum(QtteCO2emise) as sumco2
                    ,min(horodotage_precedent) as minhoro 
                    ,max(horodotage_actuel) as maxhoro
        from         possuccessivetrips
        group by MMSI , datearrivee
),

trips as (
SELECT distinct  globalSHIPSTATICROWDATA.MMSI 
      ,DateAndPosDeparture.DepartureDate
      ,DateAndPosDeparture.PosDepart as PositionDepart
      ,globalSHIPSTATICROWDATA.Destination 
      ,globalSHIPSTATICROWDATA.datearrivee
      ,ports.position as DestinationPortPosition
      , SumCo2Instantanne.sumco2 as QteCO2ConsommeeInstantannee
      , SumCo2Instantanne.maxhoro as dernieredatedemesure
      , StaticShipData.CO2emissionKgPermile* st_distance(PositionDepart,DestinationPortPosition) as QteCO2prevue
      
     from {{ ref ('stg_staticdata')}} globalSHIPSTATICROWDATA
     INNER  join {{ ref ('stg_ports')}} ports on UPPER(ports.PortName) like UPPER(globalSHIPSTATICROWDATA.DESTINATION)
      INNER join  DateAndPosDeparture on DateAndPosDeparture.MMSI=globalSHIPSTATICROWDATA.MMSI 
                                                                                      AND DateAndPosDeparture.DepartureDate <= globalSHIPSTATICROWDATA.datearrivee
     INNER join {{ ref ('dim_staticdataships')}} StaticShipData on StaticShipData.MMSI = globalSHIPSTATICROWDATA.MMSI
     INNER JOIN  SumCo2Instantanne on globalSHIPSTATICROWDATA.MMSI = SumCo2Instantanne.MMSI 
                                                                                     and SumCo2Instantanne.datearrivee =globalSHIPSTATICROWDATA.datearrivee
)

select * from trips
{% if is_incremental() %}

  -- this filter will only be applied on an incremental run
  where DERNIEREDATEDEMESURE > (select max(DERNIEREDATEDEMESURE) from {{ this }})

{% endif %}