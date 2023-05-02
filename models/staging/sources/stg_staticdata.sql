WITH stg_stat_data as (
    select MMSI
           , ImoNumber
           ,CallSign
           ,Name
           ,Typeofvessel
           ,DimA+DimB as longueur
           ,DimC+DimD as largeur
           ,Destination
           ,TIMESTAMP_LTZ_FROM_PARTS(2023,etamois,etaday,etahour,etaminute,0) as datearrivee
     from {{ source('sourcesais', 'globalSHIPSTATICROWDATA') }} 
)

select * from stg_stat_data