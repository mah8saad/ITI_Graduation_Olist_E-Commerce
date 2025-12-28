select 
     cast (GEOLOCATION_ZIP_CODE_PREFIX as string) as Zip_Code,
     GEOLOCATION_LAT,
     GEOLOCATION_LNG,
     INITCAP(TRIM(GEOLOCATION_CITY)) as City,
     INITCAP(TRIM(GEOLOCATION_STATE)) as State
from {{source('Bronze','GEOLOCATION')}}