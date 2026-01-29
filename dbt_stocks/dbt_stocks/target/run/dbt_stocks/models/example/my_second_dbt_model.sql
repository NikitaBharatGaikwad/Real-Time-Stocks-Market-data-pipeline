
  create or replace   view STOCKS_MDS.COMMON.my_second_dbt_model
  
  
  
  
  as (
    -- Use the `ref` function to select from other models

select *
from STOCKS_MDS.COMMON.my_first_dbt_model
where id = 1
  );

