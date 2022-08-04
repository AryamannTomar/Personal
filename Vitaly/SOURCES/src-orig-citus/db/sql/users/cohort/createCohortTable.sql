CREATE TABLE IF NOT EXISTS cohort_${guid:value}_${appid:value}(type char, year INTEGER, initialdate TEXT, initialdateformat INTEGER, nextdate TEXT, nextdateformat INTEGER, platform SMALLINT, initialdatecount INTEGER, nextdatecount INTEGER)

do $$
begin
    EXECUTE format('SELECT create_distributed_table(''cohort_%s_%s'', ''type'')', ${guid}, ${appid});
end $$;