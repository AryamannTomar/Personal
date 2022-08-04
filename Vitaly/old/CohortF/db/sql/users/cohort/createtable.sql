DO $$
BEGIN
    EXECUTE format('CREATE TABLE cohort_%s_%s_%s(type char, nextdate INTEGER, initialdate INTEGER, year INTEGER, nextdatecount INTEGER, initialdatecount INTEGER)', ${firstEvent}, ${secondEvent}, ${appid}); 
END
$$;