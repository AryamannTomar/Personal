DO $$
BEGIN
    EXECUTE format('INSERT INTO cohort_%s_%s_%s VALUES ($1, $2, $3, $4, $5, $6)', ${firstEvent}, ${secondEvent}, ${appid}) USING ${timeUnit}, ${nextdate}, ${initialdate}, ${year}, ${nextdatecount}, ${initialdatecount};
    END
$$;
