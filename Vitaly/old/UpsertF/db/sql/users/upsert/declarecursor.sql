DO $$
    BEGIN
    EXECUTE format('DECLARE test_cursor CURSOR WITH HOLD FOR select key, dt, did, segment, p FROM %s', ${events_tbl});
END $$;