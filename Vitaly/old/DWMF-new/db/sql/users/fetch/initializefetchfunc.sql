CREATE OR REPLACE FUNCTION get_daywisedau(appid text, dt1 text, dt2 text)
	RETURNS TABLE (
	dt DATE,
	theta_sketch_get_estimate INT
) AS $$
DECLARE 
	var_r record;
BEGIN 
	FOR var_r IN EXECUTE format('select dt, theta_sketch_get_estimate(theta_sketch_union(usercount)) from datasketches_dailyactiveusers_%s where dt BETWEEN ''%s'' AND ''%s'' GROUP BY dt ORDER BY dt 
', appid, dt1, dt2)
	LOOP
		dt := var_r.dt;
		theta_sketch_get_estimate := var_r.theta_sketch_get_estimate;
		RETURN NEXT;
	END LOOP;
END; $$
LANGUAGE 'plpgsql';

CREATE OR REPLACE FUNCTION get_daywisedau_p(appid text, dt1 text, dt2 text, p integer)
	RETURNS TABLE (
	dt DATE,
	theta_sketch_get_estimate INT
) AS $$
DECLARE 
	var_r record;
BEGIN 
	FOR var_r IN EXECUTE format('select dt, theta_sketch_get_estimate(theta_sketch_union(usercount)) from datasketches_dailyactiveusers_%s where dt BETWEEN ''%s'' AND ''%s'' AND p=%s GROUP BY dt ORDER BY dt 
', appid, dt1, dt2, p)
	LOOP
		dt := var_r.dt;
		theta_sketch_get_estimate := var_r.theta_sketch_get_estimate;
		RETURN NEXT;
	END LOOP;
END; $$
LANGUAGE 'plpgsql';


CREATE OR REPLACE FUNCTION get_unit(appid text, timeunit text, dt1 text, dt2 text)
	RETURNS TABLE (
	count INT,
	sum INT
) AS $$
DECLARE 
	var_r record;
BEGIN 
	FOR var_r IN EXECUTE format('select count(*), sum(c.%s_count) from (select EXTRACT(%s from dt) unit, EXTRACT(year from dt) y, theta_sketch_get_estimate(theta_sketch_union(usercount)) %s_count 
from datasketches_dailyactiveusers_%s where dt >= ''%s'' and dt < ''%s'' group by unit, y) c', timeunit, timeunit, timeunit, appid, dt1, dt2)
	LOOP
		count := var_r.count;
		sum := var_r.sum;
		RETURN NEXT;
	END LOOP;
END; $$
LANGUAGE 'plpgsql';

CREATE OR REPLACE FUNCTION get_unit_p(appid text, timeunit text, dt1 text, dt2 text, p integer)
	RETURNS TABLE (
	count INT,
	sum INT
) AS $$
DECLARE 
	var_r record;
BEGIN 
	FOR var_r IN EXECUTE format('select count(*), sum(c.%s_count) from (select EXTRACT(%s from dt) unit, EXTRACT(year from dt) y, theta_sketch_get_estimate(theta_sketch_union(usercount)) %s_count 
from datasketches_dailyactiveusers_%s where dt >= ''%s'' and dt < ''%s'' and p=%s group by unit, y) c', timeunit, timeunit, timeunit, appid, dt1, dt2, p)
	LOOP
		count := var_r.count;
		sum := var_r.sum;
		RETURN NEXT;
	END LOOP;
END; $$
LANGUAGE 'plpgsql';