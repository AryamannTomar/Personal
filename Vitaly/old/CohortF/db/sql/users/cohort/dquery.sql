SELECT
CONCAT(to_char(a.dt, 'yyyy'), ' ', extract(doy from a.dt)) initialdate,
CONCAT(to_char(b.dt, 'yyyy'), ' ', extract(doy from b.dt)) nextdate,
theta_sketch_get_estimate(theta_sketch_union(a.usercount)) initialdatecount,
theta_sketch_get_estimate(theta_sketch_intersection(theta_sketch_union(a.usercount), theta_sketch_union(b.usercount))) nextdatecount
FROM datasketches_events_5bebe93c25d705690ffbc75811 a
JOIN datasketches_events_5bebe93c25d705690ffbc75811 b ON a.p=b.p
WHERE a.eventkey = ${firstEvent} AND b.eventkey = ${secondEvent} AND a.dt <= b.dt
GROUP BY initialdate, nextdate
ORDER BY initialdate ASC