SELECT
a.dt initialdate,
CONCAT(to_char(a.dt, 'yyyy'), ' ', extract(doy from a.dt)) initialdateformat,
b.dt nextdate,
CONCAT(to_char(b.dt, 'yyyy'), ' ', extract(doy from b.dt)) nextdateformat,
a.p platform,
theta_sketch_get_estimate(theta_sketch_union(a.usercount)) initialdatecount,
theta_sketch_get_estimate(theta_sketch_intersection(theta_sketch_union(a.usercount), theta_sketch_union(b.usercount))) nextdatecount
FROM datasketches_events_${appid:value} a
JOIN datasketches_events_${appid:value} b ON a.p=b.p
WHERE a.eventkey = ${firstEvent} AND b.eventkey = ${secondEvent} AND a.dt <= b.dt
GROUP BY initialdate, nextdate, initialdateformat, nextdateformat, platform
ORDER BY initialdate ASC
