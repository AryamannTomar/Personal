SELECT
a.dt initialdate,
CONCAT(extract(doy from a.dt), ' ', to_char(a.dt, 'yyyy')) initialdateformat,
b.dt nextdate,
CONCAT(extract(doy from b.dt), ' ', to_char(b.dt, 'yyyy')) nextdateformat,
a.p platform,
theta_sketch_get_estimate(theta_sketch_union(a.usercount)) initialdatecount,
theta_sketch_get_estimate(theta_sketch_intersection(theta_sketch_union(a.usercount), theta_sketch_union(b.usercount))) nextdatecount
FROM datasketches_events_${appid:value} a
JOIN datasketches_events_${appid:value} b ON a.p=b.p
WHERE a.eventkey = ${firstEvent}AND b.eventkey = ${secondEvent}AND a.dt <= b.dt AND a.dt>=${startDate} AND b.dt<${endDate}
GROUP BY initialdate, nextdate, initialdateformat, nextdateformat, platform
ORDER BY initialdate ASC