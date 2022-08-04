SELECT
a.dt initialdate,
CONCAT(to_char(a.dt, 'yyyy'), ' ', extract(doy from a.dt)) initialdateformat,
b.dt nextdate,
CONCAT(to_char(b.dt, 'yyyy'), ' ', extract(doy from b.dt)) nextdateformat,
a.p platform,
theta_sketch_get_estimate(theta_sketch_union(a.usercount)) initialdatecount,
theta_sketch_get_estimate(theta_sketch_intersection(theta_sketch_union(a.usercount), theta_sketch_union(b.usercount))) nextdatecount
FROM datasketches_events_${appid:value} a
JOIN datasketches_events_${appid:value} b ON a.p=b.p and a.p=${platform}
WHERE a.eventkey = ${firstEvent}AND b.eventkey = ${secondEvent}AND a.dt <= b.dt AND a.dt>=${startDate} AND b.dt<${endDate}
GROUP BY initialdate, nextdate, initialdateformat, nextdateformat, platform
ORDER BY initialdate ASC

xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx

SELECT
a.dt initialdate,
a.date initialdateformat,
b.dt nextdate,
b.date nextdateformat,
a.p platform,
sum(theta_sketch_get_estimate(a.usercount)) initialdatecount,
sum(theta_sketch_get_estimate(theta_sketch_intersection(a.usercount, b.usercount))) nextdatecount
from (SELECT distinct on (date, p)
dt,
CONCAT(to_char(dt, 'yyyy'), ' ', DATE_PART('week', dt)) AS date,
p,
theta_sketch_union(usercount) as usercount
FROM datasketches_events_${appid:value} m
WHERE m.eventkey=${firstEvent}AND m.dt>=${startDate}
GROUP BY date, p, dt
ORDER BY date) a
Join (SELECT distinct on (date, p)
dt,
CONCAT(to_char(dt, 'yyyy'), ' ', DATE_PART('week', dt)) AS date,
p,
theta_sketch_union(usercount) as usercount
FROM datasketches_events_${appid:value} n
WHERE n.eventkey=${secondEvent}AND n.dt<${endDate}
GROUP BY date, p, dt
ORDER BY date) b on a.p = b.p and a.p=${platform}
WHERE RIGHT(a.date, 2) <= RIGHT(b.date, 2)
GROUP BY initialdate, nextdate, initialdateformat, nextdateformat, platform
ORDER BY initialdate ASC

xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx

SELECT
a.dt initialdate,
b.dt nextdate,
a.p platform,
a.date initialdateformat,
b.date nextdateformat,
sum(theta_sketch_get_estimate(a.usercount)) initialdatecount,
sum(theta_sketch_get_estimate(theta_sketch_intersection(a.usercount, b.usercount))) nextdatecount
from (SELECT distinct on(date, p)
dt,
to_char(dt, 'yyyy mm') AS date,
p,
theta_sketch_union(usercount) as usercount
FROM datasketches_events_${appid:value} m
WHERE m.eventkey=${firstEvent}AND m.dt>=${startDate}
GROUP BY date, p, dt
ORDER BY date) a
Join (SELECT distinct on(date, p)
dt,
to_char(dt, 'yyyy mm') AS date,
p,
theta_sketch_union(usercount) as usercount
FROM datasketches_events_${appid:value} n
WHERE n.eventkey=${secondEvent}AND n.dt<${endDate}
GROUP BY date, p, dt
ORDER BY date) b on a.p = b.p and a.p=${platform}
WHERE RIGHT(a.date, 2) <= RIGHT(b.date, 2)
GROUP BY initialdate, nextdate, initialdateformat, nextdateformat, platform
ORDER BY initialdate ASC