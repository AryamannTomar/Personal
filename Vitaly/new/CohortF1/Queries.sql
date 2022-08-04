SELECT
a.dt initialdate,
CONCAT(extract(doy from a.dt), ' ', to_char(a.dt, 'yyyy')) initialdateformat,
b.dt nextdate,
CONCAT(extract(doy from b.dt), ' ', to_char(b.dt, 'yyyy')) nextdateformat,
a.p platform,
theta_sketch_get_estimate(theta_sketch_union(a.usercount)) initialdatecount,
theta_sketch_get_estimate(theta_sketch_intersection(theta_sketch_union(a.usercount), theta_sketch_union(b.usercount))) nextdatecount
FROM datasketches_events_5bebe93c25d705690ffbc75811 a
JOIN datasketches_events_5bebe93c25d705690ffbc75811 b ON a.p=b.p
WHERE a.eventkey = 'Login' AND b.eventkey = 'Session_Start' AND a.dt <= b.dt AND a.dt>='2021-09-01' AND b.dt<'2021-09-10'
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
CONCAT(DATE_PART('week', dt), ' ', to_char(dt, 'yyyy')) AS date,
p,
theta_sketch_union(usercount) as usercount
FROM datasketches_events_5bebe93c25d705690ffbc75811 m
WHERE m.eventkey='Login' AND m.dt>='2021-09-01'
GROUP BY date, p, dt
ORDER BY date) a
Join (SELECT distinct on (date, p)
dt,
CONCAT(DATE_PART('week', dt), ' ', to_char(dt, 'yyyy')) AS date,
p,
theta_sketch_union(usercount) as usercount
FROM datasketches_events_5bebe93c25d705690ffbc75811 n
WHERE n.eventkey='Session_Start' AND n.dt<'2021-09-10'
GROUP BY date, p, dt
ORDER BY date) b on a.p = b.p
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
to_char(dt, 'mm yyyy') AS date,
p,
theta_sketch_union(usercount) as usercount
FROM datasketches_events_5bebe93c25d705690ffbc75811 m
WHERE m.eventkey='Login' AND m.dt>='2021-09-01'
GROUP BY date, p, dt
ORDER BY date) a
Join (SELECT distinct on(date, p)
dt,
to_char(dt, 'mm yyyy') AS date,
p,
theta_sketch_union(usercount) as usercount
FROM datasketches_events_5bebe93c25d705690ffbc75811 n
WHERE n.eventkey='Session_Start' AND n.dt<'2021-09-10'
GROUP BY date, p, dt
ORDER BY date) b on a.p = b.p
WHERE RIGHT(a.date, 2) <= RIGHT(b.date, 2)
GROUP BY initialdate, nextdate, initialdateformat, nextdateformat, platform
ORDER BY initialdate ASC