SELECT
a.date initialdate,
b.date nextdate,
sum(theta_sketch_get_estimate(a.usercount)) initialdatecount,
sum(theta_sketch_get_estimate(theta_sketch_intersection(a.usercount, b.usercount))) nextdatecount
from (SELECT
CONCAT(to_char(dt, 'yyyy'), ' ', DATE_PART('week', dt)) AS date,
p,
theta_sketch_union(usercount) as usercount
FROM datasketches_events_5bebe93c25d705690ffbc75811 m
WHERE m.eventkey=${firstEvent}
GROUP BY date, p
ORDER BY date) a
Join (SELECT
CONCAT(to_char(dt, 'yyyy'), ' ', DATE_PART('week', dt)) AS date,
p,
theta_sketch_union(usercount) as usercount
FROM datasketches_events_5bebe93c25d705690ffbc75811 n
WHERE n.eventkey=${secondEvent}
GROUP BY date, p
ORDER BY date) b on a.p = b.p
WHERE RIGHT(a.date, 2) <= RIGHT(b.date, 2)
GROUP BY initialdate, nextdate
ORDER BY initialdate ASC