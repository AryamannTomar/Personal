-- select count(*), sum(c.${timeUnit}count) from (select EXTRACT(${timeUnit} from dt) unit, EXTRACT(year from dt) y, theta_sketch_get_estimate(theta_sketch_union(usercount)) ${timeUnit}count 
-- from datasketches_dailyactiveusers_${appid} where dt >= '${startDate}' and dt < '${endDate}' group by unit, y) c
select * from get_unit(${appid}, ${timeUnit}, ${startDate}, ${endDate});