select dt, theta_sketch_get_estimate(theta_sketch_union(usercount)) from datasketches_dailyactiveusers_${appid:value} where dt BETWEEN ${startDate} AND ${endDate} GROUP BY dt ORDER BY dt 