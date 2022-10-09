SELECT DATE(createdAt) as Date, count(*) as Total_message FROM `rational-camera-361514.spark_network_challenge.message_table` group by 1 order by 1;

SELECT id as User FROM `spark_network_challenge.users_table` where id not in (SELECT receiverId from `spark_network_challenge.message_table`);

SELECT count(subscription.createdAt) as Total_active_subscription FROM  
`spark_network_challenge.users_table`  LEFT JOIN UNNEST(subscription) subscription
where subscription.status = 'Active';


Select Distinct senderID from `spark_network_challenge.message_table` where senderId not in (Select Distinct id from `spark_network_challenge.users_table` users inner join UNNEST(subscription) subscription where subscription.status = 'Active');

SELECT FORMAT_DATE("%b %Y", DATE(subscription.createdAt)) as date, Avg(CAST( subscription.amount AS NUMERIC)) as subscription_amount
FROM `rational-camera-361514.spark_network_challenge.users_table` 
LEFT JOIN UNNEST(subscription) subscription
GROUP BY 1;