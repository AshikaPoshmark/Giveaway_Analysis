

DROP TABLE IF EXISTS analytics_scratch.ashika_giveaway_show_details;
CREATE TABLE analytics_scratch.ashika_giveaway_show_details AS
    SELECT distinct dw_giveaways.show_id,
           dw_shows.creator_id,
           dw_shows.start_at,
           (TO_CHAR(DATE(DATEADD(day,(0 - EXTRACT(DOW FROM dw_shows.start_at )::integer), dw_shows.start_at )), 'YYYY-MM-DD')) as show_start_week,
           dw_shows.origin_domain,
           CASE WHEN type = 'silent' OR title ILIKE '%silent%' THEN 'Yes' ELSE 'No' END AS Is_silent_show,
           viewers.viewer_id,
           dw_users_cs.show_viewer_activated_at,
           dw_users_cs.silent_show_view_activated_at,
           dw_users_cs.live_show_view_activated_at,
           dw_users_cs.show_buyer_activated_at,
           dw_users_cs.silent_show_buyer_activated_at,
           dw_users_cs.live_show_buyer_activated_at,
           dw_users.buyer_activated_at,
           (TO_CHAR(DATE(DATEADD(day,(0 - EXTRACT(DOW FROM dw_users_cs.show_viewer_activated_at )::integer), dw_users_cs.show_viewer_activated_at)), 'YYYY-MM-DD')) as show_viewer_activated_week,
           (TO_CHAR(DATE(DATEADD(day,(0 - EXTRACT(DOW FROM dw_users_cs.silent_show_view_activated_at)::integer), dw_users_cs.silent_show_view_activated_at )), 'YYYY-MM-DD')) as silent_show_view_activated_week,
           (TO_CHAR(DATE(DATEADD(day,(0 - EXTRACT(DOW FROM dw_users_cs.live_show_view_activated_at )::integer), dw_users_cs.live_show_view_activated_at )), 'YYYY-MM-DD')) as live_show_view_activated_week,
           (TO_CHAR(DATE(DATEADD(day,(0 - EXTRACT(DOW FROM dw_users_cs.show_buyer_activated_at )::integer), dw_users_cs.show_buyer_activated_at )), 'YYYY-MM-DD')) as show_buyer_activated_week,
           (TO_CHAR(DATE(DATEADD(day,(0 - EXTRACT(DOW FROM dw_users_cs.silent_show_buyer_activated_at )::integer), dw_users_cs.silent_show_buyer_activated_at)), 'YYYY-MM-DD')) as silent_show_buyer_activated_week,
           (TO_CHAR(DATE(DATEADD(day,(0 - EXTRACT(DOW FROM dw_users_cs.live_show_buyer_activated_at )::integer), dw_users_cs.live_show_buyer_activated_at )), 'YYYY-MM-DD')) as live_show_buyer_activated_week,
           (TO_CHAR(DATE(DATEADD(day,(0 - EXTRACT(DOW FROM dw_users.buyer_activated_at)::integer), dw_users.buyer_activated_at )), 'YYYY-MM-DD')) as buyer_activated_week,
           t.participant_id,
           CASE WHEN t.participant_id IS NOT NULL THEN 'Yes' ELSE 'No' END AS Is_giveaway_participant

    FROM analytics.dw_giveaways
LEFT JOIN analytics.dw_shows ON dw_shows.show_id = dw_giveaways.show_id
LEFT JOIN analytics.dw_show_viewer_events_cs as viewers ON viewers.show_id = dw_giveaways.show_id
LEFT JOIN analytics.dw_users_cs ON viewers.viewer_id = dw_users_cs.user_id
LEFT JOIN analytics.dw_users ON viewers.viewer_id = dw_users.user_id
LEFT JOIN
        (SELECT DISTINCT dw_giveaways.show_id,
                participant_id
           FROM analytics.dw_giveaway_entries
           LEFT JOIN analytics.dw_giveaways ON dw_giveaways.giveaway_id = dw_giveaway_entries.giveaway_id) AS t
            ON viewers.viewer_id = t.participant_id AND dw_giveaways.show_id = t.show_id
WHERE origin_domain = 'us';
GRANT ALL ON analytics_scratch.ashika_giveaway_show_details TO PUBLIC;

------------- viewer reactivated -----------------

-------------- Overall,live,silent  -----------------------------------

DROP TABLE IF EXISTS analytics_scratch.ashika_viewer_reactivated_details1;
CREATE TABLE analytics_scratch.ashika_viewer_reactivated_details1 AS

WITH overall_reactivated AS (
SELECT DISTINCT t.*,
                      max(dw_shows.start_at) over (partition by participant_id,t.show_id ) as last_show_viewed_at
      FROM (SELECT DISTINCT dw_giveaways.show_id, dw_giveaway_entries.participant_id, dw_shows.start_at

            from analytics.dw_giveaway_entries
                     LEFT JOIN analytics.dw_giveaways ON dw_giveaway_entries.giveaway_id = dw_giveaways.giveaway_id
                     LEFT JOIN analytics.dw_shows ON dw_shows.show_id = dw_giveaways.show_id) AS t
               LEFT JOIN analytics.dw_show_viewer_events_cs ON t.participant_id = dw_show_viewer_events_cs.viewer_id
               LEFT JOIN analytics.dw_shows ON dw_shows.show_id = dw_show_viewer_events_cs.show_id
      WHERE  (TO_CHAR(DATE(DATEADD(day,(0 - EXTRACT(DOW FROM dw_show_viewer_events_cs.event_at )::integer), dw_show_viewer_events_cs.event_at)), 'YYYY-MM-DD')) <
             (TO_CHAR(DATE(DATEADD(day,(0 - EXTRACT(DOW FROM t.start_at )::integer), t.start_at )), 'YYYY-MM-DD')) AND dw_shows.start_at IS NOT NULL

),
    live_reactivated AS (
SELECT DISTINCT t.*,
                      max(dw_shows.start_at) over (partition by participant_id,t.show_id ) as last_live_show_viewed_at
      FROM (SELECT DISTINCT dw_giveaways.show_id, dw_giveaway_entries.participant_id, dw_shows.start_at

            from analytics.dw_giveaway_entries
                     LEFT JOIN analytics.dw_giveaways ON dw_giveaway_entries.giveaway_id = dw_giveaways.giveaway_id
                     LEFT JOIN analytics.dw_shows ON dw_shows.show_id = dw_giveaways.show_id) AS t
               LEFT JOIN analytics.dw_show_viewer_events_cs ON t.participant_id = dw_show_viewer_events_cs.viewer_id
               LEFT JOIN analytics.dw_shows ON dw_shows.show_id = dw_show_viewer_events_cs.show_id
      WHERE (TO_CHAR(DATE(DATEADD(day,(0 - EXTRACT(DOW FROM dw_show_viewer_events_cs.event_at )::integer), dw_show_viewer_events_cs.event_at)), 'YYYY-MM-DD')) <
             (TO_CHAR(DATE(DATEADD(day,(0 - EXTRACT(DOW FROM t.start_at )::integer), t.start_at )), 'YYYY-MM-DD')) AND dw_shows.start_at IS NOT NULL AND
            (CASE WHEN dw_shows.type = 'silent' OR dw_shows.title  ILIKE '%silent%' THEN 'Yes' ELSE 'No' END) = 'No'
 ),
    silent_show_reactivated AS (
SELECT DISTINCT t.*,
                      max(dw_shows.start_at) over (partition by participant_id,t.show_id ) as last_silent_show_viewed_at
      FROM (SELECT DISTINCT dw_giveaways.show_id, dw_giveaway_entries.participant_id, dw_shows.start_at

            from analytics.dw_giveaway_entries
                     LEFT JOIN analytics.dw_giveaways ON dw_giveaway_entries.giveaway_id = dw_giveaways.giveaway_id
                     LEFT JOIN analytics.dw_shows ON dw_shows.show_id = dw_giveaways.show_id) AS t
               LEFT JOIN analytics.dw_show_viewer_events_cs ON t.participant_id = dw_show_viewer_events_cs.viewer_id
               LEFT JOIN analytics.dw_shows ON dw_shows.show_id = dw_show_viewer_events_cs.show_id
      WHERE (TO_CHAR(DATE(DATEADD(day,(0 - EXTRACT(DOW FROM dw_show_viewer_events_cs.event_at )::integer), dw_show_viewer_events_cs.event_at)), 'YYYY-MM-DD')) <
             (TO_CHAR(DATE(DATEADD(day,(0 - EXTRACT(DOW FROM t.start_at )::integer), t.start_at )), 'YYYY-MM-DD')) AND dw_shows.start_at IS NOT NULL AND
            (CASE WHEN dw_shows.type = 'silent' OR dw_shows.title  ILIKE '%silent%' THEN 'Yes' ELSE 'No' END) = 'Yes'
 )
SELECT overall_reactivated.*,last_live_show_viewed_at,last_silent_show_viewed_at FROM overall_reactivated
LEFT JOIN live_reactivated ON  live_reactivated.participant_id = overall_reactivated.participant_id
AND live_reactivated.show_id =overall_reactivated.show_id
AND live_reactivated.start_at = overall_reactivated.start_at

LEFT JOIN silent_show_reactivated ON  silent_show_reactivated.participant_id = overall_reactivated.participant_id
AND silent_show_reactivated.show_id =overall_reactivated.show_id
AND silent_show_reactivated.start_at = overall_reactivated.start_at;


---------- show buyer reactivated


------------- Show Buyer reactivated (live,silent,overall)-----------------


DROP TABLE IF EXISTS analytics_scratch.ashika_buyer_reactivated_details1;
CREATE TABLE analytics_scratch.ashika_buyer_reactivated_details1 AS

WITH last_show_buy AS (SELECT DISTINCT t.show_id,t.participant_id,t.start_at,
                               max(booked_at_time) over (partition by participant_id,t.show_id) AS last_show_booked_at
FROM (SELECT DISTINCT dw_giveaways.show_id,dw_giveaway_entries.participant_id,dw_shows.start_at

      from analytics.dw_giveaway_entries
    LEFT JOIN analytics.dw_giveaways ON dw_giveaway_entries.giveaway_id = dw_giveaways.giveaway_id
    LEFT JOIN analytics.dw_shows ON dw_shows.show_id = dw_giveaways.show_id
    LEFT JOIN analytics.dw_orders ON buyer_id = participant_id AND dw_orders.show_id  = dw_giveaways.show_id
    LEFT JOIN analytics.dw_order_items ON dw_order_items.order_id = dw_orders.order_id
      GROUP BY 1,2,3) AS t

    LEFT JOIN analytics.dw_orders ON buyer_id = participant_id AND dw_orders.show_id  IS NOT NULL
    LEFT JOIN analytics.dw_order_items ON dw_order_items.order_id = dw_orders.order_id
WHERE (TO_CHAR(DATE(DATEADD(day,(0 - EXTRACT(DOW FROM dw_order_items.booked_at_time )::integer), dw_order_items.booked_at_time)), 'YYYY-MM-DD')) <
             (TO_CHAR(DATE(DATEADD(day,(0 - EXTRACT(DOW FROM t.start_at )::integer), t.start_at )), 'YYYY-MM-DD'))),
 last_live_show_buy AS (SELECT DISTINCT t.show_id,t.participant_id,t.start_at,
                               max(booked_at_time) over (partition by participant_id,t.show_id) AS last_live_show_booked_at
FROM (SELECT DISTINCT dw_giveaways.show_id,dw_giveaway_entries.participant_id,dw_shows.start_at

      from analytics.dw_giveaway_entries
    LEFT JOIN analytics.dw_giveaways ON dw_giveaway_entries.giveaway_id = dw_giveaways.giveaway_id
    LEFT JOIN analytics.dw_shows ON dw_shows.show_id = dw_giveaways.show_id
    LEFT JOIN analytics.dw_orders ON buyer_id = participant_id AND dw_orders.show_id  = dw_giveaways.show_id
    LEFT JOIN analytics.dw_order_items ON dw_order_items.order_id = dw_orders.order_id
      GROUP BY 1,2,3) AS t

    LEFT JOIN analytics.dw_orders ON buyer_id = participant_id AND dw_orders.show_id  IS NOT NULL
    LEFT JOIN analytics.dw_order_items ON dw_order_items.order_id = dw_orders.order_id
    LEFT JOIN analytics.dw_shows ON dw_orders.show_id = dw_shows.show_id
WHERE (TO_CHAR(DATE(DATEADD(day,(0 - EXTRACT(DOW FROM dw_order_items.booked_at_time )::integer), dw_order_items.booked_at_time)), 'YYYY-MM-DD')) <
             (TO_CHAR(DATE(DATEADD(day,(0 - EXTRACT(DOW FROM t.start_at )::integer), t.start_at )), 'YYYY-MM-DD'))
  AND (CASE WHEN dw_shows.type = 'silent' OR dw_shows.title  ILIKE '%silent%' THEN 'Yes' ELSE 'No' END) = 'No'),
 last_silent_show_buy AS (SELECT DISTINCT t.show_id,t.participant_id,t.start_at,
                               max(booked_at_time) over (partition by participant_id,t.show_id) AS last_silent_show_booked_at
FROM (SELECT DISTINCT dw_giveaways.show_id,dw_giveaway_entries.participant_id,dw_shows.start_at

      from analytics.dw_giveaway_entries
    LEFT JOIN analytics.dw_giveaways ON dw_giveaway_entries.giveaway_id = dw_giveaways.giveaway_id
    LEFT JOIN analytics.dw_shows ON dw_shows.show_id = dw_giveaways.show_id
    LEFT JOIN analytics.dw_orders ON buyer_id = participant_id AND dw_orders.show_id  = dw_giveaways.show_id
    LEFT JOIN analytics.dw_order_items ON dw_order_items.order_id = dw_orders.order_id
      GROUP BY 1,2,3) AS t

    LEFT JOIN analytics.dw_orders ON buyer_id = participant_id AND dw_orders.show_id  IS NOT NULL
    LEFT JOIN analytics.dw_order_items ON dw_order_items.order_id = dw_orders.order_id
    LEFT JOIN analytics.dw_shows ON dw_orders.show_id = dw_shows.show_id
WHERE (TO_CHAR(DATE(DATEADD(day,(0 - EXTRACT(DOW FROM dw_order_items.booked_at_time )::integer), dw_order_items.booked_at_time)), 'YYYY-MM-DD')) <
             (TO_CHAR(DATE(DATEADD(day,(0 - EXTRACT(DOW FROM t.start_at )::integer), t.start_at )), 'YYYY-MM-DD'))
  AND (CASE WHEN dw_shows.type = 'silent' OR dw_shows.title  ILIKE '%silent%' THEN 'Yes' ELSE 'No' END) = 'Yes')

SELECT last_show_buy.*,last_live_show_booked_at,last_silent_show_booked_at FROM last_show_buy
LEFT JOIN last_live_show_buy ON last_show_buy.show_id = last_live_show_buy.show_id
AND last_show_buy.participant_id = last_live_show_buy.participant_id
LEFT JOIN last_silent_show_buy ON last_show_buy.show_id = last_silent_show_buy.show_id
AND last_show_buy.participant_id = last_silent_show_buy.participant_id;

GRANT ALL ON analytics_scratch.ashika_buyer_reactivated_details1 TO PUBLIC;



------ giveaway participants show details

DROP TABLE IF EXISTS analytics_scratch.ashika_giveaway_show_participants_details;
CREATE TABLE analytics_scratch.ashika_giveaway_show_participants_details AS
SELECT DISTINCT dw_giveaway_entries.participant_id,
             dw_giveaways.show_id,
             dw_shows.start_at

      from analytics.dw_giveaway_entries
    LEFT JOIN analytics.dw_giveaways ON dw_giveaway_entries.giveaway_id = dw_giveaways.giveaway_id
    LEFT JOIN analytics.dw_shows ON dw_shows.show_id = dw_giveaways.show_id
WHERE dw_shows.origin_domain = 'us';
GRANT ALL ON analytics_scratch.ashika_giveaway_show_participants_details TO PUBLIC;




------------------ Giveaway participants activation details



DROP TABLE IF EXISTS analytics_scratch.ashika_giveaway_activation_details;
CREATE TABLE analytics_scratch.ashika_giveaway_activation_details AS

SELECT  gp.*,
        min(g.start_at) as giveaway_activation_at
FROM analytics_scratch.ashika_giveaway_show_participants_details  as gp
LEFT JOIN analytics_scratch.ashika_giveaway_show_participants_details as g ON gp.participant_id = g.participant_id
GROUP BY gp.participant_id, gp.show_id, gp.start_at;

GRANT ALL ON analytics_scratch.ashika_giveaway_activation_details TO PUBLIC;




------------------- giveaway show buyer -----------------------------------


DROP TABLE IF EXISTS analytics_scratch.ashika_giveaway_show_buy_details;
CREATE TABLE analytics_scratch.ashika_giveaway_show_buy_details AS
 SELECT DISTINCT t.show_id,t.participant_id,t.start_at,
                               CASE WHEN giveaway_show_winner>0 THEN 'Yes' ELSE 'No' END AS Is_giveaway_show_winner,
                               CASE WHEN giveaway_show_buyer>0 THEN 'Yes' ELSE 'No' END AS Is_giveaway_show_buyer
     FROM
(SELECT DISTINCT dw_giveaways.show_id,dw_giveaway_entries.participant_id,dw_shows.start_at,
              COUNT( DISTINCT CASE WHEN dw_giveaway_entries.participant_id = dw_giveaways.winner_participant_id THEN 1 END) AS giveaway_show_winner,
              COUNT( DISTINCT CASE WHEN dw_order_items.order_id IS NOT NULL AND dw_order_items.giveaway_id IS NULL  THEN 1  END) AS giveaway_show_buyer

      from analytics.dw_giveaway_entries
    LEFT JOIN analytics.dw_giveaways ON dw_giveaway_entries.giveaway_id = dw_giveaways.giveaway_id
    LEFT JOIN analytics.dw_shows ON dw_shows.show_id = dw_giveaways.show_id
    LEFT JOIN analytics.dw_orders ON buyer_id = participant_id AND dw_orders.show_id  = dw_giveaways.show_id
    LEFT JOIN analytics.dw_order_items ON dw_order_items.order_id = dw_orders.order_id
      WHERE dw_shows.origin_domain ='us'
      GROUP BY 1,2,3) AS t;



--------------------------------------------------------------- with re-activated date final participants base table


DROP TABLE IF EXISTS analytics_scratch.ashika_giveaway_show_viewer_details1;
CREATE TABLE analytics_scratch.ashika_giveaway_show_viewer_details1 AS
SELECT DISTINCT ashika_giveaway_show_details.*,
       last_show_viewed_at,
       last_live_show_viewed_at,
       last_silent_show_viewed_at,
       ashika_giveaway_show_buy_details.Is_giveaway_show_winner,
       ashika_giveaway_show_buy_details.Is_giveaway_show_buyer,
       last_show_booked_at,
       last_live_show_booked_at,
       last_silent_show_booked_at,
       giveaway_activation_at
FROM analytics_scratch.ashika_giveaway_show_details
LEFT JOIN analytics_scratch.ashika_viewer_reactivated_details1
    ON ashika_giveaway_show_details.participant_id = ashika_viewer_reactivated_details1.participant_id AND
       ashika_giveaway_show_details.show_id = ashika_viewer_reactivated_details1.show_id
LEFT JOIN analytics_scratch.ashika_buyer_reactivated_details1
    ON ashika_giveaway_show_details.participant_id = ashika_buyer_reactivated_details1.participant_id AND
       ashika_giveaway_show_details.show_id = ashika_buyer_reactivated_details1.show_id
LEFT JOIN analytics_scratch.ashika_giveaway_show_buy_details
    ON ashika_giveaway_show_details.participant_id = ashika_giveaway_show_buy_details.participant_id AND
       ashika_giveaway_show_details.show_id = ashika_giveaway_show_buy_details.show_id
LEFT JOIN analytics_scratch.ashika_giveaway_activation_details
        ON ashika_giveaway_show_details.participant_id = ashika_giveaway_activation_details.participant_id AND
       ashika_giveaway_show_details.show_id = ashika_giveaway_activation_details.show_id
WHERE show_start_week <= '2024-12-08' AND viewer_id IS NOT NULL;

GRANT ALL ON analytics_scratch.ashika_giveaway_show_viewer_details1 TO PUBLIC;

------------------------------------------------------------------------------------


------------------- viewed viewed time (whether they watched the giveaway show during when the giveaway happened -----------------

DROP TABLE IF EXISTS analytics_scratch.viewers_time_duration;
CREATE TABLE analytics_scratch.viewers_time_duration AS
SELECT DISTINCT
    dw_giveaways.show_id,
    dw_shows.start_at,
    dw_show_viewer_events_cs.viewer_id,
    CASE WHEN type = 'silent' OR title ILIKE '%silent%' THEN 'Yes' ELSE 'No' END AS Is_silent_show,
    event_at,
    views,
    total_watched_show_minutes,
    dateadd(minute,total_watched_show_minutes,event_at) As event_end_at,
    CASE WHEN views = 1 THEN '1 view' ELSE '> 1 view' END AS view_type
    FROM analytics.dw_giveaways
LEFT JOIN analytics.dw_shows ON dw_shows.show_id = dw_giveaways.show_id
LEFT JOIN analytics.dw_show_viewer_events_cs ON dw_giveaways.show_id  = dw_show_viewer_events_cs.show_id;

DROP TABLE IF EXISTS analytics_scratch.viewers_at_giveaway_time;
CREATE TABLE analytics_scratch.viewers_at_giveaway_time AS
SELECT show_id, start_at, Is_silent_show,viewer_id,
       count(case when Is_giveaway_viewer = 'Yes' THEN 1 ELSE NULL END ) AS giveaways_viewed_count,
       count(case when view_type = '1 view' THEN 1 ELSE NULL END ) AS count_1_view

    FROM (SELECT
    DISTINCT viewers_time_duration.*,
    dw_giveaways.giveaway_id,
    dw_giveaways.start_at as giveaway_start_at,
    dw_giveaways.end_at as giveaway_end_at,
    CASE WHEN (event_at < giveaway_end_at AND event_end_at > giveaway_start_at) OR dw_giveaway_entries.participant_id IS NOT NULL THEN 'Yes' ELSE 'No' END AS Is_giveaway_viewer
    FROM analytics_scratch.viewers_time_duration
LEFT JOIN analytics.dw_giveaways ON viewers_time_duration.show_id = dw_giveaways.show_id
LEFT JOIN analytics.dw_giveaway_entries ON viewers_time_duration.viewer_id = dw_giveaway_entries.participant_id AND dw_giveaways.giveaway_id = dw_giveaway_entries.giveaway_id
) as d
GROUP BY 1,2,3,4;
GRANT ALL ON analytics_scratch.viewers_at_giveaway_time TO PUBLIC;













