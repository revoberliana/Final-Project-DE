WITH channel_summary AS (
    SELECT
        channelId,
        channelTitle,
        COUNT(DISTINCT videoId) AS total_videos,
        SUM(viewCount) AS total_views,
        SUM(likeCount) AS total_likes,
        SUM(commentCount) AS total_comments,
        AVG(SAFE_DIVIDE(likeCount, viewCount)) AS avg_like_ratio,
        AVG(SAFE_DIVIDE(commentCount, viewCount)) AS avg_comment_ratio
    FROM `dibimbing-442703.dibimbing.stg_youtube_data`
    GROUP BY channelId, channelTitle
)

SELECT
    channelId,
    channelTitle,
    total_videos,
    total_views,
    total_likes,
    total_comments,
    avg_like_ratio,
    avg_comment_ratio
FROM channel_summary
