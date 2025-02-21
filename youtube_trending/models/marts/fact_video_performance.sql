WITH video_engagement AS (
    SELECT
        videoId,
        title,
        channelId,
        category,
        regionCode,
        publishedAt,
        trendingAt,
        viewCount,
        likeCount,
        commentCount,
        DATE_DIFF(trendingAt, publishedAt, DAY) AS days_to_trend
    FROM `dibimbing-442703.dibimbing.stg_youtube_data`
)

SELECT
    videoId,
    title,
    channelId,
    category,
    regionCode,
    publishedAt,
    trendingAt,
    viewCount,
    likeCount,
    commentCount,
    days_to_trend,
    SAFE_DIVIDE(likeCount, viewCount) AS like_ratio,
    SAFE_DIVIDE(commentCount, viewCount) AS comment_ratio
FROM video_engagement
