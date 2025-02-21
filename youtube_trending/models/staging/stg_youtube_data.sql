--Cleans raw data
--Ensures correct data types
--Normalizes tag values
--Removes NULL or inconsistent data

WITH raw_data AS (
    SELECT
        videoId,
        trendingAt,
        publishedAt,
        title,
        channelId,
        channelTitle,
        durationSec,
        description,
        tags,
        category,
        defaultAudioLanguage,
        thumbnailUrl,
        viewCount,
        likeCount,
        commentCount,
        caption,
        regionCode
    FROM `dibimbing-442703.dibimbing.trending_videos`
)

SELECT 
    videoId,
    trendingAt,
    publishedAt,
    title,
    channelId,
    channelTitle,
    durationSec,
    description,
    LOWER(tags) AS tags,  
    category,
    defaultAudioLanguage,
    thumbnailUrl,
    SAFE_CAST(viewCount AS INT64) AS viewCount, 
    SAFE_CAST(likeCount AS INT64) AS likeCount, 
    SAFE_CAST(commentCount AS INT64) AS commentCount, 
    caption,
    regionCode
FROM raw_data
WHERE videoId IS NOT NULL