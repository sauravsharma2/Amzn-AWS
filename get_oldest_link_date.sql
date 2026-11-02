;WITH ids(Id) AS (
    SELECT v.Id FROM (VALUES {orig_values}) AS v(Id)
),
src AS (
    -- ParentId dans Links
    SELECT
        o.ParentId AS OriginalId,
        CONVERT(varchar(8), o.SourceDate) AS SD
    FROM bvdaffils_yesterday.dbo.Links AS o WITH (NOLOCK)
    JOIN ids AS t ON t.Id = o.ParentId
    WHERE o.SourceDate IS NOT NULL

    UNION ALL

    -- ChildId dans Links
    SELECT
        o.ChildId AS OriginalId,
        CONVERT(varchar(8), o.SourceDate) AS SD
    FROM bvdaffils_yesterday.dbo.Links AS o WITH (NOLOCK)
    JOIN ids AS t ON t.Id = o.ChildId
    WHERE o.SourceDate IS NOT NULL

    UNION ALL

    -- ParentId dans BO_links
    SELECT
        bo.ParentId AS OriginalId,
        CONVERT(varchar(8), bo.SourceDate) AS SD
    FROM bvdaffils_yesterday.dbo.BO_links AS bo WITH (NOLOCK)
    JOIN ids AS t ON t.Id = bo.ParentId
    WHERE bo.SourceDate IS NOT NULL

    UNION ALL

    -- ChildId dans BO_links
    SELECT
        bo.ChildId AS OriginalId,
        CONVERT(varchar(8), bo.SourceDate) AS SD
    FROM bvdaffils_yesterday.dbo.BO_links AS bo WITH (NOLOCK)
    JOIN ids AS t ON t.Id = bo.ChildId
    WHERE bo.SourceDate IS NOT NULL
)
SELECT
    OriginalId,
    MIN(SD) AS OldestSourceDate
FROM src
WHERE SD LIKE '[12][0-9][0-9][0-9][01][0-9][0-3][0-9]'
GROUP BY OriginalId
ORDER BY OriginalId;
