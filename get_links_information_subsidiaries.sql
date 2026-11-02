SELECT
    l.ParentId AS Id,
    l.ChildId,
    l.DirectPercent, l.TotalPercent, l.TotalPercentIndic,
    l.SourceDate, l.SourceNr, l.LastModifiedDate, l.Archive,
    TRY_CAST(CAST(l.SourceDate AS CHAR(8)) AS DATE) AS DateObj
INTO #RawLinks
FROM bvdaffils.dbo.Links l WITH (NOLOCK)
WHERE l.ParentId IN ({ids_list}) AND l.Type = 1;

CREATE CLUSTERED INDEX IX_RL ON #RawLinks(Id, ChildId);

WITH Joined AS (
    SELECT
        p.O_Id AS OriginalId, p.M_Id AS MatchedId_base, o.ChildId,
        o.DirectPercent AS O_DirectPercent, o.TotalPercent AS O_TotalPercent,
        o.TotalPercentIndic AS O_TotalPercentIndic, o.SourceDate AS O_SourceDate,
        o.SourceNr AS O_SourceNr, o.LastModifiedDate AS O_LastModifiedDate, o.Archive AS O_Archive,
        m.DirectPercent AS M_DirectPercent, m.TotalPercent AS M_TotalPercent,
        m.TotalPercentIndic AS M_TotalPercentIndic, m.SourceDate AS M_SourceDate,
        m.SourceNr AS M_SourceNr, m.LastModifiedDate AS M_LastModifiedDate, m.Archive AS M_Archive,
        ABS(DATEDIFF(day,
            ISNULL(o.DateObj, '19000101'),
            ISNULL(m.DateObj, '19000101'))) AS DateDiffVal,
        CASE WHEN ISNULL(o.SourceDate, 0) > ISNULL(m.SourceDate, 0)
             THEN o.SourceDate ELSE m.SourceDate END AS MaxDateVal
    FROM #PairsToProcess p
    JOIN #RawLinks o ON o.Id = p.O_Id
    JOIN #RawLinks m ON m.Id = p.M_Id AND m.ChildId = o.ChildId
),
Ranked AS (
    SELECT *, ROW_NUMBER() OVER (
        PARTITION BY OriginalId, MatchedId_base
        ORDER BY DateDiffVal ASC, MaxDateVal DESC
    ) AS rn
    FROM Joined
)
SELECT OriginalId, MatchedId_base, ChildId,
       O_DirectPercent, O_TotalPercent, O_TotalPercentIndic, O_SourceDate,
       O_SourceNr, O_LastModifiedDate, O_Archive,
       M_DirectPercent, M_TotalPercent, M_TotalPercentIndic, M_SourceDate,
       M_SourceNr, M_LastModifiedDate, M_Archive
FROM Ranked WHERE rn = 1
OPTION (RECOMPILE);

DROP TABLE IF EXISTS #RawLinks;
