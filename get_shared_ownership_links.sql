-- Shared ownership links between OriginalId and MatchedId (info only).
-- Same pattern as get_links_information_*.sql:
--   1) pull links only for the batch IDs with IN (...)  -> index seek on Links / BO_links / UO_links
--   2) compare O vs M inside the small temp table.
-- #PairsToProcess (O_Id, M_Id) is created and filled by check_shared_links.py.
-- {ids_list} = all OriginalIds + MatchedIds of the batch (without trailing '!').

SET NOCOUNT ON;

SELECT l.ChildId AS Id, l.ParentId AS SharedId, CAST('Shareholder' AS VARCHAR(20)) AS LinkType
INTO #RawShared
FROM bvdaffils.dbo.Links l WITH (NOLOCK)
WHERE l.ChildId IN ({ids_list}) AND l.Type = 1;

INSERT INTO #RawShared (Id, SharedId, LinkType)
SELECT l.ParentId, l.ChildId, 'Subsidiary'
FROM bvdaffils.dbo.Links l WITH (NOLOCK)
WHERE l.ParentId IN ({ids_list}) AND l.Type = 1;

INSERT INTO #RawShared (Id, SharedId, LinkType)
SELECT l.ChildId, l.ParentId, 'BO'
FROM bvdaffils.dbo.BO_links l WITH (NOLOCK)
WHERE l.ChildId IN ({ids_list});

INSERT INTO #RawShared (Id, SharedId, LinkType)
SELECT l.ParentId, l.ChildId, 'BO Subsidiary'
FROM bvdaffils.dbo.BO_links l WITH (NOLOCK)
WHERE l.ParentId IN ({ids_list});

INSERT INTO #RawShared (Id, SharedId, LinkType)
SELECT l.ChildId, l.ParentId, 'UO'
FROM bvdaffils.dbo.UO_links l WITH (NOLOCK)
WHERE l.ChildId IN ({ids_list});

CREATE CLUSTERED INDEX IX_RS ON #RawShared(Id, LinkType, SharedId);

SELECT DISTINCT
    p.O_Id     AS OriginalId,
    p.M_Id     AS MatchedId,
    o.LinkType AS LinkType,
    o.SharedId AS SharedId
FROM #PairsToProcess p
JOIN #RawShared o ON o.Id = p.O_Id
JOIN #RawShared m ON m.Id = p.M_Id
                 AND m.LinkType = o.LinkType
                 AND m.SharedId = o.SharedId
WHERE o.SharedId NOT IN (p.O_Id, p.M_Id)
OPTION (RECOMPILE);

DROP TABLE IF EXISTS #RawShared;
