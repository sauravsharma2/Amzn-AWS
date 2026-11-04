-- Shared ownership links between OriginalId and MatchedId (info only).
-- #SharedPairs (OId, MId) is filled by main_exact_match.py (IDs without trailing '!').
-- Returns one row per shared entity: OriginalId, MatchedId, LinkType, SharedId.

-- Same shareholder (Links, both are children of the same parent)
SELECT p.OId AS OriginalId, p.MId AS MatchedId, 'Shareholder' AS LinkType, o.ParentId AS SharedId
FROM #SharedPairs p
JOIN bvdaffils.dbo.Links o WITH (NOLOCK) ON o.ChildId = p.OId AND o.Type = 1
JOIN bvdaffils.dbo.Links m WITH (NOLOCK) ON m.ChildId = p.MId AND m.Type = 1 AND m.ParentId = o.ParentId
WHERE o.ParentId NOT IN (p.OId, p.MId)

UNION

-- Same subsidiary (Links, both are parents of the same child)
SELECT p.OId, p.MId, 'Subsidiary', o.ChildId
FROM #SharedPairs p
JOIN bvdaffils.dbo.Links o WITH (NOLOCK) ON o.ParentId = p.OId AND o.Type = 1
JOIN bvdaffils.dbo.Links m WITH (NOLOCK) ON m.ParentId = p.MId AND m.Type = 1 AND m.ChildId = o.ChildId
WHERE o.ChildId NOT IN (p.OId, p.MId)

UNION

-- Same beneficial owner (BO_links)
SELECT p.OId, p.MId, 'BO', o.ParentId
FROM #SharedPairs p
JOIN bvdaffils.dbo.BO_links o WITH (NOLOCK) ON o.ChildId = p.OId
JOIN bvdaffils.dbo.BO_links m WITH (NOLOCK) ON m.ChildId = p.MId AND m.ParentId = o.ParentId
WHERE o.ParentId NOT IN (p.OId, p.MId)

UNION

-- Same beneficial subsidiary (BO_links)
SELECT p.OId, p.MId, 'BO Subsidiary', o.ChildId
FROM #SharedPairs p
JOIN bvdaffils.dbo.BO_links o WITH (NOLOCK) ON o.ParentId = p.OId
JOIN bvdaffils.dbo.BO_links m WITH (NOLOCK) ON m.ParentId = p.MId AND m.ChildId = o.ChildId
WHERE o.ChildId NOT IN (p.OId, p.MId)

UNION

-- Same ultimate owner (UO_links)
SELECT p.OId, p.MId, 'UO', o.ParentId
FROM #SharedPairs p
JOIN bvdaffils.dbo.UO_links o WITH (NOLOCK) ON o.ChildId = p.OId
JOIN bvdaffils.dbo.UO_links m WITH (NOLOCK) ON m.ChildId = p.MId AND m.ParentId = o.ParentId
WHERE o.ParentId NOT IN (p.OId, p.MId)

OPTION (RECOMPILE);
