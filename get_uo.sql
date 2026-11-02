SELECT DISTINCT
    l.ChildId,
    l.ParentId
FROM bvdaffils.dbo.UO_links AS l
WHERE l.ChildId IN ({ids_list});
