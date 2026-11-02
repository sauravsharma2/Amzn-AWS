SELECT
    c.Id,
    c.HistoricSinceDate,
    c.LegalStatus
FROM bvdaffils.dbo.Companies AS c
WHERE c.Id IN ({ids_list});
