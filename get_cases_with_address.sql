-- Original side: details for the IDs listed in cases.txt.
-- #TempIds is filled by the script from cases.txt.
SELECT
    c.Id,
    c.Name,
    c.Address,
    c.City,
    c.Postcode,
    c.Country
FROM bvdaffils.dbo.Companies AS c WITH (NOLOCK)
INNER JOIN #TempIds t ON c.Id = t.Id;
