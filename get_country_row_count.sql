SELECT COUNT(*)
FROM bvdaffils.dbo.Companies WITH(NOLOCK)
WHERE Id LIKE '{country}%'
  AND SUBSTRING(Id, 4, 1) <> '*';
