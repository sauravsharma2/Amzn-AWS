SELECT TOP ({batch_size})
       Id,
       Name,
       LegalStatus,
       NrLinks,
       Branch,
       Person,
       [Foreign],
       HistoricSinceDate
FROM bvdaffils.dbo.Companies WITH(NOLOCK)
WHERE Id LIKE '{country}%'
  AND Id > '{last_id}'
  AND Id NOT LIKE '___*%'
  AND (Branch IN (0,3) OR Branch IS NULL)
  AND Person NOT IN ('I','D')
  AND ([Foreign] NOT IN (1,2) OR [Foreign] IS NULL)
  AND (
        RIGHT(Id, 5) <> '00000'
        OR Id LIKE 'PT%'
        OR Id LIKE 'BR%'
      )
ORDER BY Id;