DECLARE @today_yyyymmdd  INT = CONVERT(INT, CONVERT(varchar(8), CAST(GETDATE() AS date), 112));
DECLARE @weekago_yyyymmdd INT = CONVERT(INT, CONVERT(varchar(8), DATEADD(day, -7, CAST(GETDATE() AS date)), 112));


SELECT
    Id,
    Name,
    Person,
    Branch,
    [Foreign]
FROM bvdaffils.dbo.Companies
WHERE
     ( Id LIKE 'CHC*%'
   OR Id LIKE 'CHPE%'
   OR Id LIKE 'CHPEM%'
   OR Id LIKE 'CHPEB%'
   OR Id LIKE 'CHSCRL%'
   OR Id LIKE 'CHCEBI%'
   OR Id LIKE 'CHCBNB%'
   OR Id LIKE 'CHCRLA%'
   OR Id LIKE 'CHCRLB%'
   OR Id LIKE 'CHCRLC%'
   OR Id LIKE 'CHCSTAT%'
   OR Id LIKE 'CHCFACT%'
   OR Id LIKE 'CHCWOR%'
   OR Id LIKE 'CHCSINO%'
   OR Id LIKE 'CHCPEI%'
   OR Id LIKE 'CHCOFWI%'
   OR Id LIKE 'CHCWIN%'
   OR Id LIKE 'CHCVEDA%'
   OR Id LIKE 'CHCZEF%'
   OR Id LIKE 'CHCSIREN%'
   OR Id LIKE 'CHCNIF%'
   OR Id LIKE 'CH%L'
   OR Id LIKE 'CHC[1-4]-%'
)
  AND (Branch IN (0,3) OR Branch IS NULL)
  AND Person NOT IN ('I','D')
  AND ([Foreign] NOT IN (1,2) OR [Foreign] IS NULL)
  AND LastTransferDate BETWEEN @weekago_yyyymmdd AND @today_yyyymmdd
ORDER BY Id;
