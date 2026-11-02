SELECT TOP ({batch_size})
       Id,
       Name,
       LegalStatus,
       NrLinks,
       Branch,
       Person,
       [Foreign],
       HistoricSinceDate
FROM bvdaffils.dbo.Companies WITH (NOLOCK)
WHERE Id LIKE '{country}%'
  AND Id > '{last_id}'
  AND (CHARINDEX('!', Id) = 0 OR RIGHT(Id, 1) = '!')
  AND (
          REPLACE(Id, '!', '') LIKE 'CH[CBI]%'
      or REPLACE(Id, '!', '') LIKE 'CHCHE%'

      )
  AND (Id NOT LIKE '%*%'
   AND Id NOT LIKE '%[CBI]PE%'
   AND Id NOT LIKE '%[CBI]PEM%'
   AND Id NOT LIKE '%[CBI]PEB%'
   AND Id NOT LIKE '%SCRL%'
   AND Id NOT LIKE '%CEBI%'
   AND Id NOT LIKE '%CBNB%'
   AND Id NOT LIKE '%CRLA%'
   AND Id NOT LIKE '%CRLB%'
   AND Id NOT LIKE '%CRLC%'
   AND Id NOT LIKE '%CSTAT%'
   AND Id NOT LIKE '%CFACT%'
   AND Id NOT LIKE '%CWOR%'
   AND Id NOT LIKE '%CSINO%'
   AND Id NOT LIKE '%CPEI%'
   AND Id NOT LIKE '%COFWI%'
   AND Id NOT LIKE '%CWIN%'
   AND Id NOT LIKE '%CVEDA%'
   AND Id NOT LIKE '%CZEF%'
   AND Id NOT LIKE '%CSIREN%'
   AND Id NOT LIKE '%CNIF%'
   AND Id NOT LIKE '%L'
   AND Id NOT LIKE '%C[1-4]-%'
   AND Id NOT LIKE '%CFS%'
   AND Id NOT LIKE '%CLEI%'
   AND Id NOT LIKE '%CBOE%')
  AND (Branch IN (0,3) OR Branch IS NULL)
  AND Person NOT IN ('I','D')
  AND ([Foreign] NOT IN (1,2) OR [Foreign] IS NULL)
ORDER BY Id;
