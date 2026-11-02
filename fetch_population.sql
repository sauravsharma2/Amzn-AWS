-- Population to search for matches (used by main_exact_match.py).
-- Edit the WHERE clause to define your population.
--
-- Rules for this file:
--   * Must return: Id, Name, Address, City, Postcode, Country
--   * Keep the {batch_size} and {last_id} placeholders + ORDER BY Id
--     (script pages through results: Id > last Id of previous batch).
--   * Remove both placeholders if you want a single run with no paging.

SELECT TOP ({batch_size})
       c.Id,
       c.Name,
       c.Address,
       c.City,
       c.Postcode,
       c.Country
FROM bvdaffils.dbo.Companies AS c WITH (NOLOCK)
WHERE c.Id > '{last_id}'
  -- ↓↓↓ your population filters here ↓↓↓
  AND c.Id NOT LIKE '___*%'
  AND (c.Branch IN (0,3) OR c.Branch IS NULL)
  AND c.Person NOT IN ('I','D')
  AND (c.[Foreign] NOT IN (1,2) OR c.[Foreign] IS NULL)
ORDER BY c.Id;
