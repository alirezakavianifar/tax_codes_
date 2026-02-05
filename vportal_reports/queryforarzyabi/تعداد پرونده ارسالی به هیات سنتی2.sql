	with tblMain as
(
SELECT [حوزه] AS [واحد مالیاتی]
  FROM [TestDb].[dbo].[tblMashCommission]
  WHERE [تاريخ تشکيل کميسيون] BETWEEN '1403/01/25' AND '1403/02/25'
 )
SELECT 
IrisCode AS [کد اداره]
,[اداره-شهر]
,count(*) AS [تعداد پرونده های ارسال به هیات سنتی]
FROM
(select ISNULL(tax.dbo.tblEdareShahr.city,'**') as 'شهرستان-',ISNULL(tax.dbo.tblEdareShahr.edare,'**') as 'اداره',ISNULL(tax.dbo.tblEdareShahr.city,'**')+'-'+ISNULL(tax.dbo.tblEdareShahr.edare,'**') as [اداره-شهر] ,ISNULL(tax.dbo.tblEdareShahr.IrisCode,'**') as IrisCode, tblMain.*,1 as tedad from 
tblMain
 inner join tax.dbo.tblEdareShahr on substring(tblMain.[واحد مالیاتی],1,5) = substring(tax.dbo.tblEdareShahr.edare,1,5) collate Arabic_CI_AS 
 
 UNION ALL
 
select ISNULL(tax.dbo.tblEdareShahr.city,'**') as 'شهرستان-',ISNULL(tax.dbo.tblEdareShahr.edare,'**') as 'اداره',ISNULL(tax.dbo.tblEdareShahr.city,'**')+'-'+ISNULL(tax.dbo.tblEdareShahr.edare,'**') as [اداره-شهر],ISNULL(tax.dbo.tblEdareShahr.IrisCode,'**') as IrisCode , tblMain.*,1 as tedad from 
tblMain
 Left join tax.dbo.tblEdareShahr on substring(tblMain.[واحد مالیاتی],1,4) = tax.dbo.tblEdareShahr.edare collate Arabic_CI_AS
 where isnull(tblMain.[واحد مالیاتی],'i') not in 
 ( SELECT [واحد مالیاتی]
 FROM tblMain inner join tax.dbo.tblEdareShahr on substring(tblMain.[واحد مالیاتی],1,5) = substring(tax.dbo.tblEdareShahr.edare,1,5) collate Arabic_CI_AS ))as a
 GROUP by 
 IrisCode
,[اداره-شهر]




