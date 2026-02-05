SELECT   
 dbo.tblParvande.HozeId *1 as 'حوزه',
 dbo.tblParvande.Kelase as 'کلاسه',
 dbo.tblMoady.FName + N' ' + dbo.tblMoady.LName as 'مودي' ,
 tblAmalkard.SalAz as 'سال از',
 tblAmalkard.SalTa as 'سال تا', 
 tblAmalkard.motamem as 'متمم', 
tblAmalkard.Daramad as 'درآمد',
tblAmalkard.Maliat as 'ماليات',
 dbo.tblMakhazType.makhazType as 'منبع' ,
 dbo.tblAgent.Fname+' '+ dbo.tblAgent.Lname as 'نام نماينده بند يک',
tblAgent2.Fname+' '+ tblAgent2.Lname as 'نام نماينده بند دو',
tblAgent3.Fname+' '+ tblAgent3.Lname as 'نام نماينده بند سه',
 
 dbo.tblKarShenas.Fname + N' ' + dbo.tblKarShenas.Lname AS 'کارشناس',
 --tblGHararKar.*,
 --tblGHarar.*,
 
tblGHarar.erja AS 'تاريخ ارجاع به کارشناس',

 tblGharar.Odat AS 'تاريخ عودت گزارش',
dbo.tblWorkCalen.Tarikh as 'تاريخ تشکيل کميسيون',------------------------------BaseForFilter ----تاریخ ارسال به هیات
dbo.tblTimeAssign.Daf_Date_Im_Ex  as 'تاريخ راي',
--dbo.tblTimeAssign.*,
--tblAmalkard.*,
--dbo.tblTimeAssign.Daf_Andi as 'شماره راي',
tblHokm.Andi as 'شماره راي',
[tblHokm].SabtDate as 'تاريخ ثبت' 
,
 dbo.TblRayType.RayType as 'نوع راي' ,

 dbo.TblHeiatType.HeiatType As 'نوع هيات'
,tblBarg.SabtDate as 'تاريخ ورود پرونده به هيات'


 --tblTimeAssign.*,

           ,  dbo.tblGhararType.GhararType as 'نوع قرار'

-- ,tblGharar.*
-- ,tblGHararKar.*
,tblAmalkard.Daramad as 'درآمد'
,tblAmalkard.Maliat as 'ماليات'
,maliatType as 'نوع ماخذ'

, [tblTimeAssign].[DaramadRay] as 'درآمد نهايي'
,[tblTimeAssign].maliat as 'ماليات راي'
,tblBarg.Andi as 'شماره انديکاتور برگ دعوت'
,tblBarg.SabtDate as 'تاريخ انديگاتور برگ دعوت'
,case when (tblTimeAssign.NoVosul=1) then 'عدم وصول' else 'وصول' end as 'دريافت پرونده'
,tblMoady.codemeli
--,tblHokm.Hokm as 'حکم'-
--,tblHokm.moadi as 'مودي'
--,tblHokm.mamur as 'مامور'

            from
                      dbo.tblBarg left join 
                      tblAmalkard on
                       tblAmalkard.SabtNo = dbo.tblBarg.SabtNo AND tblAmalkard.mahalId = dbo.tblBarg.mahalId
                       left join 
                      dbo.tblTimeAssign ON tblAmalkard.AmalkardId = dbo.tblTimeAssign.AmalkardId left JOIN
                      dbo.TblRayType ON dbo.tblTimeAssign.RayTypeId = dbo.TblRayType.RayTypeId left JOIN
                      dbo.tblMakhazType ON tblAmalkard.makhazTypeId = dbo.tblMakhazType.makhazTypeId left JOIN
                      dbo.tblMaliatType ON tblAmalkard.maliatTypeId = dbo.tblMaliatType.maliatTypeId left JOIN
                      dbo.tblMoadyParv ON tblAmalkard.MoadyParvId = dbo.tblMoadyParv.MoadyParvId left JOIN
                      dbo.tblMoady ON dbo.tblMoadyParv.MoadyId = dbo.tblMoady.MoadyId 
                      left JOIN dbo.tblParvande ON dbo.tblMoadyParv.parvId = dbo.tblParvande.parvId 
                      left JOIN dbo.Edareh ON LEFT(dbo.tblParvande.HozeId, 3) = dbo.Edareh.Code left JOIN
                      dbo.tblWorkCalen ON dbo.tblTimeAssign.WorkcalenId = dbo.tblWorkCalen.WorkCalenId  
                      left join
                      dbo.tblHokm on   dbo.tblHokm.HokmId = dbo.tblTimeAssign.HokmId LEFT  JOIN
                      dbo.tblAgent ON dbo.tblHokm.darai = dbo.tblAgent.AgentId  
                      LEFT  JOIN
                      dbo.tblAgent as tblAgent2 ON dbo.tblHokm.dadgostari = tblAgent2.AgentId  
                      LEFT  JOIN
                      dbo.tblAgent as tblAgent3 ON dbo.tblHokm.senf = tblAgent3.AgentId  
                      
                      left JOIN
                      dbo.TblHeiatType ON dbo.tblBarg.HeiatTypeId = dbo.TblHeiatType.HeiatTypeId 

                       left join   dbo.tblGharar ON dbo.tblGharar.HokmId = dbo.tblHokm.HokmId  

                       left JOIN
                       dbo.tblGHararKar on dbo.tblGHararKar.GhararId=tblGharar.GhararId   

                       LEFT  JOIN
                      dbo.tblKarShenas ON dbo.tblGHararKar.KarshenasId = dbo.tblKarShenas.KarshenasId left JOIN
                      
                      dbo.tblGhararType ON tblGharar.GhararTypeId = dbo.tblGhararType.GhararTypeId
                      
                      where  dbo.tblWorkCalen.Tarikh BETWEEN '1403/01/25' AND '1403/02/25'
 

