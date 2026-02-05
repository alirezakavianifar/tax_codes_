SELECT [Modi_Seq],adam.* FROM
(SELECT [cod_hozeh]
      ,[k_parvand]
      ,[sal]
      ,[adam_elat]
      ,[sabt_no]
      ,[sabt_date]
      ,[comment]
      ,[userid]
      ,[cnt_rpt]
      ,[taeed]
      ,[rad]
      ,[kartabl]
      ,[ebtal]
      ,[rahgiri]
      ,[changestatus]
      ,[changedate]
      ,[HostName]
      ,[Usr]
  FROM [MASHAGHEL].[dbo].[adam_faliat_inf]
  where [sal]=1397)as adam

  LEFT JOIN 

 ( SELECT [Cod_Hozeh]
      ,[K_Parvand]
      ,[Modi_Seq]
      ,[sal]
     
  FROM [MASHAGHEL].[dbo].[KMLink_Inf]
  where [sal]=1397)as moadi
  on 
  adam.cod_hozeh=moadi.Cod_Hozeh
  and adam.k_parvand=moadi.K_Parvand
  and adam.sal=moadi.sal