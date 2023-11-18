

liste = f"""SELECT distinct st.ARTIKELNR

  FROM [emea_enventa_live].[dbo].[BESTELLPOS] as bp
  left join [emea_enventa_live].[dbo].STUECKLISTE as st
  on bp.ARTIKELNR = st.BAUGRUPPE
  where bp.LIEFERANTENNR = 70044 and bp.STATUS < 3 and (st.BEZEICHNUNG like 'PM%') """


########################################################################################################################

bestand_ancorotti = f"""SELECT [ARTIKELNR]
      ,[BUCHBESTAND]
  FROM [emea_enventa_live].[dbo].[LAGER]
  where BranchKey = 110 and LAGERNR = 208"""

########################################################################################################################
fw_ancorotti = f"""SELECT

      bp.[BELEGNR]

      ,bp.[ARTIKELNR]
      ,bp.[BEZEICHNUNG]
      ,bp.[MENGE_BESTELLT]

      ,bp.[LIEFERDATUM] as 'DELIVERY DATE'

      ,bp.[FIXPOSNR]
      ,bp.[BELEGART]
      ,st.ARTIKELNR as 'PM Nr'
      ,st.BEZEICHNUNG as 'PM Description'
      ,bp.[PE14_CommentEMEA]


  FROM [emea_enventa_live].[dbo].[BESTELLPOS] as bp
  left join [emea_enventa_live].[dbo].STUECKLISTE as st
  on bp.ARTIKELNR = st.BAUGRUPPE
  where bp.LIEFERANTENNR = 70044 and bp.STATUS < 3 and (st.BEZEICHNUNG like 'PM%' or st.BEZEICHNUNG is null)

  order by bp.LIEFERDATUM"""

########################################################################################################################

pm_ancorotti = f"""SELECT [FIXPOSNR]
      ,[BELEGART]
      ,[BELEGNR]
      ,[ARTIKELNR]
      ,[BEZEICHNUNG]
      ,[PREADVISE MENGE]
      ,[ABFÜLLER]
      ,[EXP DISPATCH]

  FROM [emea_enventa_live].[db_dataviewer].[PE14_SHIPPMENTLIST]
  where BELEGART = 191 and ARTIKELNR <> '0099-001-000-01' and [SHIPMENT STATUS] <> 'received' and ABFÜLLER = 'NUCO E.i.G. Kosyl s.j.'

  order by [EXP DISPATCH]"""

########################################################################################################################

